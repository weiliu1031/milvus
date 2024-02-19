// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkers

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ Checker = (*LeaderChecker)(nil)

// LeaderChecker perform segment index check.
type LeaderChecker struct {
	*checkerActivation
	meta    *meta.Meta
	dist    *meta.DistributionManager
	target  *meta.TargetManager
	nodeMgr *session.NodeManager

	broker meta.Broker
}

func NewLeaderChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	target *meta.TargetManager,
	nodeMgr *session.NodeManager,
	broker meta.Broker,
) *LeaderChecker {
	return &LeaderChecker{
		checkerActivation: newCheckerActivation(),
		meta:              meta,
		dist:              dist,
		target:            target,
		nodeMgr:           nodeMgr,
		broker:            broker,
	}
}

func (c *LeaderChecker) ID() utils.CheckerType {
	return utils.LeaderChecker
}

func (c *LeaderChecker) Description() string {
	return "LeaderChecker checks the difference of leader view between dist, and try to correct it"
}

func (c *LeaderChecker) readyToCheck(collectionID int64) bool {
	metaExist := (c.meta.GetCollection(collectionID) != nil)
	targetExist := c.target.IsNextTargetExist(collectionID) || c.target.IsCurrentTargetExist(collectionID)

	return metaExist && targetExist
}

func (c *LeaderChecker) Check(ctx context.Context) []task.Task {
	if !c.IsActive() {
		return nil
	}

	collectionIDs := c.meta.CollectionManager.GetAll()
	tasks := make([]task.Task, 0)

	for _, collectionID := range collectionIDs {
		if !c.readyToCheck(collectionID) {
			continue
		}
		collection := c.meta.CollectionManager.GetCollection(collectionID)
		if collection == nil {
			log.Warn("collection released during check leader", zap.Int64("collection", collectionID))
			continue
		}

		replicas := c.meta.ReplicaManager.GetByCollection(collectionID)
		for _, replica := range replicas {
			for _, node := range replica.GetNodes() {
				if ok, _ := c.nodeMgr.IsStoppingNode(node); ok {
					// no need to correct leader's view which is loaded on stopping node
					continue
				}

				leaderViews := c.dist.LeaderViewManager.GetByCollectionAndNode(replica.GetCollectionID(), node)
				for ch, leaderView := range leaderViews {
					dist := c.dist.SegmentDistManager.GetByShardWithReplica(ch, replica)
					tasks = append(tasks, c.findNeedLoadedSegments(ctx, replica.ID, leaderView, dist)...)
					tasks = append(tasks, c.findNeedRemovedSegments(ctx, replica.ID, leaderView, dist)...)
				}
			}
		}
	}

	return tasks
}

func (c *LeaderChecker) findNeedLoadedSegments(ctx context.Context, replica int64, leaderView *meta.LeaderView, dist []*meta.Segment) []task.Task {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.Int64("replica", replica),
		zap.String("channel", leaderView.Channel),
		zap.Int64("leaderViewID", leaderView.ID),
	)
	ret := make([]task.Task, 0)
	dist = utils.FindMaxVersionSegments(dist)
	for _, s := range dist {
		version, ok := leaderView.Segments[s.GetID()]
		currentTarget := c.target.GetSealedSegment(s.CollectionID, s.GetID(), meta.CurrentTarget)
		existInCurrentTarget := currentTarget != nil
		existInNextTarget := c.target.GetSealedSegment(s.CollectionID, s.GetID(), meta.NextTarget) != nil

		if !existInCurrentTarget && !existInNextTarget {
			continue
		}

		leaderWithOldVersion := version.GetVersion() < s.Version
		// leader has newer version, but the query node which loaded the newer version has been shutdown
		leaderWithDirtyVersion := version.GetVersion() > s.Version && c.nodeMgr.Get(version.GetNodeID()) == nil

		if !ok || leaderWithOldVersion || leaderWithDirtyVersion {
			log.RatedDebug(10, "leader checker append a segment to set",
				zap.Int64("segmentID", s.GetID()),
				zap.Int64("nodeID", s.Node))

			resp, err := c.broker.GetSegmentInfo(ctx, s.ID)
			if err != nil || len(resp.GetInfos()) == 0 {
				log.Warn("failed to get segment info from DataCoord", zap.Error(err))
				return nil
			}

			channel := c.target.GetDmChannel(s.CollectionID, s.InsertChannel, meta.NextTargetFirst)
			if channel == nil {
				return nil
			}
			loadInfo := utils.PackSegmentLoadInfo(resp.Infos[0], channel.GetSeekPosition(), nil)

			syncAction := &querypb.SyncAction{
				Type:        querypb.SyncType_Set,
				PartitionID: s.PartitionID,
				SegmentID:   s.ID,
				NodeID:      s.Node,
				Version:     s.Version,
				Info:        loadInfo,
			}
			action := task.NewLeaderViewAction(leaderView.ID, task.ActionTypeSetSegment, s.GetInsertChannel(), s.GetID(), syncAction)
			t, err := task.NewSegmentTask(
				ctx,
				params.Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
				c.ID(),
				s.GetCollectionID(),
				replica,
				action,
			)
			if err != nil {
				log.Warn("create segment update task failed",
					zap.Int64("segmentID", s.GetID()),
					zap.Int64("node", s.Node),
					zap.Error(err),
				)
				continue
			}
			// index task shall have lower or equal priority than balance task
			t.SetPriority(task.TaskPriorityNormal)
			t.SetReason("add segment to leader view")
			ret = append(ret, t)
		}
	}
	return ret
}

func (c *LeaderChecker) findNeedRemovedSegments(ctx context.Context, replica int64, leaderView *meta.LeaderView, dists []*meta.Segment) []task.Task {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.Int64("replica", replica),
		zap.String("channel", leaderView.Channel),
		zap.Int64("leaderViewID", leaderView.ID),
	)

	ret := make([]task.Task, 0)
	distMap := make(map[int64]struct{})
	for _, s := range dists {
		distMap[s.GetID()] = struct{}{}
	}

	for sid, s := range leaderView.Segments {
		_, ok := distMap[sid]
		existInCurrentTarget := c.target.GetSealedSegment(leaderView.CollectionID, sid, meta.CurrentTarget) != nil
		existInNextTarget := c.target.GetSealedSegment(leaderView.CollectionID, sid, meta.NextTarget) != nil
		if ok || existInCurrentTarget || existInNextTarget {
			continue
		}
		log.Debug("leader checker append a segment to remove",
			zap.Int64("segmentID", sid),
			zap.Int64("nodeID", s.NodeID))

		syncAction := &querypb.SyncAction{
			Type:      querypb.SyncType_Remove,
			SegmentID: sid,
			NodeID:    s.NodeID,
		}

		action := task.NewLeaderViewAction(leaderView.ID, task.ActionTypeSetSegment, leaderView.Channel, sid, syncAction)
		t, err := task.NewSegmentTask(
			ctx,
			paramtable.Get().QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
			c.ID(),
			leaderView.CollectionID,
			replica,
			action,
		)
		if err != nil {
			log.Warn("create segment reduce task failed",
				zap.Int64("segmentID", sid),
				zap.Int64("nodeID", s.NodeID),
				zap.Error(err))
			continue
		}

		t.SetPriority(task.TaskPriorityNormal)
		t.SetReason("remove segment from leader view")
		ret = append(ret, t)
	}
	return ret
}
