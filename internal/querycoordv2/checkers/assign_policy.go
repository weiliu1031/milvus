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

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/samber/lo"
)

// AssignPolicy encapsulates the logic for assigning segments and channels to nodes.
// It provides a unified interface for both SegmentChecker and ChannelChecker to
// create assignment tasks based on replica configuration and balancer strategy.
type AssignPolicy interface {
	// AssignSegment assigns segments to nodes and creates corresponding tasks.
	// It handles grouping by shard, node selection, and special cases like L0 segments.
	AssignSegment(ctx context.Context, segments []*datapb.SegmentInfo, replica *meta.Replica) []task.Task

	// AssignChannel assigns channels to nodes and creates corresponding tasks.
	// It handles node selection based on replica configuration.
	AssignChannel(ctx context.Context, channels []*meta.DmChannel, replica *meta.Replica) []task.Task
}

// DefaultAssignPolicy is the default implementation of AssignPolicy.
// It encapsulates the standard segment and channel assignment logic used by QueryCoord.
type DefaultAssignPolicy struct {
	dist            *meta.DistributionManager
	getBalancerFunc GetBalancerFunc
	checkerID       utils.CheckerType
	timeout         time.Duration
}

// NewDefaultAssignPolicy creates a new DefaultAssignPolicy instance.
func NewDefaultAssignPolicy(
	dist *meta.DistributionManager,
	getBalancerFunc GetBalancerFunc,
	checkerID utils.CheckerType,
	timeout time.Duration,
) *DefaultAssignPolicy {
	return &DefaultAssignPolicy{
		dist:            dist,
		getBalancerFunc: getBalancerFunc,
		checkerID:       checkerID,
		timeout:         timeout,
	}
}

// AssignSegment implements the segment assignment logic.
// It groups segments by shard, determines available nodes for each shard,
// handles L0 segment special assignment, and creates tasks from assignment plans.
func (p *DefaultAssignPolicy) AssignSegment(ctx context.Context, segments []*datapb.SegmentInfo, replica *meta.Replica) []task.Task {
	if len(segments) == 0 {
		return nil
	}

	isLevel0 := segments[0].GetLevel() == datapb.SegmentLevel_L0
	shardSegments := lo.GroupBy(segments, func(s *datapb.SegmentInfo) string {
		return s.GetInsertChannel()
	})

	plans := make([]balance.SegmentAssignPlan, 0)
	for shard, segments := range shardSegments {
		// if channel is not subscribed yet, skip load segments
		leader := p.dist.LeaderViewManager.GetLatestShardLeaderByFilter(
			meta.WithReplica2LeaderView(replica),
			meta.WithChannelName2LeaderView(shard))
		if leader == nil {
			continue
		}

		// Get assignable nodes for this shard
		rwNodes := p.getAssignableNodesForSegment(replica, shard, isLevel0, leader.ID)
		if len(rwNodes) == 0 {
			continue
		}

		segmentInfos := lo.Map(segments, func(s *datapb.SegmentInfo, _ int) *meta.Segment {
			return &meta.Segment{
				SegmentInfo: s,
			}
		})
		shardPlans := p.getBalancerFunc().AssignSegment(ctx, replica.GetCollectionID(), segmentInfos, rwNodes, true)
		for i := range shardPlans {
			shardPlans[i].Replica = replica
		}
		plans = append(plans, shardPlans...)
	}

	return balance.CreateSegmentTasksFromPlans(ctx, p.checkerID, p.timeout, plans)
}

// AssignChannel implements the channel assignment logic.
// It determines available nodes for each channel and creates tasks from assignment plans.
func (p *DefaultAssignPolicy) AssignChannel(ctx context.Context, channels []*meta.DmChannel, replica *meta.Replica) []task.Task {
	plans := make([]balance.ChannelAssignPlan, 0)
	for _, ch := range channels {
		// Get assignable nodes for this channel
		rwNodes := p.getAssignableNodesForChannel(replica, ch.GetChannelName())
		if len(rwNodes) == 0 {
			continue
		}

		plan := p.getBalancerFunc().AssignChannel(ctx, replica.GetCollectionID(), []*meta.DmChannel{ch}, rwNodes, true)
		plans = append(plans, plan...)
	}

	for i := range plans {
		plans[i].Replica = replica
	}

	return balance.CreateChannelTasksFromPlans(ctx, p.checkerID, p.timeout, plans)
}

// getAssignableNodesForSegment returns the list of nodes that can be assigned to for segments.
// It first tries to get shard-specific RW nodes, falling back to replica-wide RW nodes.
// For L0 segments, it returns only the shard leader node since L0 segments must be
// co-located with the channel on the same node.
func (p *DefaultAssignPolicy) getAssignableNodesForSegment(replica *meta.Replica, shard string, isLevel0 bool, leaderID int64) []int64 {
	// L0 segment can only be assigned to shard leader's node
	if isLevel0 {
		return []int64{leaderID}
	}

	// First try to get shard-specific RW nodes
	rwNodes := replica.GetChannelRWNodes(shard)
	if len(rwNodes) == 0 {
		// Fall back to replica-wide RW nodes
		rwNodes = replica.GetRWNodes()
	}
	return rwNodes
}

// getAssignableNodesForChannel returns the list of nodes that can be assigned to for channels.
// It first tries to get channel-specific RW nodes, falling back to replica-wide RW nodes.
func (p *DefaultAssignPolicy) getAssignableNodesForChannel(replica *meta.Replica, channelName string) []int64 {
	// First try to get channel-specific RW nodes
	rwNodes := replica.GetChannelRWNodes(channelName)
	if len(rwNodes) == 0 {
		// Fall back to replica-wide RW nodes
		rwNodes = replica.GetRWNodes()
	}
	return rwNodes
}
