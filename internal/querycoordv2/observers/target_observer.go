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

package observers

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type TargetObserver struct {
	c         chan struct{}
	wg        sync.WaitGroup
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	distMgr   *meta.DistributionManager
	broker    meta.Broker

	nextTargetLastUpdate map[int64]time.Time
	stopOnce             sync.Once
}

func NewTargetObserver(meta *meta.Meta, targetMgr *meta.TargetManager, distMgr *meta.DistributionManager, broker meta.Broker) *TargetObserver {
	return &TargetObserver{
		c:                    make(chan struct{}),
		meta:                 meta,
		targetMgr:            targetMgr,
		distMgr:              distMgr,
		broker:               broker,
		nextTargetLastUpdate: make(map[int64]time.Time),
	}
}

func (ob *TargetObserver) Start(ctx context.Context) {
	ob.wg.Add(1)
	go ob.schedule(ctx)
}

func (ob *TargetObserver) Stop() {
	ob.stopOnce.Do(func() {
		close(ob.c)
		ob.wg.Wait()
	})
}

func (ob *TargetObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	log.Info("Start update next target loop")

	ticker := time.NewTicker(params.Params.QueryCoordCfg.UpdateNextTargetInterval)
	for {
		select {
		case <-ctx.Done():
			log.Info("Close target observer due to context canceled")
			return
		case <-ob.c:
			log.Info("Close target observer")
			return

		case <-ticker.C:
			ob.tryUpdateTarget()
		}
	}
}

func (ob *TargetObserver) tryUpdateTarget() {
	collections := ob.meta.GetAll()
	for _, collectionID := range collections {
		if ob.shouldUpdateCurrentTarget(collectionID) {
			ob.updateCurrentTarget(collectionID)
		}

		if ob.shouldUpdateNextTarget(collectionID) {
			// update next target in collection level
			ob.UpdateNextTarget(collectionID)
		}
	}

	collectionSet := typeutil.NewUniqueSet(collections...)
	// for collection which has been removed from target, try to clear nextTargetLastUpdate
	for collection := range ob.nextTargetLastUpdate {
		if !collectionSet.Contain(collection) {
			delete(ob.nextTargetLastUpdate, collection)
		}
	}
}

func (ob *TargetObserver) shouldUpdateNextTarget(collectionID int64) bool {
	return !ob.targetMgr.IsNextTargetExist(collectionID) || ob.isNextTargetExpired(collectionID)
}

func (ob *TargetObserver) isNextTargetExpired(collectionID int64) bool {
	return time.Since(ob.nextTargetLastUpdate[collectionID]) > params.Params.QueryCoordCfg.NextTargetSurviveTime
}

func (ob *TargetObserver) UpdateNextTarget(collectionID int64) {
	log := log.With(zap.Int64("collectionID", collectionID))

	log.Warn("observer trigger update next target")
	err := ob.targetMgr.UpdateCollectionNextTarget(collectionID)
	if err != nil {
		log.Error("failed to update next target for collection",
			zap.Error(err))
		return
	}
	ob.updateNextTargetTimestamp(collectionID)
}

func (ob *TargetObserver) updateNextTargetTimestamp(collectionID int64) {
	ob.nextTargetLastUpdate[collectionID] = time.Now()
}

func (ob *TargetObserver) shouldUpdateCurrentTarget(collectionID int64) bool {
	replicaNum := len(ob.meta.ReplicaManager.GetByCollection(collectionID))

	// check channel first
	channelNames := ob.targetMgr.GetDmChannelsByCollection(collectionID, meta.NextTarget)
	if len(channelNames) == 0 {
		// next target is empty, no need to update
		return false
	}

	for _, channel := range channelNames {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			collectionID,
			ob.distMgr.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
		if len(group) < replicaNum {
			return false
		}
	}

	// and last check historical segment
	historicalSegments := ob.targetMgr.GetHistoricalSegmentsByCollection(collectionID, meta.NextTarget)
	for _, segment := range historicalSegments {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			collectionID,
			ob.distMgr.LeaderViewManager.GetSealedSegmentDist(segment.GetID()))
		if len(group) < replicaNum {
			return false
		}
	}

	return true
}

func (ob *TargetObserver) updateCurrentTarget(collectionID int64) {
	log.Warn("observer trigger update current target",
		zap.Int64("collectionID", collectionID))
	ob.targetMgr.UpdateCollectionCurrentTarget(collectionID)
}
