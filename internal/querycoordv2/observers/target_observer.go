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

const (
	updateNextTargetInterval = 3 * time.Second
	nextTargetSurviveTime    = 3 * time.Minute
)

type TargetObserver struct {
	c         chan struct{}
	wg        sync.WaitGroup
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	distMgr   *meta.DistributionManager
	broker    meta.Broker

	nextTargetLastUpdate map[int64]time.Time
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
	close(ob.c)
	ob.wg.Wait()
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
	for ID := range ob.nextTargetLastUpdate {
		if !collectionSet.Contain(ID) {
			delete(ob.nextTargetLastUpdate, ID)
		}
	}
}

func (ob *TargetObserver) shouldUpdateNextTarget(collectionID int64) bool {
	return ob.IsNextTargetExpired(collectionID) ||
		!utils.IsNextTargetExist(ob.targetMgr, collectionID) ||
		!utils.IsNextTargetValid(collectionID)
}

func (ob *TargetObserver) IsNextTargetExpired(collectionID int64) bool {
	return time.Since(ob.nextTargetLastUpdate[collectionID]) > params.Params.QueryCoordCfg.NextTargetSurviveTime
}

func (ob *TargetObserver) UpdateNextTarget(collectionID int64) {
	err := ob.targetMgr.UpdateCollectionNextTarget(collectionID)
	if err != nil {
		log.Warn("update next target failed",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))
	}
	ob.updateNextTargetTimestamp(collectionID)
}

func (ob *TargetObserver) updateNextTargetTimestamp(collectionID int64) {
	ob.nextTargetLastUpdate[collectionID] = time.Now()
}

func (ob *TargetObserver) shouldUpdateCurrentTarget(collectionID int64) bool {
	return utils.IsNextTargetReadyForCollection(ob.targetMgr, ob.distMgr, ob.meta, collectionID)
}

func (ob *TargetObserver) updateCurrentTarget(collectionID int64) {
	err := ob.targetMgr.UpdateCollectionCurrentTarget(collectionID)
	if err != nil {
		log.Warn("update next target failed",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))
	}
}
