package observers

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
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
		nextTargetLastUpdate: map[int64]time.Time{},
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

	ticker := time.NewTicker(updateNextTargetInterval)
	for {
		select {
		case <-ctx.Done():
			log.Info("Close handoff handler dut to context canceled")
			return
		case <-ob.c:
			log.Info("Close handoff handler")
			return

		case <-ticker.C:
			ob.tryUpdateTarget()
		}
	}
}

func (ob *TargetObserver) tryUpdateTarget() {
	collectionSet := typeutil.UniqueSet{}

	collectionSet.Insert(ob.targetMgr.Current.GetAllCollections()...)
	collectionSet.Insert(ob.targetMgr.Next.GetAllCollections()...)

	for collectionID := range collectionSet {
		if ob.shouldUpdateCurrentTarget(collectionID) {
			ob.updateCurrentTarget(collectionID)
		}

		partitionIDs, err := utils.GetPartitions(ob.meta.CollectionManager, ob.broker, collectionID)
		if err != nil {
			log.Warn("tryUpdateTarget: get partition failed", zap.Int64("collectionID", collectionID))
			continue
		}
		if ob.shouldUpdateNextTarget(collectionID) {
			// update next target in collection level
			ob.UpdateNextTarget(collectionID, partitionIDs)
		}
	}

	// for collection which has been removed from target, try to clear nextTargetLastUpdate
	for ID := range ob.nextTargetLastUpdate {
		if !collectionSet.Contain(ID) {
			delete(ob.nextTargetLastUpdate, ID)
		}
	}

}

func (ob *TargetObserver) shouldUpdateNextTarget(collectionID int64, partitionIDs ...int64) bool {
	return ob.IsNextTargetExpired(collectionID) ||
		!utils.IsNextTargetExist(ob.targetMgr, collectionID) ||
		!utils.IsNextTargetValid(collectionID, partitionIDs...)
}

func (ob *TargetObserver) IsNextTargetExpired(collectionID int64) bool {
	return time.Since(ob.nextTargetLastUpdate[collectionID]) > nextTargetSurviveTime
}

func (ob *TargetObserver) UpdateNextTarget(collectionID int64, partitionIDs []int64) {
	ctx, cancel := context.WithTimeout(context.Background(), updateNextTargetInterval/2)
	defer cancel()

	err := ob.targetMgr.UpdateNextTarget(ctx, collectionID, partitionIDs, ob.broker)
	if err != nil {
		log.Warn("UpdateNextTarget Failed", zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
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
	ob.targetMgr.UpdateCollectionCurrentTarget(collectionID)
}
