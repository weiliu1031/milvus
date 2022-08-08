package observers

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

const (
	loadTimeout = 10 * time.Minute
)

type CollectionObserver struct {
	stopCh    chan struct{}
	dist      *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager
}

func NewCollectionObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
) *CollectionObserver {
	return &CollectionObserver{
		stopCh:    make(chan struct{}),
		dist:      dist,
		meta:      meta,
		targetMgr: targetMgr,
	}
}

func (observer *CollectionObserver) Start(ctx context.Context) {
	const observePeriod = time.Second
	go func() {
		ticker := time.NewTicker(observePeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Info("CollectionObserver stopped due to context canceled")
				return

			case <-observer.stopCh:
				log.Info("CollectionObserver stopped")
				return

			case <-ticker.C:
				observer.Observe()
			}
		}
	}()
}

func (observer *CollectionObserver) Stop() {
	close(observer.stopCh)
}

func (observer *CollectionObserver) Observe() {
	observer.observeTimeout()
	observer.observeLoadStatus()
}

func (observer *CollectionObserver) observeTimeout() {
	collections := observer.meta.CollectionManager.GetAllCollections()
	log.Info("observes collections timeout", zap.Int("collection-num", len(collections)))
	for _, collection := range collections {
		if collection.GetStatus() != querypb.LoadStatus_Loading ||
			time.Now().Before(collection.CreatedAt.Add(loadTimeout)) {
			continue
		}

		observer.meta.CollectionManager.RemoveCollection(collection.GetCollectionID())
		observer.meta.ReplicaManager.RemoveCollection(collection.GetCollectionID())
		observer.targetMgr.RemoveCollection(collection.GetCollectionID())
	}

	partitions := observer.meta.CollectionManager.GetAllPartitions()
	log.Info("observes partitions timeout", zap.Int("partition-num", len(partitions)))
	for _, partition := range partitions {
		if partition.GetStatus() != querypb.LoadStatus_Loading &&
			time.Now().After(partition.CreatedAt.Add(loadTimeout)) {
			continue
		}

		observer.meta.CollectionManager.RemoveCollection(partition.GetCollectionID())
		if observer.meta.CollectionManager.GetReplicaNumber(partition.GetCollectionID()) <= 0 { // All partitions have been released
			observer.meta.ReplicaManager.RemoveCollection(partition.GetCollectionID())
			observer.targetMgr.RemoveCollection(partition.GetCollectionID())
		} else {
			observer.targetMgr.RemovePartition(partition.GetPartitionID())
		}
	}
}

func (observer *CollectionObserver) observeLoadStatus() {
	collections := observer.meta.CollectionManager.GetAllCollections()
	log.Info("observe collections status", zap.Int("collection-num", len(collections)))
	for _, collection := range collections {
		if collection.GetStatus() != querypb.LoadStatus_Loading {
			continue
		}
		observer.observeCollectionLoadStatus(collection)
	}

	partitions := observer.meta.CollectionManager.GetAllPartitions()
	log.Info("observe partitions status", zap.Int("collection-num", len(partitions)))
	for _, partition := range partitions {
		if partition.GetStatus() != querypb.LoadStatus_Loading {
			continue
		}
		observer.observePartitionLoadStatus(partition)
	}
}

func (observer *CollectionObserver) observeCollectionLoadStatus(collection *meta.Collection) {
	log := log.With(zap.Int64("collection-id", collection.GetCollectionID()))

	segmentTargets := observer.targetMgr.GetSegmentsByCollection(collection.GetCollectionID())
	channelTargets := observer.targetMgr.GetDmChannelsByCollection(collection.GetCollectionID())
	targetNum := len(segmentTargets) + len(channelTargets)
	log.Info("collection targets",
		zap.Int("segment-target-num", len(segmentTargets)),
		zap.Int("channel-target-num", len(channelTargets)),
		zap.Int("total-target-num", targetNum))
	if targetNum == 0 {
		log.Info("load collection failed, will clear it later")
		return
	}

	loadedCount := 0
	for _, segment := range segmentTargets {
		group := utils.GroupNodesByReplica(observer.meta.ReplicaManager,
			collection.GetCollectionID(),
			observer.dist.LeaderViewManager.GetSegmentDist(segment.GetID()))
		if len(group) >= int(collection.GetReplicaNumber()) {
			loadedCount++
		}
	}
	for _, channel := range channelTargets {
		group := utils.GroupNodesByReplica(observer.meta.ReplicaManager,
			collection.GetCollectionID(),
			observer.dist.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
		if len(group) >= int(collection.GetReplicaNumber()) {
			loadedCount++
		}
	}

	collection = collection.Clone()
	collection.LoadPercentage = int32(loadedCount / targetNum)
	if loadedCount >= len(segmentTargets)+len(channelTargets) {
		collection.Status = querypb.LoadStatus_Loaded
		observer.meta.CollectionManager.PutCollection(collection)
	} else {
		observer.meta.CollectionManager.PutCollectionWithoutSave(collection)
	}
	log.Info("collection load status updated",
		zap.Int32("load-percentage", collection.LoadPercentage),
		zap.Int32("collection-status", int32(collection.GetStatus())))
}

func (observer *CollectionObserver) observePartitionLoadStatus(partition *meta.Partition) {
	log := log.With(
		zap.Int64("collection-id", partition.GetCollectionID()),
		zap.Int64("partition-id", partition.GetPartitionID()),
	)

	segmentTargets := observer.targetMgr.GetSegmentsByCollection(partition.GetCollectionID(), partition.GetPartitionID())
	channelTargets := observer.targetMgr.GetDmChannelsByCollection(partition.GetCollectionID())
	targetNum := len(segmentTargets) + len(channelTargets)
	log.Info("partition targets",
		zap.Int("segment-target-num", len(segmentTargets)),
		zap.Int("channel-target-num", len(channelTargets)),
		zap.Int("total-target-num", targetNum))
	if targetNum == 0 {
		log.Info("load partition failed, will clear it later")
		return
	}

	loadedCount := 0
	for _, segment := range segmentTargets {
		group := utils.GroupNodesByReplica(observer.meta.ReplicaManager,
			partition.GetCollectionID(),
			observer.dist.LeaderViewManager.GetSegmentDist(segment.GetID()))
		if len(group) >= int(partition.GetReplicaNumber()) {
			loadedCount++
		}
	}
	for _, channel := range channelTargets {
		group := utils.GroupNodesByReplica(observer.meta.ReplicaManager,
			partition.GetCollectionID(),
			observer.dist.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
		if len(group) >= int(partition.GetReplicaNumber()) {
			loadedCount++
		}
	}

	partition = partition.Clone()
	partition.LoadPercentage = int32(loadedCount / targetNum)
	if loadedCount >= len(segmentTargets)+len(channelTargets) {
		partition.Status = querypb.LoadStatus_Loaded
		observer.meta.CollectionManager.PutPartition(partition)
	} else {
		observer.meta.CollectionManager.PutPartitionWithoutSave(partition)
	}
	log.Info("partition load status updated",
		zap.Int32("load-percentage", partition.LoadPercentage),
		zap.Int32("partition-status", int32(partition.GetStatus())))
}
