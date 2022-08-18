package observers

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

const (
	defaultLoadTimeout = 10 * time.Minute
)

type CollectionObserver struct {
	stopCh chan struct{}

	// configs
	loadTimeout time.Duration

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
		stopCh:      make(chan struct{}),
		loadTimeout: defaultLoadTimeout,
		dist:        dist,
		meta:        meta,
		targetMgr:   targetMgr,
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
			time.Now().Before(collection.CreatedAt.Add(observer.loadTimeout)) {
			continue
		}

		log.Info("load collection timeout, cancel it",
			zap.Int64("collection-id", collection.GetCollectionID()),
			zap.Duration("load-time", time.Since(collection.CreatedAt)))
		observer.meta.CollectionManager.RemoveCollection(collection.GetCollectionID())
		observer.meta.ReplicaManager.RemoveCollection(collection.GetCollectionID())
		observer.targetMgr.RemoveCollection(collection.GetCollectionID())
	}

	partitions := observer.meta.CollectionManager.GetAllPartitions()
	log.Info("observes partitions timeout", zap.Int("partition-num", len(partitions)))
	for _, partition := range partitions {
		if partition.GetStatus() != querypb.LoadStatus_Loading ||
			time.Now().Before(partition.CreatedAt.Add(observer.loadTimeout)) {
			continue
		}

		log.Info("load partition timeout, cancel it",
			zap.Int64("collection-id", partition.GetCollectionID()),
			zap.Int64("partition-id", partition.GetPartitionID()),
			zap.Duration("load-time", time.Since(partition.CreatedAt)))
		observer.meta.CollectionManager.RemovePartition(partition.GetPartitionID())
		observer.targetMgr.RemovePartition(partition.GetPartitionID())
		// All partitions have been released
		if !observer.meta.CollectionManager.Exist(partition.GetCollectionID()) {
			observer.meta.ReplicaManager.RemoveCollection(partition.GetCollectionID())
			observer.targetMgr.RemoveCollection(partition.GetCollectionID())
		}
	}
}

func (observer *CollectionObserver) observeLoadStatus() {
	collections := observer.meta.CollectionManager.GetAllCollections()
	log.Info("observe collections status", zap.Int("collection-num", len(collections)))
	for _, collection := range collections {
		if collection.LoadPercentage == 100 {
			continue
		}
		observer.observeCollectionLoadStatus(collection)
	}

	partitions := observer.meta.CollectionManager.GetAllPartitions()
	log.Info("observe partitions status", zap.Int("partition-num", len(partitions)))
	for _, partition := range partitions {
		if partition.LoadPercentage == 100 {
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
		log.Info("collection released, skip it")
		return
	}

	loadedCount := 0
	for _, channel := range channelTargets {
		group := utils.GroupNodesByReplica(observer.meta.ReplicaManager,
			collection.GetCollectionID(),
			observer.dist.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
		if len(group) >= int(collection.GetReplicaNumber()) {
			loadedCount++
		}
	}
	subChannelCount := loadedCount
	for _, segment := range segmentTargets {
		group := utils.GroupNodesByReplica(observer.meta.ReplicaManager,
			collection.GetCollectionID(),
			observer.dist.LeaderViewManager.GetSegmentDist(segment.GetID()))
		if len(group) >= int(collection.GetReplicaNumber()) {
			loadedCount++
		}
	}
	if loadedCount > 0 {
		log.Info("collection load progress",
			zap.Int("sub-channel-count", subChannelCount),
			zap.Int("load-segment-count", loadedCount-subChannelCount),
		)
	}

	collection = collection.Clone()
	collection.LoadPercentage = int32(loadedCount * 100 / targetNum)
	if loadedCount >= len(segmentTargets)+len(channelTargets) {
		collection.Status = querypb.LoadStatus_Loaded
		observer.meta.CollectionManager.UpdateCollection(collection)

		elapsed := time.Since(collection.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
	} else {
		observer.meta.CollectionManager.UpdateCollectionInMemory(collection)
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
		log.Info("partition released, skip it")
		return
	}

	loadedCount := 0
	for _, channel := range channelTargets {
		group := utils.GroupNodesByReplica(observer.meta.ReplicaManager,
			partition.GetCollectionID(),
			observer.dist.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
		if len(group) >= int(partition.GetReplicaNumber()) {
			loadedCount++
		}
	}
	subChannelCount := loadedCount
	for _, segment := range segmentTargets {
		group := utils.GroupNodesByReplica(observer.meta.ReplicaManager,
			partition.GetCollectionID(),
			observer.dist.LeaderViewManager.GetSegmentDist(segment.GetID()))
		if len(group) >= int(partition.GetReplicaNumber()) {
			loadedCount++
		}
	}
	if loadedCount > 0 {
		log.Info("partition load progress",
			zap.Int("sub-channel-count", subChannelCount),
			zap.Int("load-segment-count", loadedCount-subChannelCount))
	}

	partition = partition.Clone()
	partition.LoadPercentage = int32(loadedCount * 100 / targetNum)
	if loadedCount >= len(segmentTargets)+len(channelTargets) {
		partition.Status = querypb.LoadStatus_Loaded
		observer.meta.CollectionManager.PutPartition(partition)

		elapsed := time.Now().Sub(partition.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
	} else {
		observer.meta.CollectionManager.UpdatePartitionInMemory(partition)
	}
	log.Info("partition load status updated",
		zap.Int32("load-percentage", partition.LoadPercentage),
		zap.Int32("partition-status", int32(partition.GetStatus())))
}
