package observers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Params is param table of query coordinator
var Params paramtable.ComponentParam

const (
	checkHandOffResultInterval = 5 * time.Second
)

type HandoffHandler struct {
	store    *meta.MetaStore
	c        chan struct{}
	wg       *sync.WaitGroup
	meta     *meta.Meta
	dist     *meta.DistributionManager
	target   *meta.TargetManager
	broker   *meta.CoordinatorBroker
	revision int64
}

func newHandoffHandler(
	ctx context.Context,
	store *meta.MetaStore,
	meta *meta.Meta,
	dist *meta.DistributionManager,
	target *meta.TargetManager,
	broker *meta.CoordinatorBroker) *HandoffHandler {
	h := &HandoffHandler{
		store:  store,
		c:      make(chan struct{}),
		meta:   meta,
		dist:   dist,
		target: target,
		broker: broker,
	}

	h.wg.Add(1)
	go h.Start(ctx)

	return h
}

func (ho *HandoffHandler) Start(ctx context.Context) {
	defer ho.wg.Done()
	log.Info("Start watch segment handoff loop")

	ticker := time.NewTicker(checkHandOffResultInterval)
	watchChan := ho.store.WatchHandoffEvent(ho.revision + 1)
	for {
		select {
		case <-ctx.Done():
			log.Info("Close handoff handler due to context done!")
		case <-ho.c:
			log.Info("close handoff handler")

		case resp, ok := <-watchChan:
			if !ok {
				log.Error("Watch segment handoff loop failed because watch channel is closed!")
				panic("failed to handle handoff event, handoff handler exit...")
			}

			if err := resp.Err(); err != nil {
				log.Error("receive error handoff event from etcd, %s, %s",
					zap.String("prefix", util.HandoffSegmentPrefix),
					zap.Error(err))
				panic("failed to handle handoff event, handoff handler exit...")
			}

			for _, event := range resp.Events {
				segmentInfo := &querypb.SegmentInfo{}
				err := proto.Unmarshal(event.Kv.Value, segmentInfo)
				if err != nil {
					log.Error("watch segment handoff loop failed", zap.Error(err))
					continue
				}

				switch event.Type {
				case mvccpb.PUT:
					ho.handleHandOffEvent(segmentInfo)
				default:
					log.Warn("receive handoff event", zap.String("type", event.Type.String()))
				}
			}

		case <-ticker.C:
			collections := ho.meta.CollectionManager.GetAllCollections()
			partitions := ho.meta.CollectionManager.GetAllPartitions()
			collectionIDs := utils.Collect(collections, partitions)

			// todo: parallel
			for _, collectionID := range collectionIDs {
				// try release compact source which compactTo segment already load on all replica
				ho.tryRelease(ctx, collectionID)
				// try clean handoff task
				ho.tryClean(ctx, collectionID)
			}
		}
	}
}

func (ho *HandoffHandler) tryClean(ctx context.Context, collectionID typeutil.UniqueID) {
	handOffSegments := ho.getHandOffSegment(collectionID)
	for _, segmentInfo := range handOffSegments {
		if ho.isHandoffDone(segmentInfo) {
			ho.clean(ctx, segmentInfo)
		}
	}
}

func (ho *HandoffHandler) tryHandoff(ctx context.Context, segment *querypb.SegmentInfo) error {
	log.With(zap.Int64("collection", segment.CollectionID),
		zap.Int64("partition", segment.PartitionID),
		zap.Int64("segment", segment.SegmentID))

	if ho.isCollectionLoaded(segment) && !Params.QueryCoordCfg.AutoHandoff {
		// require segment reference lock
		if err := ho.broker.AcquireSegmentsReferLock(ctx, []typeutil.UniqueID{segment.SegmentID}); err != nil {
			log.Warn("HandoffHandler: acquire segment reference lock failed", zap.Error(err))
			return fmt.Errorf("failed to acquire segment refer lock")
		}
		ho.handleHandOffEvent(segment)
	} else {
		// ignore handoff task
		log.Debug("Handoff event trigger failed due to collection/partition is not loaded!")
	}

	return nil
}

func (ho *HandoffHandler) isCollectionLoaded(segmentInfo *querypb.SegmentInfo) bool {
	return (ho.meta.GetCollection(segmentInfo.CollectionID) != nil && ho.meta.GetLoadType(segmentInfo.CollectionID) == querypb.LoadType_LoadCollection) ||
		(ho.meta.GetPartition(segmentInfo.PartitionID) != nil && ho.meta.GetLoadType(segmentInfo.CollectionID) == querypb.LoadType_LoadCollection)
}

func (ho *HandoffHandler) handleHandOffEvent(segment *querypb.SegmentInfo) {
	segmentInfo := &datapb.SegmentInfo{
		ID:                  segment.SegmentID,
		CollectionID:        segment.CollectionID,
		PartitionID:         segment.PartitionID,
		InsertChannel:       segment.DmChannel,
		CreatedByCompaction: segment.GetCreatedByCompaction(),
		CompactionFrom:      segment.GetCompactionFrom(),
	}

	// when handoff event load a segment, it should remove all recursive handoff compact from
	// this it used for query node to release growing segment when load new segment.
	target := ho.target.GetSegments(segmentInfo.GetCollectionID(), segmentInfo.GetPartitionID())
	recursiveCompactFrom := ho.getOverrideSegmentInfo(target, segmentInfo.CompactionFrom...)
	recursiveCompactFrom = append(recursiveCompactFrom, segmentInfo.GetCompactionFrom()...)
	ho.target.HandoffSegment(segmentInfo, recursiveCompactFrom...)
}

func (ho *HandoffHandler) getOverrideSegmentInfo(handOffSegments []*datapb.SegmentInfo, segmentIDs ...typeutil.UniqueID) []typeutil.UniqueID {
	overrideSegments := make([]typeutil.UniqueID, 0)
	for _, segmentID := range segmentIDs {
		for _, segmentInHandoff := range handOffSegments {
			if segmentID == segmentInHandoff.ID {
				toReleaseSegments := ho.getOverrideSegmentInfo(handOffSegments, segmentInHandoff.CompactionFrom...)
				if len(toReleaseSegments) > 0 {
					overrideSegments = append(overrideSegments, toReleaseSegments...)

				}

				overrideSegments = append(overrideSegments, segmentID)
			}
		}
	}

	return overrideSegments
}

func (ho *HandoffHandler) isHandoffDone(segmentInfo *datapb.SegmentInfo) bool {
	for _, compactSegment := range segmentInfo.CompactionFrom {
		if ho.target.ContainSegment(compactSegment) {
			return false
		}
	}

	return true
}

func (ho *HandoffHandler) clean(ctx context.Context, segmentInfo *datapb.SegmentInfo) {
	log.With(zap.Int64("collection", segmentInfo.CollectionID),
		zap.Int64("partition", segmentInfo.PartitionID),
		zap.Int64("segment", segmentInfo.ID))

	if err := ho.broker.ReleaseSegmentReferLock(ctx, []typeutil.UniqueID{segmentInfo.ID}); err != nil {
		log.Warn("HandoffHandler: release segment reference lock failed", zap.Error(err))
	}

	if err := ho.store.RemoveHandoffEvent(segmentInfo); err != nil {
		log.Warn("Clean handoff event from etcd failed", zap.Error(err))
	}
}

func (ho *HandoffHandler) getHandOffSegment(collection typeutil.UniqueID) []*datapb.SegmentInfo {
	// filter handoff segment
	target := ho.target.GetSegments(collection)
	isHandoffSegment := func(segment *datapb.SegmentInfo) bool { return len(segment.CompactionFrom) > 0 }
	return ho.filterSegmentsWithRule(target, isHandoffSegment)
}

func (ho *HandoffHandler) filterSegmentsWithRule(segments []*datapb.SegmentInfo, filter func(*datapb.SegmentInfo) bool) []*datapb.SegmentInfo {
	ret := make([]*datapb.SegmentInfo, 0)
	for _, segment := range segments {
		if filter(segment) {
			ret = append(ret, segment)
		}
	}
	return ret
}

func (ho *HandoffHandler) tryRelease(ctx context.Context, collectionID typeutil.UniqueID) {
	replicas := ho.meta.ReplicaManager.GetByCollection(collectionID)

	// find segments which already load from all replica
	segmentDist := ho.target.GetSegments(collectionID)
	for _, r := range replicas {
		segmentDist = ho.getHandoffSegmentsOnReplica(ctx, r, segmentDist)
	}

	for _, segment := range segmentDist {
		toReleaseSegments := ho.getSegmentToRelease(segmentDist, segment)
		for _, toRelease := range toReleaseSegments {
			ho.target.RemoveSegment(toRelease)
		}
	}
}

// find handoff finished segment from targetDist range
func (ho *HandoffHandler) getHandoffSegmentsOnReplica(ctx context.Context, replica *meta.Replica, targetDist []*datapb.SegmentInfo) []*datapb.SegmentInfo {
	// filter handoff segment
	isHandoffSegment := func(segment *datapb.SegmentInfo) bool { return len(segment.CompactionFrom) > 0 }
	handoffSegments := ho.filterSegmentsWithRule(targetDist, isHandoffSegment)

	// filter already loaded segment
	segmentDist := ho.getSegmentsDist(replica)
	isSegmentInDist := func(segment *datapb.SegmentInfo) bool {
		for _, segmentInDist := range segmentDist {
			if segmentInDist.ID == segment.ID {
				return true
			}
		}
		return false
	}
	alreadyLoadedSegments := ho.filterSegmentsWithRule(handoffSegments, isSegmentInDist)

	// filter index ready segment
	isSegmentIndexReady := func(segment *datapb.SegmentInfo) bool {
		// TODO we should not directly poll the index info, wait for notification should be a better idea.
		_, err := ho.broker.GetIndexInfo(ctx, segment.CollectionID, segment.ID)
		return err == nil
	}
	indexReadySegments := ho.filterSegmentsWithRule(alreadyLoadedSegments, isSegmentIndexReady)

	return indexReadySegments
}

func (ho *HandoffHandler) getSegmentToRelease(handOffSegments []*datapb.SegmentInfo, compactToSegment *datapb.SegmentInfo) []typeutil.UniqueID {
	toReleaseSegments := ho.getOverrideSegmentInfo(handOffSegments, compactToSegment.GetCompactionFrom()...)
	if len(toReleaseSegments) > 0 {
		log.Info("find recursive handoff",
			zap.Int64("collection", compactToSegment.CollectionID),
			zap.Int64("partition", compactToSegment.PartitionID),
			zap.Int64("segment", compactToSegment.ID),
			zap.Int64s("to Release Segment", toReleaseSegments))
	}

	toReleaseSegments = append(toReleaseSegments, compactToSegment.CompactionFrom...)
	return toReleaseSegments
}

func (ho *HandoffHandler) getSegmentsDist(replica *meta.Replica) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	for _, node := range replica.Nodes.Collect() {
		ret = append(ret, ho.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, node)...)
	}
	return ret
}

func (ho *HandoffHandler) Close() {
	close(ho.c)
	ho.wg.Wait()
}
