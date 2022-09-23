package meta

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type TargetScope = int

const (
	CurrentTarget TargetScope = iota
	NextTarget
)

type TargetManager struct {
	rwMutex sync.RWMutex

	meta   *Meta
	broker Broker

	// all read segment/channel operation happens on current -> only current target are visible to outer
	// all add segment/channel operation happens on next -> changes can only happen on next target
	// all remove segment/channel operation happens on Both current and next -> delete status should be consistent
	current *target
	next    *target
}

func NewTargetManager(meta *Meta, broker Broker) *TargetManager {
	return &TargetManager{
		meta:    meta,
		broker:  broker,
		current: NewTarget(),
		next:    NewTarget(),
	}
}

func (mgr *TargetManager) UpdateCollectionCurrentTarget(collectionID int64) error {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	log.Info("start to update current target for collection",
		zap.Int64("collectionID", collectionID))

	newTarget := mgr.next.getCollectionTarget(collectionID)
	if newTarget == nil || newTarget.isEmpty() {
		log.Info("next target does not exist, skip it",
			zap.Int64("collectionID", collectionID))
		return nil
	}
	mgr.current.updateCollectionTarget(collectionID, newTarget)
	mgr.next.removeCollectionTarget(collectionID)

	log.Info("finish to update current target for collection",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("segments", newTarget.GetAllSegmentIDs()),
		zap.Strings("channels", newTarget.GetAllDmChannelNames()))
	return nil
}

// UpdatePartitionCurrentTarget for load partition to trigger Next target to current target
func (mgr *TargetManager) UpdatePartitionCurrentTarget(collectionID int64, partitionIDs ...int64) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	log.Info("start to update current target for partition",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("PartitionIDs", partitionIDs))

	newTarget := mgr.next.getCollectionTarget(collectionID)

	if newTarget == nil || newTarget.isEmpty() {
		log.Info("next target does not exist, skip it",
			zap.Int64("collectionID", collectionID),
			zap.Int64s("PartitionIDs", partitionIDs))
		return
	}
	mgr.current.updateCollectionTarget(collectionID, newTarget)
	mgr.next.removeCollectionTarget(collectionID)

	log.Info("finish to update current target for partition",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("PartitionIDs", partitionIDs),
		zap.Int64s("segments", newTarget.GetAllSegmentIDs()),
		zap.Strings("channels", newTarget.GetAllDmChannelNames()))
}

func (mgr *TargetManager) UpdateCollectionNextTarget(collectionID int64) error {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	log.Info("start to update next targets for collection",
		zap.Int64("collectionID", collectionID))

	partitionIDs, err := GetPartitions(mgr.meta.CollectionManager, mgr.broker, collectionID)
	if err != nil {
		log.Error("failed to update next targets for collection",
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Error(err))
		return err
	}

	newTarget, err := mgr.pullNextTarget(collectionID, partitionIDs...)
	if err != nil {
		log.Error("failed to update next targets for collection",
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Error(err))
		return err
	}

	mgr.next.updateCollectionTarget(collectionID, newTarget)

	log.Info("finish to update next targets for collection",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("segments", newTarget.GetAllSegmentIDs()),
		zap.Strings("channels", newTarget.GetAllDmChannelNames()))
	return nil
}

func (mgr *TargetManager) UpdatePartitionNextTarget(collectionID int64, partitionIDs ...int64) error {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	log.Info("start to update next targets for partition",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))

	newTarget, err := mgr.pullNextTarget(collectionID, partitionIDs...)
	if err != nil {
		log.Error("failed to update next targets for collection",
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Error(err))
		return err
	}

	mgr.next.updateCollectionTarget(collectionID, newTarget)

	log.Info("finish to update next targets for collection",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("segments", newTarget.GetAllSegmentIDs()),
		zap.Strings("channels", newTarget.GetAllDmChannelNames()))
	return nil

}

func (mgr *TargetManager) pullNextTarget(collectionID int64, partitionIDs ...int64) (*CollectionTarget, error) {
	log.Info("start to pull next targets for partition",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))

	channelInfos := make(map[string][]*datapb.VchannelInfo)
	segments := make(map[int64]*datapb.SegmentInfo, 0)
	for _, partitionID := range partitionIDs {
		log.Debug("get recovery info...",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID))
		vChannelInfos, binlogs, err := mgr.broker.GetRecoveryInfo(context.TODO(), collectionID, partitionID)
		if err != nil {
			return nil, err
		}

		for _, binlog := range binlogs {
			segments[binlog.GetSegmentID()] = &datapb.SegmentInfo{
				ID:            binlog.GetSegmentID(),
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				InsertChannel: binlog.GetInsertChannel(),
				NumOfRows:     binlog.GetNumOfRows(),
				Binlogs:       binlog.GetFieldBinlogs(),
				Statslogs:     binlog.GetStatslogs(),
				Deltalogs:     binlog.GetDeltalogs(),
			}
		}

		for _, info := range vChannelInfos {
			channelInfos[info.GetChannelName()] = append(channelInfos[info.GetChannelName()], info)
		}
	}

	dmChannels := make(map[string]*DmChannel)
	for _, infos := range channelInfos {
		merged := mgr.mergeDmChannelInfo(infos)
		dmChannels[merged.GetChannelName()] = merged
	}

	return NewCollectionTarget(segments, dmChannels), nil
}

// todo: dedup?
func (mgr *TargetManager) mergeDmChannelInfo(infos []*datapb.VchannelInfo) *DmChannel {
	var dmChannel *DmChannel

	for _, info := range infos {
		if dmChannel == nil {
			dmChannel = DmChannelFromVChannel(info)
			continue
		}

		if info.SeekPosition.GetTimestamp() < dmChannel.SeekPosition.GetTimestamp() {
			dmChannel.SeekPosition = info.SeekPosition
		}
		dmChannel.DroppedSegmentIds = append(dmChannel.DroppedSegmentIds, info.DroppedSegmentIds...)
		dmChannel.UnflushedSegmentIds = append(dmChannel.UnflushedSegmentIds, info.UnflushedSegmentIds...)
		dmChannel.FlushedSegmentIds = append(dmChannel.FlushedSegmentIds, info.FlushedSegmentIds...)
	}

	return dmChannel
}

// RemoveCollection removes all channels and segments in the given collection
func (mgr *TargetManager) RemoveCollection(collectionID int64) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	log.Info("remove collection from targets",
		zap.Int64("collectionID", collectionID))

	mgr.current.removeCollectionTarget(collectionID)
	mgr.next.removeCollectionTarget(collectionID)
}

// RemovePartition removes all segment in the given partition,
// NOTE: this doesn't remove any channel even the given one is the only partition
func (mgr *TargetManager) RemovePartition(collectionID int64, partitionIDs ...int64) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	log.Info("remove partition from current targets",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))

	mgr.current.removeCollectionTarget(collectionID)
	mgr.next.removeCollectionTarget(collectionID)
}

func (mgr *TargetManager) getTarget(scope TargetScope) *target {
	if scope == CurrentTarget {
		return mgr.current
	} else {
		return mgr.next
	}
}

func (mgr *TargetManager) GetStreamingSegmentsByCollection(collectionID int64,
	scope TargetScope) map[int64]*datapb.SegmentInfo {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targetMap := mgr.getTarget(scope)
	collectionTarget := targetMap.getCollectionTarget(collectionID)

	if collectionTarget == nil {
		log.Info("collection target not found when getting streaming segments",
			zap.Int64("collectionID", collectionID),
			zap.Int("targetType", scope))
		return map[int64]*datapb.SegmentInfo{}
	}

	segments := make(map[int64]*datapb.SegmentInfo)
	for _, channel := range collectionTarget.GetAllDmChannels() {
		for _, s := range channel.GetUnflushedSegments() {
			segments[s.GetID()] = s
		}
	}

	return segments
}

func (mgr *TargetManager) GetHistoricalSegmentsByCollection(collectionID int64,
	scope TargetScope) map[int64]*datapb.SegmentInfo {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targetMap := mgr.getTarget(scope)
	collectionTarget := targetMap.getCollectionTarget(collectionID)

	if collectionTarget == nil {
		log.Info("collection target not found when getting historical segments",
			zap.Int64("collectionID", collectionID),
			zap.Int("targetType", scope))
		return map[int64]*datapb.SegmentInfo{}
	}
	return collectionTarget.GetAllSegments()
}

func (mgr *TargetManager) GetHistoricalSegmentsByPartition(collectionID int64,
	partitionID int64, scope TargetScope) map[int64]*datapb.SegmentInfo {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targetMap := mgr.getTarget(scope)
	collectionTarget := targetMap.getCollectionTarget(collectionID)

	if collectionTarget == nil {
		log.Info("collection target not found when getting historical segments",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID),
			zap.Int("targetType", scope))
		return map[int64]*datapb.SegmentInfo{}
	}

	segments := make(map[int64]*datapb.SegmentInfo)
	for _, s := range collectionTarget.GetAllSegments() {
		if s.GetPartitionID() == partitionID {
			segments[s.GetID()] = s
		}
	}

	return segments
}

func (mgr *TargetManager) GetDmChannelsByCollection(collectionID int64, scope TargetScope) map[string]*DmChannel {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targetMap := mgr.getTarget(scope)
	collectionTarget := targetMap.getCollectionTarget(collectionID)

	if collectionTarget == nil {
		log.Info("collection target not found when getting dmChannels",
			zap.Int64("collectionID", collectionID),
			zap.Int("targetType", scope))
		return map[string]*DmChannel{}
	}
	return collectionTarget.GetAllDmChannels()
}

func (mgr *TargetManager) GetDmChannel(collectionID int64, channel string, scope TargetScope) *DmChannel {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targetMap := mgr.getTarget(scope)
	collectionTarget := targetMap.getCollectionTarget(collectionID)

	if collectionTarget == nil {
		log.Info("collection target not found when getting dmChannel",
			zap.Int64("collectionID", collectionID),
			zap.String("channelName", channel),
			zap.Int("targetType", scope))
		return nil
	}
	return collectionTarget.GetAllDmChannels()[channel]
}

func (mgr *TargetManager) GetHistoricalSegment(collectionID int64, id int64, scope TargetScope) *datapb.SegmentInfo {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()
	targetMap := mgr.getTarget(scope)
	collectionTarget := targetMap.getCollectionTarget(collectionID)

	if collectionTarget == nil {
		log.Info("collection target not found when getting historical segment",
			zap.Int64("collectionID", collectionID),
			zap.Int64("segmentID", id),
			zap.Int("targetType", scope))
		return nil
	}
	return collectionTarget.GetAllSegments()[id]
}

// todo: refine test case and remove this method
func (mgr *TargetManager) AddSegment(segment *datapb.SegmentInfo) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	collectionTarget := mgr.next.collectionTargetMap[segment.GetCollectionID()]
	if collectionTarget == nil {
		log.Info("collection target not found when add segment, add a new one",
			zap.Int64("collectionID", segment.GetCollectionID()),
			zap.Int64("segmentID", segment.GetID()))
		collectionTarget = NewCollectionTarget(map[int64]*datapb.SegmentInfo{}, map[string]*DmChannel{})
		mgr.next.updateCollectionTarget(segment.GetCollectionID(), collectionTarget)
	}
	collectionTarget.GetAllSegments()[segment.GetID()] = segment
}

// todo: refine test case and remove this method
func (mgr *TargetManager) AddDmChannel(channel *DmChannel) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	collectionTarget := mgr.next.collectionTargetMap[channel.GetCollectionID()]
	if collectionTarget == nil {
		log.Info("collection target not found when add channel, add a new one",
			zap.Int64("collectionID", channel.GetCollectionID()),
			zap.String("channelName", channel.GetChannelName()))
		collectionTarget = NewCollectionTarget(map[int64]*datapb.SegmentInfo{}, map[string]*DmChannel{})
		mgr.next.updateCollectionTarget(channel.GetCollectionID(), collectionTarget)
	}
	collectionTarget.GetAllDmChannels()[channel.ChannelName] = channel
}

func GetPartitions(collectionMgr *CollectionManager, broker Broker, collectionID int64) ([]int64, error) {
	collection := collectionMgr.GetCollection(collectionID)
	if collection != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		partitions, err := broker.GetPartitions(ctx, collectionID)
		return partitions, err
	}

	partitions := collectionMgr.GetPartitionsByCollection(collectionID)
	if partitions != nil {
		return lo.Map(partitions, func(partition *Partition, i int) int64 {
			return partition.PartitionID
		}), nil
	}

	// todo(yah01): replace this error with a defined error
	return nil, fmt.Errorf("collection/partition not loaded")
}
