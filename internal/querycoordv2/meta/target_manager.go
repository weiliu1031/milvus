package meta

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type TargetManager struct {
	// all read segment/channel operation happens on Current -> only current target are visible to outer
	// all add segment/channel operation happens on Next -> changes can only happen on Next target
	// all remove segment/channel operation happens on Both Current and Next -> delete status should be consistent
	Current *Target
	Next    *Target
}

func NewTargetManager() *TargetManager {
	return &TargetManager{
		Current: NewTarget(),
		Next:    NewTarget(),
	}
}

func (mgr *TargetManager) UpdateCollectionCurrentTarget(collectionID int64) {
	mgr.Current.RWMutex.Lock()
	defer mgr.Current.RWMutex.Unlock()
	mgr.Next.RWMutex.Lock()
	defer mgr.Next.RWMutex.Unlock()

	log.Info("updateCurrentTarget for collection start", zap.Int64("collectionID", collectionID),
		zap.Int64("PartitionID", collectionID))

	// add Next to current
	newHistoricalSegments := mgr.Next.getHistoricalSegmentIDsByCollection(collectionID)
	newChannels := mgr.Next.getDmChannels(collectionID)

	// can not use an empty Next target to update current target
	if len(newChannels) == 0 {
		return
	}

	// delete current
	mgr.Current.removeCollection(collectionID)

	for ID, segment := range newHistoricalSegments {
		mgr.Current.updateSegment(segment)
		mgr.Next.removeSegment(ID)
	}

	for name, channel := range newChannels {
		mgr.Current.updateDmChannel(channel)
		mgr.Next.removeDmChannel(name)
	}

	log.Info("updateCurrentTarget finish for collection", zap.Int64("collectionID", collectionID),
		zap.Int64("PartitionID", collectionID))
}

// UpdatePartitionCurrentTarget for load partition to trigger Next target to current target
func (mgr *TargetManager) UpdatePartitionCurrentTarget(collectionID int64, partitionIDs ...int64) {
	mgr.Current.RWMutex.Lock()
	defer mgr.Current.RWMutex.Unlock()
	mgr.Next.RWMutex.Lock()
	defer mgr.Next.RWMutex.Unlock()

	log.Info("updateCurrentTarget, for partition", zap.Int64("collectionID", collectionID),
		zap.Int64s("PartitionID", partitionIDs))

	newHistoricalSegments := mgr.Next.getHistoricalSegmentIDsByPartition(partitionIDs...)
	newChannels := mgr.Next.getDmChannels(collectionID)

	if len(newChannels) == 0 {
		return
	}

	// delete from current
	for _, partitionID := range partitionIDs {
		mgr.Current.removePartition(partitionID)
	}

	for ID, segment := range newHistoricalSegments {
		mgr.Current.updateSegment(segment)
		mgr.Next.removeSegment(ID)
	}

	for name, channel := range newChannels {
		mgr.Current.updateDmChannel(channel)
		mgr.Next.removeDmChannel(name)
	}

	log.Info("updateCurrentTarget finish for partition", zap.Int64("collectionID", collectionID),
		zap.Int64s("PartitionID", partitionIDs))
}

func (mgr *TargetManager) UpdateNextTarget(ctx context.Context, collectionID int64, partitionIDs []int64, broker Broker) error {
	mgr.Current.RWMutex.Lock()
	defer mgr.Current.RWMutex.Unlock()
	mgr.Next.RWMutex.Lock()
	defer mgr.Next.RWMutex.Unlock()

	log.Info("UpdateNextTarget start", zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))

	// clear old Next target first if exists.
	mgr.Next.removeCollection(collectionID)

	dmChannels := make(map[string][]*datapb.VchannelInfo)
	allBinlogs := make(map[int64][]*datapb.SegmentBinlogs, 0)
	for _, partitionID := range partitionIDs {
		log.Debug("get recovery info...",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID))
		vChannelInfos, binlogs, err := broker.GetRecoveryInfo(ctx, collectionID, partitionID)
		if err != nil {
			return err
		}

		allBinlogs[partitionID] = binlogs

		for _, info := range vChannelInfos {
			channelName := info.GetChannelName()
			dmChannels[channelName] = append(dmChannels[channelName], info)
		}
	}

	// Merge and register channels
	for _, channels := range dmChannels {
		dmChannel := mgr.mergeDmChannelInfo(channels)
		mgr.Next.addDmChannel(dmChannel)
	}

	// Register segments
	for partitionID, binlogs := range allBinlogs {
		for _, segmentBinlog := range binlogs {
			mgr.Next.addSegment(&datapb.SegmentInfo{
				ID:            segmentBinlog.GetSegmentID(),
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				InsertChannel: segmentBinlog.GetInsertChannel(),
				NumOfRows:     segmentBinlog.GetNumOfRows(),
				Binlogs:       segmentBinlog.GetFieldBinlogs(),
				Statslogs:     segmentBinlog.GetStatslogs(),
				Deltalogs:     segmentBinlog.GetDeltalogs(),
			})
		}
	}

	log.Info("UpdateNextTarget finish", zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

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
	mgr.Current.RWMutex.Lock()
	defer mgr.Current.RWMutex.Unlock()
	mgr.Next.RWMutex.Lock()
	defer mgr.Next.RWMutex.Unlock()

	log.Info("remove collection from current target", zap.Int64("collectionID", collectionID))
	mgr.Current.removeCollection(collectionID)

	log.Info("remove collection from Next target", zap.Int64("collectionID", collectionID))
	mgr.Next.removeCollection(collectionID)
}

// RemovePartition removes all segment in the given partition,
// NOTE: this doesn't remove any channel even the given one is the only partition
func (mgr *TargetManager) RemovePartition(partitionID int64) {
	mgr.Current.RWMutex.Lock()
	defer mgr.Current.RWMutex.Unlock()
	mgr.Next.RWMutex.Lock()
	defer mgr.Next.RWMutex.Unlock()

	log.Info("remove partition from current targets", zap.Int64("partitionID", partitionID))
	mgr.Current.removePartition(partitionID)

	log.Info("remove partition from Next targets", zap.Int64("partitionID", partitionID))
	mgr.Next.removePartition(partitionID)
}
