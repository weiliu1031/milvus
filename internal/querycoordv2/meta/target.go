package meta

import (
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

type Target struct {
	RWMutex    sync.RWMutex
	segments   map[int64]*datapb.SegmentInfo
	dmChannels map[string]*DmChannel
}

func NewTarget() *Target {
	return &Target{
		segments:   make(map[int64]*datapb.SegmentInfo),
		dmChannels: make(map[string]*DmChannel)}
}

func (t *Target) GetStreamingSegmentIDs(collectionID int64) map[int64]*datapb.SegmentInfo {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	return t.getStreamingSegmentIDs(collectionID)
}

func (t *Target) getStreamingSegmentIDs(collectionID int64) map[int64]*datapb.SegmentInfo {
	segments := make(map[int64]*datapb.SegmentInfo, 0)
	for _, channel := range t.dmChannels {
		if channel.CollectionID == collectionID {
			for _, segment := range channel.UnflushedSegments {
				segments[segment.GetID()] = segment
			}
		}
	}
	return segments
}

func (t *Target) GetHistoricalSegmentIDsByCollection(collectionID int64) map[int64]*datapb.SegmentInfo {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	return t.getHistoricalSegmentIDsByCollection(collectionID)
}

func (t *Target) getHistoricalSegmentIDsByCollection(collectionID int64) map[int64]*datapb.SegmentInfo {
	segments := make(map[int64]*datapb.SegmentInfo, 0)
	for _, segment := range t.segments {
		if segment.GetCollectionID() == collectionID {
			segments[segment.GetID()] = segment
		}
	}
	return segments
}

func (t *Target) GetHistoricalSegmentIDsByPartition(partitionIDs ...int64) map[int64]*datapb.SegmentInfo {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	return t.getHistoricalSegmentIDsByPartition(partitionIDs...)
}

func (t *Target) getHistoricalSegmentIDsByPartition(partitionIDs ...int64) map[int64]*datapb.SegmentInfo {
	segments := make(map[int64]*datapb.SegmentInfo, 0)
	for _, segment := range t.segments {
		if lo.Contains(partitionIDs, segment.PartitionID) {
			segments[segment.GetID()] = segment
		}
	}
	return segments
}

func (t *Target) GetDmChannels(collectionID int64) map[string]*DmChannel {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	return t.getDmChannels(collectionID)
}

func (t *Target) getDmChannels(collectionID int64) map[string]*DmChannel {
	channels := make(map[string]*DmChannel, 0)
	for _, channel := range t.dmChannels {
		if channel.CollectionID == collectionID {
			channels[channel.ChannelName] = channel
		}
	}

	return channels
}

func (t *Target) GetAllCollections() []int64 {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	collections := make([]int64, 0)
	for _, channel := range t.dmChannels {
		collections = append(collections, channel.CollectionID)
	}

	return collections
}

// RemoveCollection removes all channels and segments in the given collection
func (t *Target) RemoveCollection(collectionID int64) {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	t.removeCollection(collectionID)
}

func (t *Target) removeCollection(collectionID int64) {
	for _, segment := range t.segments {
		if segment.CollectionID == collectionID {
			t.removeSegment(segment.GetID())
		}
	}

	for _, segment := range t.segments {
		if segment.CollectionID == collectionID {
			t.removeSegment(segment.GetID())
		}
	}

	for _, dmChannel := range t.dmChannels {
		if dmChannel.CollectionID == collectionID {
			t.removeDmChannel(dmChannel.GetChannelName())
		}
	}

	for _, dmChannel := range t.dmChannels {
		if dmChannel.CollectionID == collectionID {
			t.removeDmChannel(dmChannel.GetChannelName())
		}
	}
}

// RemovePartition removes all segment in the given partition,
// NOTE: this doesn't remove any channel even the given one is the only partition
func (t *Target) RemovePartition(partitionID int64) {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()
	t.removePartition(partitionID)
}

func (t *Target) removePartition(partitionID int64) {
	log.Info("remove partition from targets", zap.Int64("partitionID", partitionID))
	for _, segment := range t.segments {
		if segment.PartitionID == partitionID {
			t.removeSegment(segment.GetID())
		}
	}

	for _, segment := range t.segments {
		if segment.PartitionID == partitionID {
			t.removeSegment(segment.GetID())
		}
	}
}

func (t *Target) RemoveSegment(segmentID int64) {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	delete(t.segments, segmentID)
}

func (t *Target) removeSegment(segmentID int64) {
	log.Info("remove segment from target", zap.Int64("segmentID", segmentID))
	delete(t.segments, segmentID)
}

// AddSegment adds segment into target set,
// requires CollectionID, PartitionID, InsertChannel, SegmentID are set
func (t *Target) AddSegment(segments ...*datapb.SegmentInfo) {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	t.addSegment(segments...)
}

func (t *Target) ContainSegment(id int64) bool {
	t.RWMutex.RLock()
	defer t.RWMutex.RUnlock()

	return t.containSegment(id)
}

func (t *Target) addSegment(segments ...*datapb.SegmentInfo) {
	for _, segment := range segments {
		log.Info("add segment into target",
			zap.Int64("segmentID", segment.GetID()),
			zap.Int64("collectionID", segment.GetCollectionID()),
		)
		t.segments[segment.GetID()] = segment
	}
}

func (t *Target) containSegment(id int64) bool {
	_, ok := t.segments[id]
	return ok
}

func (t *Target) UpdateSegment(segment *datapb.SegmentInfo) {
	t.RWMutex.RLock()
	defer t.RWMutex.RUnlock()

	t.updateSegment(segment)
}

func (t *Target) updateSegment(segment *datapb.SegmentInfo) {
	t.segments[segment.GetID()] = segment
}

func (t *Target) UpdateDmChannel(channel *DmChannel) {
	t.RWMutex.RLock()
	defer t.RWMutex.RUnlock()

	t.updateDmChannel(channel)
}

func (t *Target) updateDmChannel(channel *DmChannel) {

	t.dmChannels[channel.GetChannelName()] = channel
}

func (t *Target) GetSegmentsByCollection(collection int64, partitions ...int64) []*datapb.SegmentInfo {
	t.RWMutex.RLock()
	defer t.RWMutex.RUnlock()

	segments := make([]*datapb.SegmentInfo, 0)
	for _, segment := range t.segments {
		if segment.CollectionID == collection &&
			(len(partitions) == 0 || funcutil.SliceContain(partitions, segment.PartitionID)) {
			segments = append(segments, segment)
		}
	}

	return segments
}

// AddDmChannel adds a channel into target set,
// requires CollectionID, ChannelName are set
func (t *Target) AddDmChannel(channels ...*DmChannel) {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	t.addDmChannel(channels...)
}

func (t *Target) addDmChannel(channels ...*DmChannel) {
	for _, channel := range channels {
		log.Info("add channel into target",
			zap.Int64("collectionID", channel.GetCollectionID()),
			zap.String("channel", channel.GetChannelName()))
		t.dmChannels[channel.ChannelName] = channel
	}
}

func (t *Target) GetDmChannel(channel string) *DmChannel {
	t.RWMutex.RLock()
	defer t.RWMutex.RUnlock()
	for _, ch := range t.dmChannels {
		if ch.ChannelName == channel {
			return ch
		}
	}
	return nil
}

func (t *Target) ContainDmChannel(channel string) bool {
	t.RWMutex.RLock()
	defer t.RWMutex.RUnlock()

	_, ok := t.dmChannels[channel]
	return ok
}

func (t *Target) RemoveDmChannel(channel string) {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	t.removeDmChannel(channel)
}

func (t *Target) removeDmChannel(channel string) {
	delete(t.dmChannels, channel)
	log.Info("remove channel from target", zap.String("channel", channel))
}

func (t *Target) GetDmChannelsByCollection(collectionID int64) []*DmChannel {
	t.RWMutex.RLock()
	defer t.RWMutex.RUnlock()

	channels := make([]*DmChannel, 0)
	for _, channel := range t.dmChannels {
		if channel.GetCollectionID() == collectionID {
			channels = append(channels, channel)
		}
	}
	return channels
}

func (t *Target) GetHistoricalSegment(id int64) *datapb.SegmentInfo {
	t.RWMutex.RLock()
	defer t.RWMutex.RUnlock()

	for _, s := range t.segments {
		if s.GetID() == id {
			return s
		}
	}
	return nil
}
