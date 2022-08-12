package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type TargetManager struct {
	rwmutex sync.RWMutex

	segments        map[int64]*datapb.SegmentInfo
	growingSegments typeutil.UniqueSet // Growing segments to release
	dmChannels      map[string]*DmChannel
}

func NewTargetManager() *TargetManager {
	return &TargetManager{
		segments:        make(map[int64]*datapb.SegmentInfo),
		growingSegments: make(typeutil.UniqueSet),
		dmChannels:      make(map[string]*DmChannel),
	}
}

// RemoveCollection removes all channels and segments in the given collection
func (mgr *TargetManager) RemoveCollection(collectionID int64) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	for _, segment := range mgr.segments {
		if segment.CollectionID == collectionID {
			delete(mgr.segments, segment.ID)
		}
	}
	for _, dmChannel := range mgr.dmChannels {
		if dmChannel.CollectionID == collectionID {
			delete(mgr.dmChannels, dmChannel.ChannelName)
		}
	}
}

// RemovePartition removes all segment in the given partition,
// NOTE: this doesn't remove any channel even the given one is the only partition
func (mgr *TargetManager) RemovePartition(partitionID int64) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	for _, segment := range mgr.segments {
		if segment.GetPartitionID() == partitionID {
			delete(mgr.segments, segment.ID)
		}
	}
}

func (mgr *TargetManager) AddSegment(segments ...*datapb.SegmentInfo) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	mgr.addSegment(segments...)
}

func (mgr *TargetManager) addSegment(segments ...*datapb.SegmentInfo) {
	for _, segment := range segments {
		mgr.segments[segment.GetID()] = segment
	}
}

func (mgr *TargetManager) ContainSegment(id int64) bool {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.containSegment(id)
}

func (mgr *TargetManager) containSegment(id int64) bool {
	_, ok := mgr.segments[id]
	return ok
}

func (mgr *TargetManager) GetSegments(collectionID int64, partitionIDs ...int64) []*datapb.SegmentInfo {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	segments := make([]*datapb.SegmentInfo, 0)
	for _, segment := range mgr.segments {
		if segment.CollectionID == collectionID &&
			(len(partitionIDs) == 0 || funcutil.SliceContain(partitionIDs, segment.PartitionID)) {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (mgr *TargetManager) GetSegmentsByCollection(collection int64, partitions ...int64) []*datapb.SegmentInfo {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	segments := make([]*datapb.SegmentInfo, 0)
	for _, segment := range mgr.segments {
		if segment.CollectionID == collection &&
			(len(partitions) == 0 || funcutil.SliceContain(partitions, segment.PartitionID)) {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (mgr *TargetManager) HandoffSegment(dest *datapb.SegmentInfo, sources ...int64) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	dest.CompactionFrom = sources
	dest.CreatedByCompaction = true
	mgr.addSegment(dest)
}

func (mgr *TargetManager) AddDmChannel(channels ...*DmChannel) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	for _, channel := range channels {
		mgr.dmChannels[channel.ChannelName] = channel
	}
}

func (mgr *TargetManager) ContainDmChannel(channel string) bool {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	_, ok := mgr.dmChannels[channel]
	return ok
}

func (mgr *TargetManager) GetDmChannelsByCollection(collectionID int64) []*DmChannel {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	channels := make([]*DmChannel, 0)
	for _, channel := range mgr.dmChannels {
		if channel.GetCollectionID() == collectionID {
			channels = append(channels, channel)
		}
	}
	return channels
}
