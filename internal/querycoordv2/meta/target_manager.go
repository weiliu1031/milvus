package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type TargetManager struct {
	rwmutex sync.RWMutex

	segments        map[int64]*Segment
	growingSegments typeutil.UniqueSet // Growing segments to release
	dmChannels      map[string]*DmChannel
	deltaChannels   map[string]*DeltaChannel
}

func NewTargetManager() *TargetManager {
	return &TargetManager{
		segments:        make(map[int64]*Segment),
		growingSegments: make(typeutil.UniqueSet),
		dmChannels:      make(map[string]*DmChannel),
		deltaChannels:   make(map[string]*DeltaChannel),
	}
}

func (mgr *TargetManager) AddSegment(segments ...*Segment) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	mgr.addSegment(segments...)
}

func (mgr *TargetManager) addSegment(segments ...*Segment) {
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

func (mgr *TargetManager) GetSegments(collectionID int64, partitionIDs ...int64) []*Segment {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	segments := make([]*Segment, 0)
	for _, segment := range mgr.segments {
		if segment.CollectionID == collectionID &&
			(len(partitionIDs) == 0 || funcutil.SliceContain(partitionIDs, segment.PartitionID)) {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (mgr *TargetManager) GetSegmentsByCollection(collection int64, partitions ...int64) []*Segment {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	segments := make([]*Segment, 0)
	for _, segment := range mgr.segments {
		if segment.CollectionID == collection &&
			(len(partitions) == 0 || funcutil.SliceContain(partitions, segment.PartitionID)) {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (mgr *TargetManager) HandoffSegment(dest *Segment, sources ...int64) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	dest.CompactionFrom = sources
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

// func (mgr *TargetManager) AddDeltaChannel(channels ...*DeltaChannel) {
// 	mgr.rwmutex.Lock()
// 	defer mgr.rwmutex.Unlock()

// 	for _, channel := range channels {
// 		mgr.deltaChannels[channel.] = channel
// 	}
// }
