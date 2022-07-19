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
		segments:      make(map[int64]*Segment),
		dmChannels:    make(map[string]*DmChannel),
		deltaChannels: make(map[string]*DeltaChannel),
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

func (mgr *TargetManager) HandoffSegment(dest *Segment, sources ...int64) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	for _, src := range sources {
		if mgr.containSegment(src) {
			delete(mgr.segments, src)
		} else { // This segment is a growing segment,
			mgr.growingSegments.Insert(src)
		}
	}

	// Must add dest segment after handling source segments,
	// For flushing segment, the dest and sources are the same
	mgr.addSegment(dest)
}

func (mgr *TargetManager) AddDmChannel(channels ...*DmChannel) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	for _, channel := range channels {
		mgr.dmChannels[channel.Channel] = channel
	}
}

// func (mgr *TargetManager) AddDeltaChannel(channels ...*DeltaChannel) {
// 	mgr.rwmutex.Lock()
// 	defer mgr.rwmutex.Unlock()

// 	for _, channel := range channels {
// 		mgr.deltaChannels[channel.] = channel
// 	}
// }
