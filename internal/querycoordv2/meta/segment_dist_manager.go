package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Segment struct {
	datapb.SegmentInfo
}

type SegmentMap map[UniqueID]*Segment

type SegmentDistManager struct {
	rwmutex sync.RWMutex

	// NodeID ->
	segments map[UniqueID][]*Segment
}

func NewSegmentDistManager() *SegmentDistManager {
	return &SegmentDistManager{
		segments: make(map[int64][]*Segment),
	}
}

func (m *SegmentDistManager) Update(nodeID UniqueID, segments ...*Segment) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.segments[nodeID] = segments
}

// func (m *SegmentDistManager) Get(id UniqueID) *Segment {
// 	m.rwmutex.RLock()
// 	defer m.rwmutex.RUnlock()

// 	return m.segments[id]
// }

func (m *SegmentDistManager) GetAll() []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	segments := make([]*Segment, 0, len(m.segments))
	for _, v := range m.segments {
		segments = append(segments, v...)
	}

	return segments
}

// func (m *SegmentDistManager) Remove(ids ...UniqueID) {
// 	m.rwmutex.Lock()
// 	defer m.rwmutex.Unlock()

// 	for _, id := range ids {
// 		delete(m.segments, id)
// 	}
// }

func (m *SegmentDistManager) GetByNode(nodeID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.segments[nodeID]
}

func (m *SegmentDistManager) GetByCollectionAndNode(collectionID, nodeID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	segments := m.segments[nodeID]

	result := make([]*Segment, 0)
	for _, segment := range segments {
		if segment.CollectionID == collectionID {
			result = append(result, segment)
		}
	}

	return result
}
