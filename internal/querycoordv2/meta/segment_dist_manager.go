package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Segment struct {
	datapb.SegmentInfo
}

type SegmentContainer struct {
	segments []*Segment
}

type SegmentDistManager struct {
	rwmutex sync.RWMutex

	segments map[UniqueID]*SegmentContainer
}

func NewSegmentDistManager() *SegmentDistManager {
	return &SegmentDistManager{
		segments: make(map[int64]*SegmentContainer),
	}
}

func (m *SegmentDistManager) Update(nodeID UniqueID, segments ...*Segment) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.segments[nodeID] = &SegmentContainer{segments}
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
	for _, container := range m.segments {
		segments = append(segments, container.segments...)
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

	container, ok := m.segments[nodeID]
	if !ok {
		return nil
	}
	return container.segments
}

func (m *SegmentDistManager) GetByCollectionAndNode(collectionID, nodeID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	container := m.segments[nodeID]

	result := make([]*Segment, 0)
	for _, segment := range container.segments {
		if segment.CollectionID == collectionID {
			result = append(result, segment)
		}
	}

	return result
}
