package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Segment struct {
	datapb.SegmentInfo
	Nodes map[UniqueID]UniqueID // ReplicaID -> NodeID
}

type SegmentManager struct {
	rwmutex sync.RWMutex

	segments map[UniqueID]*Segment
}

func NewSegmentManager() *SegmentManager {
	return &SegmentManager{
		segments: make(map[int64]*Segment),
	}
}

func (m *SegmentManager) Get(id UniqueID) *Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.segments[id]
}

func (m *SegmentManager) Put(segments ...*Segment) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, segment := range segments {
		m.segments[segment.ID] = segment
	}
}

func (m *SegmentManager) Remove(ids ...UniqueID) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, id := range ids {
		delete(m.segments, id)
	}
}

func (m *SegmentManager) GetByNode(nodeID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	segments := make([]*Segment, 0)
	for _, segment := range m.segments {
		isFound := false
		for _, node := range segment.Nodes {
			if node == nodeID {
				isFound = true
				break
			}
		}

		if isFound {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (m *SegmentManager) GetByCollectionAndNode(collectionID, nodeID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	segments := make([]*Segment, 0)
	for _, segment := range m.segments {
		if segment.CollectionID == collectionID {
			isFound := false
			for _, node := range segment.Nodes {
				if node == nodeID {
					isFound = true
					break
				}
			}

			if isFound {
				segments = append(segments, segment)
			}
		}
	}

	return segments
}
