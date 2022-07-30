package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Segment struct {
	datapb.SegmentInfo
	Version int64 // Version is the timestamp of loading segment
	Node    int64 // Node the segment is in
}

type SegmentDistManager struct {
	rwmutex sync.RWMutex

	// shard, nodeID -> []*Segment
	segments map[string]map[UniqueID][]*Segment
}

func NewSegmentDistManager() *SegmentDistManager {
	return &SegmentDistManager{
		segments: make(map[string]map[UniqueID][]*Segment),
	}
}

func (m *SegmentDistManager) Update(nodeID UniqueID, segments ...*Segment) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	shardSegments := make(map[string][]*Segment)
	for _, segment := range segments {
		segment.Node = nodeID
		shardSegments[segment.InsertChannel] = append(shardSegments[segment.InsertChannel], segment)
	}

	for shard, segments := range shardSegments {
		m.segments[shard][nodeID] = segments
	}
}

// GetAll returns all segments,
// group by collection and replica
func (m *SegmentDistManager) GetAll() []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	segments := make([]*Segment, 0, len(m.segments))
	for _, shardSegments := range m.segments {
		for _, nodeSegments := range shardSegments {
			segments = append(segments, nodeSegments...)
		}
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

// GetByNode returns all segments of the given node.
func (m *SegmentDistManager) GetByNode(nodeID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	segments := make([]*Segment, 0)
	for _, shardSegments := range m.segments {
		segments = append(segments, shardSegments[nodeID]...)
	}
	return segments
}

// GetByCollection returns all segments of the given collection.
func (m *SegmentDistManager) GetByCollection(collectionID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	result := make([]*Segment, 0)
	for _, shardSegments := range m.segments {
		for _, segments := range shardSegments {
			for _, segment := range segments {
				if segment.CollectionID == collectionID {
					result = append(result, segment)
				}
			}
		}
	}
	return result
}

// GetByCollectionAndNode returns all segments of the given collection and node.
func (m *SegmentDistManager) GetByCollectionAndNode(collectionID, nodeID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	result := make([]*Segment, 0)
	for _, shardSegments := range m.segments {
		for _, segment := range shardSegments[nodeID] {
			if segment.CollectionID == collectionID {
				result = append(result, segment)
			}
		}
	}
	return result
}
