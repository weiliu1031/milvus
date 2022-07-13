package meta

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestSegmentManager(t *testing.T) {
	var (
		CollectionID UniqueID = 20220707
		PartitionID  UniqueID = 14585896
		ReplicaID    UniqueID = 4309831
		Channel               = "insert_channel"
		Nodes                 = []UniqueID{1, 2, 3}
	)

	mgr := NewSegmentManager()

	segments := make([]*Segment, 3)
	for i := range segments {
		segments[i] = &Segment{
			SegmentInfo: datapb.SegmentInfo{
				CollectionID:  CollectionID,
				PartitionID:   PartitionID,
				ID:            UniqueID(i + 1),
				InsertChannel: Channel,
			},
			Nodes: map[int64]int64{ReplicaID: Nodes[i]},
		}
	}

	mgr.Put(segments...)

	for i := range segments {
		segment := segments[i]
		assert.Equal(t, segment, mgr.Get(segment.ID))

		results := mgr.GetByNode(Nodes[i])
		assert.Equal(t, 1, len(results))
		assert.Equal(t, segment, results[0])

		results = mgr.GetByCollectionAndNode(CollectionID, Nodes[i])
		assert.Equal(t, 1, len(results))
		assert.Equal(t, segment, results[0])

		results = mgr.GetByCollectionAndNode(CollectionID-1, Nodes[i])
		assert.Equal(t, 0, len(results))
	}
}
