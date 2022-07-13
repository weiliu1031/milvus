package meta

import (
	"fmt"
	"testing"

	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestChannelManager(t *testing.T) {
	var (
		CollectionID UniqueID = 20220707
		ReplicaID    UniqueID = 4309831
		Nodes                 = []UniqueID{1, 2, 3}
	)

	mgr := NewChannelManager()

	dmcs := make([]*DmChannel, 3)
	for i := range dmcs {
		dmcs[i] = &DmChannel{
			CollectionID: CollectionID,
			Channel:      "dmc" + fmt.Sprint(i),
			Nodes:        map[int64]int64{ReplicaID: Nodes[i]},
		}
	}

	mgr.PutDmChannel(dmcs...)

	for i := range dmcs {
		dmc := dmcs[i]
		assert.Equal(t, dmc, mgr.GetDmChannel(dmc.Channel))

		results := mgr.GetDmChannelByNode(Nodes[i])
		assert.Equal(t, 1, len(results))
		assert.Equal(t, dmc, results[0])
	}
	results := mgr.GetDmChannelByCollection(CollectionID)
	assert.Equal(t, 3, len(results))
	assert.ElementsMatch(t, dmcs, results)

	mgr.RemoveDmChannel(dmcs[0].Channel)
}
