package meta

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestChannelManager(t *testing.T) {
	var (
		CollectionID UniqueID = 20220707
		Nodes                 = []UniqueID{1, 2, 3}
	)

	mgr := NewChannelDistManager()

	dmcs := make([]*DmChannel, 3)
	for i := range dmcs {
		dmcs[i] = &DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: CollectionID,
				ChannelName:  "dmc" + fmt.Sprint(i),
			},
		}
	}

	for i, node := range Nodes {
		mgr.UpdateDmChannels(node, dmcs[i])
	}

	for i := range dmcs {
		dmc := dmcs[i]

		results := mgr.GetDmChannelByNode(Nodes[i])
		assert.Equal(t, 1, len(results))
		assert.Equal(t, dmc, results[0])
	}
}
