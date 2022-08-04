package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type DmChannel struct {
	*datapb.VchannelInfo
	Node    int64
	Version int64
}

func DmChannelFromVChannel(channel *datapb.VchannelInfo) *DmChannel {
	return &DmChannel{
		VchannelInfo: channel,
	}
}

type ChannelDistManager struct {
	rwmutex sync.RWMutex

	// NodeID -> Channels
	dmChannels map[UniqueID][]*DmChannel
}

func NewChannelDistManager() *ChannelDistManager {
	return &ChannelDistManager{
		dmChannels: make(map[UniqueID][]*DmChannel),
	}
}

func (m *ChannelDistManager) GetByNode(nodeID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getByNode(nodeID)
}

func (m *ChannelDistManager) getByNode(nodeID UniqueID) []*DmChannel {
	channels, ok := m.dmChannels[nodeID]
	if !ok {
		return nil
	}

	return channels
}

func (m *ChannelDistManager) GetAll() []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	result := make([]*DmChannel, 0)
	for _, channels := range m.dmChannels {
		for _, channel := range channels {
			result = append(result, channel)
		}
	}
	return result
}

// GetShardLeader returns the node whthin the given replicaNodes and subscribing the given shard,
// returns (0, false) if not found.
func (m *ChannelDistManager) GetShardLeader(replica *Replica, shard string) (int64, bool) {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	for node := range replica.Nodes {
		channels := m.dmChannels[node]
		for _, dmc := range channels {
			if dmc.ChannelName == shard {
				return node, true
			}
		}
	}

	return 0, false
}

func (m *ChannelDistManager) GetByCollection(collectionID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	channels := make([]*DmChannel, 0)
	for _, channels := range m.dmChannels {
		for _, channel := range channels {
			if channel.CollectionID == collectionID {
				channels = append(channels, channel)
			}
		}
	}

	return channels
}

func (m *ChannelDistManager) GetByNodeAndCollection(nodeID, collectionID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	channels := make([]*DmChannel, 0)
	for _, channel := range m.getByNode(nodeID) {
		if channel.CollectionID == collectionID {
			channels = append(channels, channel)
		}
	}

	return channels
}

func (m *ChannelDistManager) Update(nodeID UniqueID, channels ...*DmChannel) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.dmChannels[nodeID] = channels
}
