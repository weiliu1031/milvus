package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type DmChannel struct {
	datapb.VchannelInfo
}

type DeltaChannel struct {
	datapb.VchannelInfo
}

type DmChannelContainer struct {
	channels []*DmChannel
}

type DeltaChannelContainer struct {
	channels []*DeltaChannel
}

type ChannelDistManager struct {
	rwmutex sync.RWMutex

	// NodeID -> Channels
	dmChannels    map[UniqueID][]*DmChannel
	deltaChannels map[UniqueID][]*DeltaChannel
}

func NewChannelDistManager() *ChannelDistManager {
	return &ChannelDistManager{
		dmChannels:    make(map[UniqueID][]*DmChannel),
		deltaChannels: make(map[UniqueID][]*DeltaChannel),
	}
}

func (m *ChannelDistManager) GetDmChannelByNode(nodeID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getDmChannelByNode(nodeID)
}

func (m *ChannelDistManager) getDmChannelByNode(nodeID UniqueID) []*DmChannel {
	channels, ok := m.dmChannels[nodeID]
	if !ok {
		return nil
	}

	return channels
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

func (m *ChannelDistManager) GetDmChannelByNodeAndCollection(nodeID, collectionID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	channels := make([]*DmChannel, 0)
	for _, channel := range m.getDmChannelByNode(nodeID) {
		if channel.CollectionID == collectionID {
			channels = append(channels, channel)
		}
	}

	return channels
}

func (m *ChannelDistManager) UpdateDmChannels(nodeID UniqueID, channels ...*DmChannel) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.dmChannels[nodeID] = channels
}

func (m *ChannelDistManager) GetDeltaChannelByNode(nodeID UniqueID) []*DeltaChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getDeltaChannelByNode(nodeID)
}

func (m *ChannelDistManager) getDeltaChannelByNode(nodeID UniqueID) []*DeltaChannel {
	channels, ok := m.deltaChannels[nodeID]
	if !ok {
		return nil
	}

	return channels
}

func (m *ChannelDistManager) UpdateDeltaChannels(nodeID UniqueID, channels ...*DeltaChannel) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.deltaChannels[nodeID] = channels
}
