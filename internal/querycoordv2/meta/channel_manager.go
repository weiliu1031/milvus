package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type DmChannel struct {
	CollectionID UniqueID
	Channel      string
	Nodes        map[UniqueID]UniqueID // ReplicaID -> NodeID
}

type DeltaChannel []*datapb.VchannelInfo

type ChannelManager struct {
	rwmutex sync.RWMutex

	dmChannels    map[string]*DmChannel
	deltaChannels map[string]*DeltaChannel
}

func NewChannelManager() *ChannelManager {
	return &ChannelManager{
		dmChannels:    make(map[string]*DmChannel),
		deltaChannels: make(map[string]*DeltaChannel),
	}
}

func (m *ChannelManager) GetDmChannel(channel string) *DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.dmChannels[channel]
}

func (m *ChannelManager) PutDmChannel(channels ...*DmChannel) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, dmc := range channels {
		m.dmChannels[dmc.Channel] = dmc
	}
}

func (m *ChannelManager) RemoveDmChannel(channels ...string) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, channel := range channels {
		delete(m.dmChannels, channel)
	}
}

func (m *ChannelManager) GetDmChannelByNode(nodeID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	channels := make([]*DmChannel, 0)
	for _, dmc := range m.dmChannels {
		isFound := false
		for _, node := range dmc.Nodes {
			if node == nodeID {
				isFound = true
				break
			}
		}

		if isFound {
			channels = append(channels, dmc)
		}
	}

	return channels
}

func (m *ChannelManager) GetDmChannelByCollection(collectionID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	channels := make([]*DmChannel, 0)
	for _, dmc := range m.dmChannels {
		if dmc.CollectionID == collectionID {
			channels = append(channels, dmc)
		}
	}

	return channels
}

func (m *ChannelManager) GetDeltaChannel(channel string) *DeltaChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.deltaChannels[channel]
}

func (m *ChannelManager) PutDeltaChannel(channels ...*DeltaChannel) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, channel := range channels {
		name := (*channel)[0].ChannelName
		m.deltaChannels[name] = channel
	}
}

func (m *ChannelManager) RemoveDeltaChannel(channels ...string) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, channel := range channels {
		delete(m.deltaChannels, channel)
	}
}
