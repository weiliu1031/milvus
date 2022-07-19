package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type DmChannel struct {
	CollectionID UniqueID
	Channel      string
}

type DeltaChannel []*datapb.VchannelInfo

type DmChannelContainer struct {
	channels []*DmChannel
}

type DeltaChannelContainer struct {
	channels []*DeltaChannel
}

type ChannelDistManager struct {
	rwmutex sync.RWMutex

	// NodeID -> Channels
	dmChannels    map[UniqueID]*DmChannelContainer
	deltaChannels map[UniqueID]*DeltaChannelContainer
}

func NewChannelDistManager() *ChannelDistManager {
	return &ChannelDistManager{
		dmChannels:    make(map[UniqueID]*DmChannelContainer),
		deltaChannels: make(map[UniqueID]*DeltaChannelContainer),
	}
}

func (m *ChannelDistManager) GetDmChannelByNode(nodeID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getDmChannelByNode(nodeID)
}

func (m *ChannelDistManager) getDmChannelByNode(nodeID UniqueID) []*DmChannel {
	container, ok := m.dmChannels[nodeID]
	if !ok {
		return nil
	}

	return container.channels
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

	m.dmChannels[nodeID] = &DmChannelContainer{
		channels: channels,
	}
}

func (m *ChannelDistManager) GetDeltaChannelByNode(nodeID UniqueID) []*DeltaChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getDeltaChannelByNode(nodeID)
}

func (m *ChannelDistManager) getDeltaChannelByNode(nodeID UniqueID) []*DeltaChannel {
	container, ok := m.deltaChannels[nodeID]
	if !ok {
		return nil
	}

	return container.channels
}

func (m *ChannelDistManager) UpdateDeltaChannels(nodeID UniqueID, channels ...*DeltaChannel) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.deltaChannels[nodeID] = &DeltaChannelContainer{
		channels: channels,
	}
}
