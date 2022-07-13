package meta

import (
	"sync"

	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Replica struct {
	ID           UniqueID
	CollectionID UniqueID
	PartitionIDs UniqueSet
	Nodes        UniqueSet
}

type ReplicaManager struct {
	rwmutex sync.RWMutex

	replicas map[UniqueID]*Replica
}

func NewReplicaManager() *ReplicaManager {
	return &ReplicaManager{
		replicas: make(map[int64]*Replica),
	}
}

func (m *ReplicaManager) Get(id UniqueID) *Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.replicas[id]
}

func (m *ReplicaManager) Put(replicas ...*Replica) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, replica := range replicas {
		m.replicas[replica.ID] = replica
	}
}

func (m *ReplicaManager) Remove(ids ...UniqueID) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, id := range ids {
		delete(m.replicas, id)
	}
}

func (m *ReplicaManager) GetByCollection(collectionID UniqueID) []*Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	replicas := make([]*Replica, 0, 3)
	for _, replica := range m.replicas {
		if replica.CollectionID == collectionID {
			replicas = append(replicas, replica)
		}
	}

	return replicas
}

func (m *ReplicaManager) GetByNode(nodeID UniqueID) []*Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	replicas := make([]*Replica, 0)
	for _, replica := range m.replicas {
		if replica.Nodes.Contain(nodeID) {
			replicas = append(replicas, replica)
		}
	}

	return replicas
}

func (m *ReplicaManager) AddNode(replicaID UniqueID, nodes ...UniqueID) bool {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	replica, ok := m.replicas[replicaID]
	if !ok {
		return false
	}

	replica.Nodes.Insert(nodes...)
	return true
}

func (m *ReplicaManager) RemoveNode(replicaID UniqueID, nodes ...UniqueID) bool {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	replica, ok := m.replicas[replicaID]
	if !ok {
		return false
	}

	replica.Nodes.Remove(nodes...)
	return true
}
