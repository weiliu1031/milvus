package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Replica struct {
	*querypb.Replica
	Nodes UniqueSet // a helper field for manipulating replica's Nodes slice field
}

type ReplicaManager struct {
	rwmutex sync.RWMutex

	idAllocator func() (int64, error)
	nodeMgr     *session.NodeManager
	replicas    map[UniqueID]*Replica
	store       Store
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

func (m *ReplicaManager) Spawn(collection int64, replicaNumber int32) ([]*Replica, error) {
	var (
		replicas = make([]*Replica, replicaNumber)
		err      error
	)
	for i := range replicas {
		replicas[i], err = m.spawn(collection)
		if err != nil {
			return nil, err
		}
	}
	return replicas, err
}

func (m *ReplicaManager) Put(replicas ...*Replica) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.put(replicas...)
}

func (m *ReplicaManager) spawn(collectionID UniqueID) (*Replica, error) {
	id, err := m.idAllocator()
	if err != nil {
		return nil, err
	}
	return &Replica{
		Replica: &querypb.Replica{
			ID:           id,
			CollectionID: collectionID,
		},
		Nodes: make(UniqueSet),
	}, nil
}

func (m *ReplicaManager) put(replicas ...*Replica) error {
	for _, replica := range replicas {
		err := m.store.SaveReplica(replica.Replica)
		if err != nil {
			return err
		}
		m.replicas[replica.ID] = replica
	}
	return nil
}

func (m *ReplicaManager) Remove(ids ...UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, id := range ids {
		replica, ok := m.replicas[id]
		if !ok {
			continue
		}

		err := m.store.ReleaseReplica(replica.GetCollectionID(), replica.GetID())
		if err != nil {
			return err
		}
		delete(m.replicas, id)
	}
	return nil
}

// RemoveCollection removes replicas of given collection,
// returns error if failed to remove replica from KV
func (m *ReplicaManager) RemoveCollection(collectionID UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	err := m.store.ReleaseReplicas(collectionID)
	if err != nil {
		return err
	}
	for id, replica := range m.replicas {
		if replica.CollectionID == collectionID {
			delete(m.replicas, id)
		}
	}
	return nil
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

func (m *ReplicaManager) GetByCollectionAndNode(collectionID, nodeID UniqueID) *Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	for _, replica := range m.replicas {
		if replica.CollectionID == collectionID && replica.Nodes.Contain(nodeID) {
			return replica
		}
	}

	return nil
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
