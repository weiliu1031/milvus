package meta

import (
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Replica struct {
	ID           UniqueID
	CollectionID UniqueID
	Nodes        UniqueSet
}

type ReplicaManager struct {
	rwmutex sync.RWMutex

	idAllocator func() (int64, error)
	nodeMgr     *session.NodeManager
	replicas    map[UniqueID]*Replica
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

func (m *ReplicaManager) Put(replicaNumber int32, collectionID UniqueID) ([]*Replica, error) {
	var (
		replicas = make([]*Replica, replicaNumber)
		err      error
	)
	for i := range replicas {
		replicas[i], err = m.spawn(collectionID)
		if err != nil {
			return nil, err
		}
	}

	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.put(replicas...)
	m.assignCollectionReplicas(replicas...)

	return replicas, nil
}

func (m *ReplicaManager) spawn(collectionID UniqueID) (*Replica, error) {
	id, err := m.idAllocator()
	if err != nil {
		return nil, err
	}
	return &Replica{
		ID:           id,
		CollectionID: collectionID,
		Nodes:        make(UniqueSet),
	}, nil
}

func (m *ReplicaManager) put(replicas ...*Replica) {
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

// Assign assigns nodes to the given replicas,
// all given replicas must be the same collection,
// tries to assign nodes to all replicas if no given replica
func (m *ReplicaManager) Assign(replicaIDs ...UniqueID) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	if len(replicaIDs) == 0 {
		m.assignAllReplicas()
	} else {
		replicas := make([]*Replica, len(replicaIDs))
		for i := range replicas {
			replicas[i] = m.replicas[replicaIDs[i]]
		}
		m.assignCollectionReplicas(replicas...)
	}
}

func (m *ReplicaManager) assignCollectionReplicas(replicas ...*Replica) {
	replicaNumber := len(replicas)
	nodes := m.nodeMgr.GetAll()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetScore() < nodes[j].GetScore()
	})

	for i, node := range nodes {
		replicas[i%replicaNumber].Nodes.Insert(node.ID())
	}
}

func (m *ReplicaManager) assignAllReplicas() {
	panic("not implemented")
}

// RemoveCollection removes replicas of given collection,
// this operation is slower than Remove, use Remove if the replicas' ID is known
func (m *ReplicaManager) RemoveCollection(collectionID UniqueID) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for id, replica := range m.replicas {
		if replica.CollectionID == collectionID {
			delete(m.replicas, id)
		}
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
