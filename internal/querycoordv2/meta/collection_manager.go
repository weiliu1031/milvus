package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Collection struct {
	CollectionID       UniqueID
	Partitions         []UniqueID
	Schema             *schemapb.CollectionSchema
	InMemoryPercentage int32
	ReplicaNumber      int32
}

type CollectionManager struct {
	rwmutex sync.RWMutex

	collections map[UniqueID]*Collection
}

func NewCollectionManager() *CollectionManager {
	return &CollectionManager{
		collections: make(map[int64]*Collection),
	}
}

func (m *CollectionManager) Get(id UniqueID) *Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.collections[id]
}

func (m *CollectionManager) Put(collections ...*Collection) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, collection := range collections {
		m.collections[collection.CollectionID] = collection
	}
}

func (m *CollectionManager) Remove(ids ...UniqueID) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, id := range ids {
		delete(m.collections, id)
	}
}
