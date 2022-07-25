package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/kv"
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
	store       Store

	broker *CoordinatorBroker
}

func NewCollectionManager(txnKV kv.TxnKV, broker *CoordinatorBroker) *CollectionManager {
	return &CollectionManager{
		collections: make(map[int64]*Collection),
		store:       NewMetaStore(txnKV),

		broker: broker,
	}
}

func (m *CollectionManager) Reload() {
	// TODO(yah01)
}

func (m *CollectionManager) Get(id UniqueID) *Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.collections[id]
}

func (m *CollectionManager) Put(collection *Collection) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	err := m.store.Load(collection.CollectionID, collection.Partitions)
	if err != nil {
		return err
	}
	m.collections[collection.CollectionID] = collection

	return nil
}

func (m *CollectionManager) Remove(id UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	collection, ok := m.collections[id]
	if !ok {
		return nil
	}

	err := m.store.Release(collection.CollectionID, collection.Partitions)
	if err != nil {
		return err
	}
	delete(m.collections, id)

	return nil
}
