package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type CollectionStatus int32

const (
	CollectionStatusLoading CollectionStatus = iota + 1
	CollectionStatusLoaded
)

type Collection struct {
	ID                 UniqueID
	Partitions         []UniqueID
	Schema             *schemapb.CollectionSchema
	InMemoryPercentage int32
	ReplicaNumber      int32
	LoadType           querypb.LoadType
	Status             CollectionStatus
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

// Reload recovers collections from kv store,
// panics if failed
func (m *CollectionManager) Reload() {

}

func (m *CollectionManager) Get(id UniqueID) *Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.collections[id]
}

func (m *CollectionManager) GetAll() []*Collection {
	collections := make([]*Collection, 0, len(m.collections))

	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	for _, collection := range m.collections {
		collections = append(collections, collection)
	}
	return collections
}

func (m *CollectionManager) Put(collection *Collection) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.put(collection)
}

func (m *CollectionManager) put(collection *Collection) error {
	err := m.store.Load(collection.ID, collection.Partitions)
	if err != nil {
		return err
	}
	m.collections[collection.ID] = collection

	return nil
}

func (m *CollectionManager) GetOrPut(collectionID UniqueID, collection *Collection) (*Collection, bool, error) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	old, ok := m.collections[collectionID]
	if ok {
		return old, true, nil
	}

	err := m.put(collection)
	if err != nil {
		return nil, false, err
	}

	return collection, false, nil
}

func (m *CollectionManager) Remove(id UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	collection, ok := m.collections[id]
	if !ok {
		return nil
	}

	err := m.store.Release(collection.ID, collection.Partitions)
	if err != nil {
		return err
	}
	delete(m.collections, id)

	return nil
}
