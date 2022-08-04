package meta

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Collection struct {
	*querypb.CollectionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
}

type Partition struct {
	*querypb.PartitionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
}

type CollectionManager struct {
	rwmutex sync.RWMutex

	collections map[UniqueID]*Collection
	parttions   map[UniqueID]*Partition
	store       Store

	broker *CoordinatorBroker
}

func NewCollectionManager(store Store, broker *CoordinatorBroker) *CollectionManager {
	return &CollectionManager{
		collections: make(map[int64]*Collection),
		store:       store,
		broker:      broker,
	}
}

// Reload recovers collections from kv store,
// panics if failed
func (m *CollectionManager) Reload() {
	collections, err := m.store.GetCollections()
	if err != nil {
		panic(err)
	}
	partitions, err := m.store.GetPartitions()
	if err != nil {
		panic(err)
	}

	for _, collection := range collections {
		m.collections[collection.CollectionID] = &Collection{
			CollectionLoadInfo: collection,
		}
	}

	for _, partition := range partitions {
		m.parttions[partition.PartitionID] = &Partition{
			PartitionLoadInfo: partition,
		}
	}
}

func (m *CollectionManager) Get(id UniqueID) *Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.collections[id]
}

func (m *CollectionManager) GetLoadType(id UniqueID) querypb.LoadType {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	_, ok := m.collections[id]
	if ok {
		return querypb.LoadType_LoadCollection
	}
	return querypb.LoadType_LoadPartition
}

func (m *CollectionManager) GetReplicaNumber(id UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[id]
	if ok {
		return collection.GetReplicaNumber()
	}
	partitions := m.getPartitionsByCollection(id)
	if len(partitions) > 0 {
		return partitions[0].GetReplicaNumber()
	}
	return 0
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

func (m *CollectionManager) GetPartitionsByCollection(collectionID UniqueID) []*Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getPartitionsByCollection(collectionID)
}

func (m *CollectionManager) getPartitionsByCollection(collectionID UniqueID) []*Partition {
	partitions := make([]*Partition, 0)
	for _, partition := range m.parttions {
		if partition.CollectionID == collectionID {
			partitions = append(partitions, partition)
		}
	}
	return partitions
}

func (m *CollectionManager) Put(collection *Collection) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.put(collection)
}

func (m *CollectionManager) put(collection *Collection) error {
	err := m.store.SaveCollection(collection.CollectionLoadInfo)
	if err != nil {
		return err
	}
	m.collections[collection.CollectionID] = collection

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

func (m *CollectionManager) RemoveCollection(id UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.collections[id]
	if !ok {
		return nil
	}

	err := m.store.ReleaseCollection(id)
	if err != nil {
		return err
	}
	delete(m.collections, id)

	return nil
}
