package meta

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
)

type Collection struct {
	*querypb.CollectionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
}

func (collection *Collection) Clone() *Collection {
	new := *collection
	new.CollectionLoadInfo = proto.Clone(collection.CollectionLoadInfo).(*querypb.CollectionLoadInfo)
	return &new
}

type Partition struct {
	*querypb.PartitionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
}

func (partition *Partition) Clone() *Partition {
	new := *partition
	new.PartitionLoadInfo = proto.Clone(partition.PartitionLoadInfo).(*querypb.PartitionLoadInfo)
	return &new
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

func (m *CollectionManager) GetCollection(id UniqueID) *Collection {
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
	if len(m.getPartitionsByCollection(id)) > 0 {
		return querypb.LoadType_LoadPartition
	}
	return querypb.LoadType_UnKnownType
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
	return -1
}

func (m *CollectionManager) GetLoadPercentage(id UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[id]
	if ok {
		return collection.LoadPercentage
	}
	partitions := m.getPartitionsByCollection(id)
	if len(partitions) > 0 {
		return lo.SumBy(partitions, func(partition *Partition) int32 {
			return partition.LoadPercentage
		}) / int32(len(partitions))
	}
	return -1
}

func (m *CollectionManager) GetAllCollections() []*Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return lo.Values(m.collections)
}

func (m *CollectionManager) GetAllPartitions() []*Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return lo.Values(m.parttions)
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

func (m *CollectionManager) PutCollection(collection *Collection) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putCollection(collection, true)
}

func (m *CollectionManager) PutCollectionWithoutSave(collection *Collection) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.putCollection(collection, false)
}

func (m *CollectionManager) putCollection(collection *Collection, withSave bool) error {
	if withSave {
		err := m.store.SaveCollection(collection.CollectionLoadInfo)
		if err != nil {
			return err
		}
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

	err := m.putCollection(collection, true)
	if err != nil {
		return nil, false, err
	}

	return collection, false, nil
}

func (m *CollectionManager) GetOrPutPartition(partition *Partition) (*Partition, bool, error) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	old, ok := m.parttions[partition.GetPartitionID()]
	if ok {
		return old, true, nil
	}

	err := m.putPartition(partition, true)
	if err != nil {
		return nil, false, err
	}

	return partition, false, nil
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

func (m *CollectionManager) PutPartition(partition *Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putPartition(partition, true)
}

func (m *CollectionManager) PutPartitionWithoutSave(partition *Partition) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.putPartition(partition, false)
}

func (m *CollectionManager) putPartition(partition *Partition, withSave bool) error {
	if withSave {
		err := m.store.SavePartition(partition.PartitionLoadInfo)
		if err != nil {
			return err
		}
	}
	m.parttions[partition.PartitionID] = partition

	return nil
}

func (m *CollectionManager) RemovePartition(id UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	partition, ok := m.parttions[id]
	if !ok {
		return nil
	}

	err := m.store.ReleasePartition(partition.CollectionID, partition.PartitionID)
	if err != nil {
		return err
	}
	delete(m.parttions, id)

	return nil
}
