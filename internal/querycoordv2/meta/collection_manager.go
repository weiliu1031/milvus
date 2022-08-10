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

func (m *CollectionManager) GetPartition(id UniqueID) *Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.parttions[id]
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

func (m *CollectionManager) Exist(id UniqueID) bool {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	_, ok := m.collections[id]
	if ok {
		return true
	}
	partitions := m.getPartitionsByCollection(id)
	if len(partitions) > 0 {
		return true
	}
	return false
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

func (m *CollectionManager) RemoveCollection(id UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.collections[id]
	if ok {
		err := m.store.ReleaseCollection(id)
		if err != nil {
			return err
		}
		delete(m.collections, id)
		return nil
	}

	partitions := lo.Map(m.getPartitionsByCollection(id),
		func(partition *Partition, _ int) int64 {
			return partition.GetPartitionID()
		})
	return m.removePartition(partitions...)
}

func (m *CollectionManager) PutPartition(partition *Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putPartition([]*Partition{partition}, true)
}

func (m *CollectionManager) PutPartitionWithoutSave(partition *Partition) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.putPartition([]*Partition{partition}, false)
}

func (m *CollectionManager) putPartition(partitions []*Partition, withSave bool) error {
	if withSave {
		loadInfos := lo.Map(partitions, func(partition *Partition, _ int) *querypb.PartitionLoadInfo {
			return partition.PartitionLoadInfo
		})
		err := m.store.SavePartition(loadInfos...)
		if err != nil {
			return err
		}
	}
	for _, partition := range partitions {
		m.parttions[partition.GetPartitionID()] = partition
	}
	return nil
}

func (m *CollectionManager) RemovePartition(ids ...UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.removePartition(ids...)
}

func (m *CollectionManager) removePartition(ids ...UniqueID) error {
	partition := m.parttions[ids[0]]
	err := m.store.ReleasePartition(partition.CollectionID, ids...)
	if err != nil {
		return err
	}
	for _, id := range ids {
		delete(m.parttions, id)
	}

	return nil
}
