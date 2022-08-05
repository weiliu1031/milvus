package meta

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

var (
	ErrInvalidKey = errors.New("invalid load info key")
)

const (
	collectionLoadInfoPrefix = "querycoord-collection-loadinfo"
	partitionLoadInfoPrefix  = "querycoord-partition-loadinfo"
	replicaPrefix            = "querycoord-replica"
)

// Store is used to save and get from object storage.
type Store interface {
	SaveCollection(info *querypb.CollectionLoadInfo) error
	SavePartition(info *querypb.PartitionLoadInfo) error
	SaveReplica(replica *querypb.Replica) error
	GetCollections() ([]*querypb.CollectionLoadInfo, error)
	GetPartitions() ([]*querypb.PartitionLoadInfo, error)
	GetReplicas(collectionID int64) ([]*querypb.Replica, error)
	ReleaseCollection(id int64) error
	ReleasePartition(collection, partition int64) error
	ReleaseReplicas(collectionID int64) error
	ReleaseReplica(collection, replica int64) error
}

type MetaStore struct {
	cli kv.TxnKV
}

func NewMetaStore(cli kv.TxnKV) MetaStore {
	return MetaStore{
		cli: cli,
	}
}

func (s MetaStore) SaveCollection(info *querypb.CollectionLoadInfo) error {
	k := encodeCollectionLoadInfoKey(info.GetCollectionID())
	v, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	return s.cli.Save(k, string(v))
}

func (s MetaStore) SavePartition(info *querypb.PartitionLoadInfo) error {
	k := encodePartitionLoadInfoKey(info.GetCollectionID(), info.GetPartitionID())
	v, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	return s.cli.Save(k, string(v))
}

func (s MetaStore) SaveReplica(replica *querypb.Replica) error {
	key := encodeReplicaKey(replica.GetCollectionID(), replica.GetID())
	value, err := proto.Marshal(replica)
	if err != nil {
		return err
	}
	return s.cli.Save(key, string(value))
}

func (s MetaStore) GetCollections() ([]*querypb.CollectionLoadInfo, error) {
	_, values, err := s.cli.LoadWithPrefix(collectionLoadInfoPrefix)
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.CollectionLoadInfo, 0, len(values))
	for _, v := range values {
		info := querypb.CollectionLoadInfo{}
		if err := proto.Unmarshal([]byte(v), &info); err != nil {
			return nil, err
		}
		ret = append(ret, &info)
	}
	return ret, nil
}

func (s MetaStore) GetPartitions() ([]*querypb.PartitionLoadInfo, error) {
	_, values, err := s.cli.LoadWithPrefix(partitionLoadInfoPrefix)
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.PartitionLoadInfo, 0, len(values))
	for _, v := range values {
		info := querypb.PartitionLoadInfo{}
		if err := proto.Unmarshal([]byte(v), &info); err != nil {
			return nil, err
		}
		ret = append(ret, &info)
	}
	return ret, nil
}

func (s MetaStore) GetReplicas(collectionID int64) ([]*querypb.Replica, error) {
	_, values, err := s.cli.LoadWithPrefix(replicaPrefix)
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.Replica, 0, len(values))
	for _, v := range values {
		info := querypb.Replica{}
		if err := proto.Unmarshal([]byte(v), &info); err != nil {
			return nil, err
		}
		ret = append(ret, &info)
	}
	return ret, nil
}

func (s MetaStore) ReleaseCollection(id int64) error {
	k := encodeCollectionLoadInfoKey(id)
	return s.cli.Remove(k)
}

func (s MetaStore) ReleasePartition(collection, partition int64) error {
	k := encodePartitionLoadInfoKey(collection, partition)
	return s.cli.Remove(k)
}

func (s MetaStore) ReleaseReplicas(collectionID int64) error {
	key := encodeCollectionReplicaKey(collectionID)
	return s.cli.RemoveWithPrefix(key)
}

func (s MetaStore) ReleaseReplica(collection, replica int64) error {
	key := encodeReplicaKey(collection,replica)
	return s.cli.Remove(key)
}

func encodeCollectionLoadInfoKey(collection int64) string {
	return fmt.Sprintf("%s/%d", collectionLoadInfoPrefix, collection)
}

func encodePartitionLoadInfoKey(collection, partition int64) string {
	return fmt.Sprintf("%s/%d/%d", partitionLoadInfoPrefix, collection, partition)
}

func encodeReplicaKey(collection, replica int64) string {
	return fmt.Sprintf("%s/%d/%d", replicaPrefix, collection, replica)
}

func encodeCollectionReplicaKey(collection int64) string {
	return fmt.Sprintf("%s/%d", replicaPrefix, collection)
}
