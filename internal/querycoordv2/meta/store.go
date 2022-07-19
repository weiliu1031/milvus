package meta

import (
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/kv"
)

type Store interface {
	Load(collection int64, partitions []int64) error
	Release(collection int64, partitions []int64) error
	GetPartitions() (map[int64][]int64, error)
}

const loadInfoPrefix = "querycoord-load-info"

var (
	ErrEmptyPartitions = errors.New("partitions can not be empty")
)

type MetaStore struct {
	cli kv.TxnKV
}

func (s *MetaStore) Load(collection int64, partitions []int64) error {
	if len(partitions) == 0 {
		return ErrEmptyPartitions
	}

	res := make(map[string]string)
	for _, partition := range partitions {
		k := encodeLoadInfoKey(collection, partition)
		res[k] = ""
	}

	return s.cli.MultiSave(res)
}

func (s *MetaStore) Release(collection int64, partitions []int64) error {
	if len(partitions) == 0 {
		return ErrEmptyPartitions
	}

	res := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		k := encodeLoadInfoKey(collection, partition)
		res = append(res, k)
	}

	return s.cli.MultiRemove(res)
}

func (s *MetaStore) GetPartitions() (map[int64][]int64, error) {
	keys, _, err := s.cli.LoadWithPrefix(loadInfoPrefix)
	if err != nil {
		return nil, err
	}

	ret := make(map[int64][]int64)
	for _, key := range keys {
		coll, part, err := decodeLoadInfoKey(key)
		if err != nil {
			return nil, err
		}

		if _, ok := ret[coll]; !ok {
			ret[coll] = make([]int64, 0)
		}
		ret[coll] = append(ret[coll], part)
	}
	return ret, nil
}

func encodeLoadInfoKey(collection int64, partition int64) string {
	return fmt.Sprintf("%s/%d/%d", loadInfoPrefix, collection, partition)
}

func decodeLoadInfoKey(key string) (collection int64, partition int64, err error) {
	var tmp string
	_, err = fmt.Sscanf(key, "%s/%d/%d", &tmp, &collection, &partition)
	return
}
