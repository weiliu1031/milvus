package utils

import "github.com/milvus-io/milvus/internal/querycoordv2/meta"

func Collect(collections []*meta.Collection, partitions []*meta.Partition) []int64 {
	var ret []int64
	m := make(map[int64]struct{})
	ret, m = ids(collections, ret, m)
	ret, m = ids(partitions, ret, m)
	return ret
}

func ids[E interface{ GetCollectionID() int64 }](elems []E, res []int64, filter map[int64]struct{}) ([]int64, map[int64]struct{}) {
	for _, e := range elems {
		id := e.GetCollectionID()
		if _, ok := filter[id]; ok {
			continue
		}
		res = append(res, id)
		filter[id] = struct{}{}
	}
	return res, filter
}
