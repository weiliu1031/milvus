// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

var (
	ErrReplicaNotFound = errors.New("Replica not found")
	ErrReplicaNumber   = errors.New("Replica number can't be zero")
)

func GetPartitions(collectionMgr *meta.CollectionManager, broker meta.Broker, collectionID int64) ([]int64, error) {
	collection := collectionMgr.GetCollection(collectionID)
	if collection != nil {
		partitions, err := broker.GetPartitions(context.Background(), collectionID)
		return partitions, err
	}

	partitions := collectionMgr.GetPartitionsByCollection(collectionID)
	if partitions != nil {
		return lo.Map(partitions, func(partition *meta.Partition, i int) int64 {
			return partition.PartitionID
		}), nil
	}

	// todo(yah01): replace this error with a defined error
	return nil, fmt.Errorf("collection/partition not loaded")
}

// GroupNodesByReplica groups nodes by replica,
// returns ReplicaID -> NodeIDs
func GroupNodesByReplica(meta *meta.Meta, collectionID int64, nodes []int64) map[int64][]int64 {
	ret := make(map[int64][]int64)
	replicas := meta.ReplicaManager.GetByCollection(collectionID)
	for _, replica := range replicas {
		for _, node := range nodes {
			if meta.ResourceManager.ContainsNode(replica.GetResourceGroup(), node) {
				ret[replica.ID] = append(ret[replica.ID], node)
			}
		}
	}
	return ret
}

// GroupPartitionsByCollection groups partitions by collection,
// returns CollectionID -> Partitions
func GroupPartitionsByCollection(partitions []*meta.Partition) map[int64][]*meta.Partition {
	ret := make(map[int64][]*meta.Partition, 0)
	for _, partition := range partitions {
		collection := partition.GetCollectionID()
		ret[collection] = append(ret[collection], partition)
	}
	return ret
}

// SpawnReplicas spawns replicas for given collection, assign nodes to them, and save them
func SpawnReplicasWithRG(replicaMgr *meta.ReplicaManager, collection int64, replicasInRG map[string]int32, replicaNumber int32) ([]*meta.Replica, error) {
	if len(replicasInRG) == 0 {
		if replicaNumber == 0 {
			return nil, ErrReplicaNumber
		}
		// if no rg specified, create replica in default rg
		replicas, err := replicaMgr.SpawnReplicas(collection, map[string]int32{meta.DefaultResourceGroupName: replicaNumber})
		if err != nil {
			return nil, err
		}

		return replicas, replicaMgr.Put(replicas...)
	}

	// create replica with specified RG
	replicas, err := replicaMgr.SpawnReplicas(collection, replicasInRG)
	if err != nil {
		return nil, err
	}

	return replicas, replicaMgr.Put(replicas...)
}
