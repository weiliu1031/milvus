package utils

import (
	"context"
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/samber/lo"
)

func GetReplicaNodesInfo(replicaMgr *meta.ReplicaManager, nodeMgr *session.NodeManager, replicaID int64) []*session.NodeInfo {
	replica := replicaMgr.Get(replicaID)
	if replica == nil {
		return nil
	}

	nodes := make([]*session.NodeInfo, 0, len(replica.Nodes))
	for node := range replica.Nodes {
		nodes = append(nodes, nodeMgr.Get(node))
	}
	return nodes
}

func GetPartitions(collectionMgr *meta.CollectionManager, broker *meta.CoordinatorBroker, collectionID int64) ([]int64, error) {
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
func GroupNodesByReplica(replicaMgr *meta.ReplicaManager, collectionID int64, nodes []int64) map[int64][]int64 {
	ret := make(map[int64][]int64)
	replicas := replicaMgr.GetByCollection(collectionID)
	for _, replica := range replicas {
		for _, node := range nodes {
			if replica.Nodes.Contain(node) {
				ret[replica.ID] = append(ret[replica.ID], node)
			}
		}
	}
	return ret
}

// AssignNodesToReplicas assigns nodes to the given replicas,
// all given replicas must be the same collection,
// the given replicas have to be not in ReplicaManager
func AssignNodesToReplicas(nodeMgr *session.NodeManager, replicas ...*meta.Replica) {
	replicaNumber := len(replicas)
	nodes := nodeMgr.GetAll()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetScore() < nodes[j].GetScore()
	})

	for i, node := range nodes {
		replicas[i%replicaNumber].Nodes.Insert(node.ID())
	}
}
