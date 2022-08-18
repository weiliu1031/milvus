package utils

import (
	"context"
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/internal/proto/datapb"
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

// GroupSegmentsByReplica groups segments by replica,
// returns ReplicaID -> Segments
func GroupSegmentsByReplica(replicaMgr *meta.ReplicaManager, collectionID int64, segments []*meta.Segment) map[int64][]*meta.Segment {
	ret := make(map[int64][]*meta.Segment)
	replicas := replicaMgr.GetByCollection(collectionID)
	for _, replica := range replicas {
		for _, segment := range segments {
			if replica.Nodes.Contain(segment.Node) {
				ret[replica.ID] = append(ret[replica.ID], segment)
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
		replicas[i%replicaNumber].AddNode(node.ID())
	}
}

// SpawnReplicas spawns replicas for given collection, assign nodes to them, and save them
func SpawnReplicas(replicaMgr *meta.ReplicaManager, nodeMgr *session.NodeManager, collection int64, replicaNumber int32) error {
	replicas, err := replicaMgr.Spawn(collection, replicaNumber)
	if err != nil {
		return err
	}
	AssignNodesToReplicas(nodeMgr, replicas...)
	return replicaMgr.Put(replicas...)
}

// RegisterTargets fetch channels and segments of given collection(partitions) from DataCoord,
// and then registers them on Target Manager
func RegisterTargets(ctx context.Context,
	targetMgr *meta.TargetManager,
	broker *meta.CoordinatorBroker,
	collection int64, partitions ...int64) error {
	var dmChannels map[string][]*datapb.VchannelInfo

	for _, partitionID := range partitions {
		vChannelInfos, binlogs, err := broker.GetRecoveryInfo(ctx, collection, partitionID)
		if err != nil {
			return err
		}

		// Register segments
		for _, segmentBinlogs := range binlogs {
			targetMgr.AddSegment(SegmentBinlogs2SegmentInfo(
				collection,
				partitionID,
				segmentBinlogs))
		}

		for _, info := range vChannelInfos {
			channelName := info.GetChannelName()
			dmChannels[channelName] = append(dmChannels[channelName], info)
		}
	}
	// Merge and register channels
	for _, channels := range dmChannels {
		dmChannel := MergeDmChannelInfo(channels)
		targetMgr.AddDmChannel(dmChannel)
	}
	return nil
}
