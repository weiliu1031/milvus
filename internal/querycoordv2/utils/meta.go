package utils

import (
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
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
