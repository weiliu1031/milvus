package checkers

import (
	"context"
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type SegmentChecker struct {
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	nodeMgr   *session.NodeManager
}

func NewSegmentChecker(dist *meta.DistributionManager, targetMgr *meta.TargetManager) *SegmentChecker {
	return &SegmentChecker{
		dist:      dist,
		targetMgr: targetMgr,
	}
}

// SegmentID, ReplicaID -> Nodes
type segmentDistribution map[int64]map[int64]typeutil.UniqueSet

func (checker *SegmentChecker) Check(nodeID int64) []task.Task {
	collections := checker.meta.CollectionManager.GetAll()
	segments := checker.dist.SegmentDistManager.GetAll()

	segmentDist := make(segmentDistribution)
	for _, segment := range segments {
		replica := checker.meta.ReplicaManager.GetByCollectionAndNode(segment.GetCollectionID(), segment.Node)
		if replica == nil {
			log.Info("failed to get replica for given collection and node",
				zap.Int64("collection-id", segment.GetCollectionID()),
				zap.Int64("node-id", segment.Node))
			continue
		}

		dist, ok := segmentDist[segment.GetID()]
		if !ok {
			dist = make(map[int64]typeutil.UniqueSet, 0)
			segmentDist[segment.GetID()] = dist
		}

		nodes, ok := dist[replica.ID]
		if !ok {
			nodes = make(typeutil.UniqueSet)
			dist[replica.ID] = nodes
		}
		nodes.Insert(segment.Node)
	}

	tasks := make([]task.Task, 0)
	tasks = append(tasks, checker.checkLackSegment(collections, segmentDist)...)

	return tasks
}

func (checker *SegmentChecker) checkLackSegment(collections []*meta.Collection, segmentDist segmentDistribution) []task.Task {
	tasks := make([]task.Task, 0)

	for _, collection := range collections {
		log := log.With(
			zap.Int64("collection-id", collection.CollectionID),
		)
		replicas := checker.meta.ReplicaManager.GetByCollection(collection.CollectionID)
		targets := checker.targetMgr.GetSegmentsByCollection(collection.CollectionID)

		// SegmentID -> Replicas
		toAdd := make(map[int64][]*meta.Replica)
		for _, target := range targets {
			for _, replica := range replicas {
				dist, ok := segmentDist[target.ID]
				if !ok {
					toAdd[target.ID] = append(toAdd[target.ID], replica)
					continue
				}

				nodes, ok := dist[replica.ID]
				if !ok || len(nodes) == 0 {
					toAdd[target.ID] = append(toAdd[target.ID], replica)
				}
			}
		}

		replicaNodes := make(map[int64][]*session.NodeInfo)
		for segmentID, replicas := range toAdd {
			log = log.With(zap.Int64("segment-id", segmentID))

			for _, replica := range replicas {
				log = log.With(zap.Int64("replica-id", replica.ID))

				nodes, ok := replicaNodes[replica.ID]
				if !ok {
					nodes = make([]*session.NodeInfo, 0, len(replica.Nodes))
					for nodeID := range replica.Nodes {
						node := checker.nodeMgr.Get(nodeID)
						if node != nil {
							nodes = append(nodes, node)
						}
					}
					sort.Slice(nodes, func(i, j int) bool {
						return nodes[i].GetScore() < nodes[i].GetScore()
					})
					replicaNodes[replica.ID] = nodes
				}

				if len(nodes) == 0 {
					log.Warn("no node to assign segment")
					continue
				}

				ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
				segmentTask := task.NewSegmentTask(task.NewBaseTask(ctx, 0, collection.CollectionID, replica.ID),
					task.NewSegmentAction(nodes[0].ID(), task.ActionTypeGrow, segmentID))
				tasks = append(tasks, segmentTask)
			}
		}
	}

	return tasks
}
