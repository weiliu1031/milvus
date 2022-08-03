package checkers

import (
	"context"
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

type SegmentChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	nodeMgr   *session.NodeManager
}

func NewSegmentChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	nodeMgr *session.NodeManager,
) *SegmentChecker {
	return &SegmentChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		nodeMgr:   nodeMgr,
	}
}

func (checker *SegmentChecker) Description() string {
	return "SegmentChecker checks the lack of segments, or some segments are redundant"
}

// SegmentID, ReplicaID -> Segments
type segmentSet map[*meta.Segment]struct{}
type segmentDistribution map[int64]map[int64]segmentSet

func (checker *SegmentChecker) Check(ctx context.Context) []task.Task {
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
			dist = make(map[int64]segmentSet, 0)
			segmentDist[segment.GetID()] = dist
		}

		replicaSegments, ok := dist[replica.ID]
		if !ok {
			replicaSegments = make(segmentSet)
			dist[replica.ID] = replicaSegments
		}
		replicaSegments[segment] = struct{}{}
	}

	tasks := checker.checkLack(ctx, collections, segmentDist)
	tasks = append(tasks, checker.checkRedundancy(ctx, segmentDist)...)
	return tasks
}

// checkLackSegment checks lack of segments,
// returns tasks that load segments,
// for each of given collections:
// 1. Get target segments of the collection,
// 2. For each target segment, check whether it is loaded within all replicas of the collection,
// 3. Spawn SegmentTask to load lacking segments.
func (checker *SegmentChecker) checkLack(ctx context.Context, collections []*meta.Collection, segmentDist segmentDistribution) []task.Task {
	const (
		LackSegmentTaskTimeout = 60 * time.Second
	)

	tasks := make([]task.Task, 0)
	for _, collection := range collections {
		log := log.With(
			zap.Int64("collection-id", collection.ID),
		)
		replicas := checker.meta.ReplicaManager.GetByCollection(collection.ID)
		targets := checker.targetMgr.GetSegmentsByCollection(collection.ID)

		// SegmentID -> Replicas
		toAdd := make(map[int64][]int64)
		for _, target := range targets {
			for _, replica := range replicas {
				dist, ok := segmentDist[target.ID]
				if !ok {
					toAdd[target.ID] = append(toAdd[target.ID], replica.ID)
					continue
				}

				replicaSegments, ok := dist[replica.ID]
				if !ok || len(replicaSegments) == 0 {
					toAdd[target.ID] = append(toAdd[target.ID], replica.ID)
				}
			}
		}

		replicaNodes := make(map[int64][]*session.NodeInfo)
		for segmentID, replicas := range toAdd {
			log := log.With(zap.Int64("segment-id", segmentID))

			for _, replica := range replicas {
				log := log.With(zap.Int64("replica-id", replica))

				nodes, ok := replicaNodes[replica]
				if !ok {
					nodes = utils.GetReplicaNodesInfo(checker.meta.ReplicaManager, checker.nodeMgr, replica)
					sort.Slice(nodes, func(i, j int) bool {
						return nodes[i].GetScore() < nodes[i].GetScore()
					})
					replicaNodes[replica] = nodes
				}

				if len(nodes) == 0 {
					log.Warn("no node to assign segment")
					continue
				}

				segmentTask := task.NewSegmentTask(task.NewBaseTask(ctx, LackSegmentTaskTimeout, checker.ID(), collection.ID, replica),
					task.NewSegmentAction(nodes[0].ID(), task.ActionTypeGrow, segmentID))
				if collection.Status == meta.CollectionStatusLoading {
					segmentTask.SetPriority(task.TaskPriorityNormal)
				} else {
					segmentTask.SetPriority(task.TaskPriorityHigh)
				}
				tasks = append(tasks, segmentTask)
			}
		}
	}

	return tasks
}

// checkRedundantSegment checks whether redundant segments exist,
// 1. Remove all segments not in target manager
// 2. Remove the segment with minimum version if it's not hold by any leader
// returns tasks that release segments.
func (checker *SegmentChecker) checkRedundancy(ctx context.Context, segmentDist segmentDistribution) []task.Task {
	const (
		RedundantSegmentTaskTimeout = 30 * time.Second
	)

	// todo(yah01): check replica number changed

	tasks := make([]task.Task, 0)
	for segmentID, replicaSegments := range segmentDist {
		for replicaID, segments := range replicaSegments {
			if !checker.targetMgr.ContainSegment(segmentID) { // The segment is compacted or the collection/partition has been released
				for segment := range segments {
					segmentTask := task.NewSegmentTask(task.NewBaseTask(ctx, RedundantSegmentTaskTimeout, checker.ID(), segment.CollectionID, replicaID),
						task.NewSegmentAction(segment.Node, task.ActionTypeReduce, segment.ID))
					segmentTask.SetPriority(task.TaskPriorityNormal)
					tasks = append(tasks, segmentTask)
				}
			} else if len(segments) > 1 { // Redundant segments exist
				// Release the segment with minimum version
				var toRemove *meta.Segment
				for segment := range segments {
					if toRemove == nil || toRemove.Version > segment.Version {
						toRemove = segment
					}
				}

				if !lo.Contains(checker.dist.LeaderViewManager.GetSegmentByNode(toRemove.Node),
					toRemove.ID) {
					segmentTask := task.NewSegmentTask(task.NewBaseTask(ctx, RedundantSegmentTaskTimeout, checker.ID(), toRemove.CollectionID, replicaID),
						task.NewSegmentAction(toRemove.Node, task.ActionTypeReduce, toRemove.ID))
					segmentTask.SetPriority(task.TaskPriorityNormal)
					tasks = append(tasks, segmentTask)
				}
			}
		}
	}

	return tasks
}
