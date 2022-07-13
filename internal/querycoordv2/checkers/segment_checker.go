package checkers

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
)

const segmentTaskTimeout = 10 * time.Second

type SegmentChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	broker    *meta.CoordinatorBroker
	nodeMgr   *session.NodeManager
	balancer  balance.Balance
}

func NewSegmentChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	broker *meta.CoordinatorBroker,
	nodeMgr *session.NodeManager,
) *SegmentChecker {
	return &SegmentChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		broker:    broker,
		nodeMgr:   nodeMgr,
	}
}

func (c *SegmentChecker) Description() string {
	return "SegmentChecker checks the lack of segments, or some segments are redundant"
}

func (c *SegmentChecker) Check(ctx context.Context) []task.Task {
	collections := c.meta.CollectionManager.GetAllCollections()
	partitions := c.meta.CollectionManager.GetAllPartitions()
	collectionIDs := utils.Collect(collections, partitions)
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		replicas := c.meta.ReplicaManager.GetByCollection(cid)
		for _, r := range replicas {
			tasks = append(tasks, c.checkReplica(ctx, r)...)
		}
	}

	// find already released segments which are not contained in target
	segments := c.dist.SegmentDistManager.GetAll()
	released := utils.FilterReleased(segments, collectionIDs)
	tasks = append(tasks, c.createSegmentReduceTasks(ctx, released, -1)...)
	return tasks
}

func (c *SegmentChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)
	targets := c.targetMgr.GetSegmentsByCollection(replica.CollectionID)
	dists := c.getSegmentsDist(replica)

	// compare with targets to find the lack and redundancy of segments
	lacks, redundancies := diffSegments(targets, dists)
	tasks := c.createSegmentLoadTasks(ctx,  lacks, replica)
	ret = append(ret, tasks...)

	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID())
	ret = append(ret, tasks...)

	// compare inner dists to find repeated loaded segments
	redundancies = findRepeatedSegments(dists)
  redundancies = c.filterExistedOnLeader(replica, redundancies)
	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID())
	ret = append(ret, tasks...)
	return ret
}

func (c *SegmentChecker) getSegmentsDist(replica *meta.Replica) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	for _, node := range replica.Nodes.Collect() {
		ret = append(ret, c.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, node)...)
	}
	return ret
}

func diffSegments(targets []*datapb.SegmentInfo, dists []*meta.Segment) (lacks []*datapb.SegmentInfo, redundancies []*meta.Segment) {
	distMap := make(map[int64]struct{})
	targetMap := make(map[int64]struct{})
	for _, s := range targets {
		targetMap[s.GetID()] = struct{}{}
	}
	for _, s := range dists {
		distMap[s.GetID()] = struct{}{}
		if _, ok := targetMap[s.GetID()]; !ok {
			redundancies = append(redundancies, s)
		}
	}
	for _, s := range targets {
		if _, ok := distMap[s.GetID()]; !ok {
			lacks = append(lacks, s)
		}
	}
	return
}

func findRepeatedSegments(dists []*meta.Segment) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	versions := make(map[int64]*meta.Segment)
	for _, s := range dists {
		maxVer, ok := versions[s.GetID()]
		if !ok {
			versions[s.GetID()] = s
			continue
		}
		if maxVer.Version <= s.Version {
			ret = append(ret, maxVer)
			versions[s.GetID()] = s
		} else {
			ret = append(ret, s)
		}
	}
	return ret
}

func (c *SegmentChecker) filterExistedOnLeader(replica *meta.Replica, segments []*meta.Segment) []*meta.Segment {
  filtered := make([]*meta.Segment, 0, len(segments))
  for _, s :=range segments {
    leaderID, ok :=  c.dist.ChannelDistManager.GetShardLeader(replica, s.GetInsertChannel())
    if !ok {
      continue
    }
    leaderView := c.dist.LeaderViewManager.GetLeaderView(leaderID)
    node, ok :=  leaderView.Segments[s.GetID()]
    if ok && node == s.Node {
      // if this segment is serving on leader, do not remove it for search available
      continue
    }
    filtered = append(filtered, s)
  }
  return filtered
}

func (c *SegmentChecker) createSegmentLoadTasks(ctx context.Context, segments []*datapb.SegmentInfo, replica *meta.Replica) []task.Task {
	packedSegments := make([]*meta.Segment, 0, len(segments))
	for _, s := range segments {
		packedSegments = append(packedSegments, &meta.Segment{SegmentInfo: s})
	}
	plans := c.balancer.AssignSegment(packedSegments, replica.GetNodes())
	return c.createSegmentTaskFromPlans(ctx, plans, replica.GetID())
}

func (c *SegmentChecker) createSegmentReduceTasks(ctx context.Context, segments []*meta.Segment, replicaID int64) []task.Task {
	ret := make([]task.Task, 0, len(segments))
	for _, s := range segments {
		action := task.NewSegmentAction(s.Node, task.ActionTypeReduce, s.GetID())
		ret = append(ret, task.NewSegmentTask(
      ctx, 
      segmentTaskTimeout, 
      c.ID(), 
      s.GetCollectionID(), 
      replicaID, 
      action,
    ))
	}
	return ret
}

func (c *SegmentChecker) createSegmentTaskFromPlans(ctx context.Context, plans []balance.SegmentAssignPlan, replicaID int64) []task.Task {
	ret := make([]task.Task, 0)
	for _, p := range plans {
		actions := make([]task.Action, 0)
		if p.To != -1 {
			action := task.NewSegmentAction(p.To, task.ActionTypeGrow, p.Segment.GetID())
			actions = append(actions, action)
		}
		if p.From != -1 {
			action := task.NewSegmentAction(p.From, task.ActionTypeReduce, p.Segment.GetID())
			actions = append(actions, action)
		}
		task := task.NewSegmentTask(
      ctx, 
      segmentTaskTimeout, 
      c.ID(), 
      p.Segment.GetCollectionID(), 
      replicaID, 
      actions...,
    )
		ret = append(ret, task)
	}
	return ret
}
