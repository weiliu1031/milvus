package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
)

type SegmentChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	balancer  balance.Balance
}

func NewSegmentChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	balancer balance.Balance,
) *SegmentChecker {
	return &SegmentChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		balancer:  balancer,
	}
}

func (c *SegmentChecker) Description() string {
	return "SegmentChecker checks the lack of segments, or some segments are redundant"
}

func (c *SegmentChecker) Check(ctx context.Context) []task.Task {
	collectionIDs := c.meta.CollectionManager.GetAll()
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
	tasks = append(tasks, c.createSegmentReduceTasks(ctx, released, -1, querypb.DataScope_All)...)
	return tasks
}

func (c *SegmentChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)

	// compare with targets to find the lack and redundancy of segments
	lacks, redundancies := utils.GetHistoricalSegmentDiff(c.targetMgr, c.dist, c.meta, replica.GetCollectionID(), replica.GetID())
	tasks := c.createSegmentLoadTasks(ctx, lacks, replica)
	ret = append(ret, tasks...)

	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID(), querypb.DataScope_All)
	ret = append(ret, tasks...)

	// compare inner dists to find repeated loaded segments
	redundancies = utils.FindRepeatedHistoricalSegments(c.dist, c.meta, replica.GetID())
	redundancies = c.filterExistedOnLeader(replica, redundancies)
	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID(), querypb.DataScope_All)
	ret = append(ret, tasks...)

	// compare with target to find the lack and redundancy of segments
	// todo: should we deal with the lack of growing segment here ?
	_, redundancies = utils.GetStreamingSegmentDiff(c.targetMgr, c.dist, c.meta, replica.GetCollectionID(), replica.GetID())
	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID(), querypb.DataScope_Streaming)
	ret = append(ret, tasks...)

	return ret
}

func (c *SegmentChecker) filterExistedOnLeader(replica *meta.Replica, segments []*meta.Segment) []*meta.Segment {
	filtered := make([]*meta.Segment, 0, len(segments))
	for _, s := range segments {
		leaderID, ok := c.dist.ChannelDistManager.GetShardLeader(replica, s.GetInsertChannel())
		if !ok {
			continue
		}
		onLeader := false
		leaderViews := c.dist.LeaderViewManager.GetLeaderView(leaderID)
		for _, view := range leaderViews {
			node, ok := view.Segments[s.GetID()]
			if ok && node == s.Node {
				onLeader = true
				break
			}
		}
		if onLeader {
			// if this segment is serving on leader, do not remove it for search available
			continue
		}
		filtered = append(filtered, s)
	}
	return filtered
}

func (c *SegmentChecker) createSegmentLoadTasks(ctx context.Context, segments []*datapb.SegmentInfo, replica *meta.Replica) []task.Task {
	if len(segments) == 0 {
		return nil
	}
	packedSegments := make([]*meta.Segment, 0, len(segments))
	for _, s := range segments {
		if len(c.dist.LeaderViewManager.GetLeadersByShard(s.GetInsertChannel())) == 0 {
			continue
		}
		packedSegments = append(packedSegments, &meta.Segment{SegmentInfo: s})
	}
	plans := c.balancer.AssignSegment(packedSegments, replica.Replica.GetNodes())
	for i := range plans {
		plans[i].ReplicaID = replica.GetID()
	}
	return balance.CreateSegmentTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.SegmentTaskTimeout, plans)
}

func (c *SegmentChecker) createSegmentReduceTasks(ctx context.Context, segments []*meta.Segment, replicaID int64, scope querypb.DataScope) []task.Task {
	ret := make([]task.Task, 0, len(segments))
	for _, s := range segments {
		action := task.NewSegmentActionWithScope(s.Node, task.ActionTypeReduce, s.GetID(), scope)
		ret = append(ret, task.NewSegmentTask(
			ctx,
			Params.QueryCoordCfg.SegmentTaskTimeout,
			c.ID(),
			s.GetCollectionID(),
			replicaID,
			action,
		))
	}
	return ret
}
