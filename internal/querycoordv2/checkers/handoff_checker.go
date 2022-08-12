package checkers

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	handoffTime = 30 * time.Second
)

// HandoffChecker check target, generate handoff task
type HandoffChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	// todo: right way to choose balance
	balancer *balance.RoundRobinBalance
}

func NewHandoffChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	balancer *balance.RoundRobinBalance) *HandoffChecker {
	return &HandoffChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		balancer:  balancer,
	}
}

func (c *HandoffChecker) Description() string {
	return "HandoffChecker check the handoff segment"
}

func (c *HandoffChecker) Check(ctx context.Context) []task.Task {
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
	return tasks
}

func (c *HandoffChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)
	target := c.targetMgr.GetSegments(replica.CollectionID)

	// filter out handoff segment
	isHandoffSegment := func(segment *datapb.SegmentInfo) bool { return segment.CreatedByCompaction }
	handoffSegments := c.filterSegmentsWithRule(target, isHandoffSegment)

	for _, segment := range handoffSegments {
		ret = append(ret, c.createHandoffTask(ctx, segment, replica))
	}

	return ret
}

func (c *HandoffChecker) filterSegmentsWithRule(segments []*datapb.SegmentInfo, filter func(*datapb.SegmentInfo) bool) []*datapb.SegmentInfo {
	ret := make([]*datapb.SegmentInfo, 0)
	for _, segment := range segments {
		if filter(segment) {
			ret = append(ret, segment)
		}
	}
	return ret
}

func (c *HandoffChecker) createHandoffTask(ctx context.Context, segment *datapb.SegmentInfo, replica *meta.Replica) task.Task {
	baseTask := task.NewBaseTask(ctx, handoffTime, 0, segment.CollectionID, replica.ID)

	actions := make([]task.Action, 0)
	// load new segment
	newNode := c.allocateNodeForSegment(segment.ID)
	actions = append(actions, task.NewSegmentAction(newNode, task.ActionTypeGrow, segment.ID, nil))

	// release compact from segment
	toRelease := c.getReleaseSegmentInfo(segment, replica)
	leftNodes := c.getReplicaNodes(replica)
	plans := c.balancer.AssignSegment(toRelease, leftNodes)

	for _, plan := range plans {
		actions = append(actions, task.NewSegmentAction(plan.To, task.ActionTypeReduce, plan.SegmentID, nil))
	}

	return task.NewSegmentTask(baseTask, actions...)
}

// todo: add exclude rule
func (c *HandoffChecker) getReplicaNodes(replica *meta.Replica) []int64 {
	leftNodes := make([]int64, 0)
	for node := range replica.Nodes {
		leftNodes = append(leftNodes, node)
	}

	return leftNodes
}

func (c *HandoffChecker) getReleaseSegmentInfo(segment *datapb.SegmentInfo, replica *meta.Replica) []*meta.Segment {
	replicaDist := c.getSegmentsDist(replica)
	segments := make([]*meta.Segment, 0)
	for _, segmentID := range segment.CompactionFrom {
		if segment := replicaDist[segmentID]; segment != nil {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (c *HandoffChecker) getSegmentsDist(replica *meta.Replica) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	for _, node := range replica.Nodes.Collect() {
		ret = append(ret, c.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, node)...)
	}
	return ret
}

func (c *HandoffChecker) allocateNodeForSegment(segmentID typeutil.UniqueID) typeutil.UniqueID {
	return 0
}
