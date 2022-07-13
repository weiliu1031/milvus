package checkers

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	handoffTimeout = 30 * time.Second
)

// HandoffChecker check target, generate handoff task
type HandoffChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
}

func NewHandoffChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager) *HandoffChecker {
	return &HandoffChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
	}
}

func (c *HandoffChecker) Description() string {
	return "HandoffChecker check the handoff segment"
}

func (c *HandoffChecker) Check(ctx context.Context) []task.Task {
	tasks := make([]task.Task, 0)
	for segmentID := range c.targetMgr.GetGrowingSegmentToRelease() {
		if c.targetMgr.ContainSegment(segmentID) {
			c.targetMgr.RemoveSegment(segmentID)
			c.targetMgr.UnregisterGrowingSegmentToRelease(segmentID)
		} else {
			segmentsToRelease := c.getSegmentInfo(segmentID)
			for _, segment := range segmentsToRelease {
				tasks = append(tasks, c.generateReleaseSegmentTask(ctx, segment))
			}
		}
	}
	return tasks
}

func (c *HandoffChecker) generateReleaseSegmentTask(ctx context.Context, segment *meta.Segment) task.Task {
	onReleaseDown := func() { c.targetMgr.UnregisterGrowingSegmentToRelease(segment.ID) }

	releaseAction := task.NewSegmentAction(segment.Node, task.ActionTypeReduce, segment.ID, onReleaseDown)
	return task.NewSegmentTask(ctx, handoffTimeout, 0, segment.CollectionID, segment.PartitionID, releaseAction)
}

func (c *HandoffChecker) getSegmentInfo(segmentID typeutil.UniqueID) []*meta.Segment {
	return c.dist.SegmentDistManager.Get(segmentID)
}

func (c *HandoffChecker) getSegmentsDist(replica *meta.Replica) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	for _, node := range replica.Nodes.Collect() {
		ret = append(ret, c.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, node)...)
	}
	return ret
}
