package balance

import (
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type RowCountBasedBalancer struct {
	RoundRobinBalancer
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
}

func (b *RowCountBasedBalancer) AssignSegment(segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
	nodesInfo := b.getNodes(nodes)
	nodeItems := b.convertToNodeItems(nodesInfo)
	queue := newPriorityQueue()
	for _, ni := range nodeItems {
		queue.push(&ni)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
	})

	plans := make([]SegmentAssignPlan, 0, len(segments))
	for _, s := range segments {
		// pick the node with the least row count and allocate to it.
		ni := queue.pop().(*nodeItem)
		plan := SegmentAssignPlan{
			From:    -1,
			To:      ni.nodeID,
			Segment: s,
		}
		plans = append(plans, plan)
		// change node's priority and push back
		p := ni.getPriority()
		ni.setPriority(p - int(s.GetNumOfRows()))
		queue.push(ni)
	}
	return plans
}

func (b *RowCountBasedBalancer) convertToNodeItems(nodesInfo []*session.NodeInfo) []nodeItem {
	ret := make([]nodeItem, 0, len(nodesInfo))
	for _, node := range nodesInfo {
		segments := b.dist.SegmentDistManager.GetByNode(node.ID())
		rowcnt := 0
		for _, s := range segments {
			rowcnt += int(s.GetNumOfRows())
		}
		// more row count, less priority
		nodeItem := newNodeItem(-rowcnt, node.ID())
		ret = append(ret, nodeItem)
	}
	return ret
}

func (b *RowCountBasedBalancer) Balance() ([]SegmentAssignPlan, []ChannelAssignPlan) {
	nodesInfo := b.nodeManager.GetAll()
	nodesRowCnt := make(map[int64]int)
	nodesSegments := make(map[int64][]*meta.Segment)
	totalCnt := 0
	for _, ni := range nodesInfo {
		segments := b.dist.SegmentDistManager.GetByNode(ni.ID())
		cnt := 0
		for _, s := range segments {
			cnt += int(s.GetNumOfRows())
		}
		nodesRowCnt[ni.ID()] = cnt
		nodesSegments[ni.ID()] = segments
		totalCnt += cnt
	}

	average := totalCnt / len(nodesInfo)
	neededRowCnt := 0
	for _, rowcnt := range nodesRowCnt {
		if rowcnt < average {
			neededRowCnt += average - rowcnt
		}
	}

	if neededRowCnt == 0 {
		return nil, nil
	}

	segmentsToMove := make([]*meta.Segment, 0)

	// select segments to be moved
outer:
	for nodeID, rowcnt := range nodesRowCnt {
		if rowcnt <= average {
			continue
		}
		segments := nodesSegments[nodeID]
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
		})

		for _, s := range segments {
			if rowcnt-int(s.GetNumOfRows()) < average {
				continue
			}
			rowcnt -= int(s.GetNumOfRows())
			segmentsToMove = append(segmentsToMove, s)
			neededRowCnt -= int(s.GetNumOfRows())
			if neededRowCnt <= 0 {
				break outer
			}
		}
	}

	sort.Slice(segmentsToMove, func(i, j int) bool {
		return segmentsToMove[i].GetNumOfRows() < segmentsToMove[j].GetNumOfRows()
	})
	// allocate segments to those nodes with row cnt less than average
	plans := make([]SegmentAssignPlan, 0)
	start := 0
	for nodeID, rowcnt := range nodesRowCnt {
		if rowcnt >= average {
			continue
		}
		for i := start; i < len(segmentsToMove); i, start = i+1, start+1 {
			if rowcnt >= average {
				break
			}
			s := segmentsToMove[i]
			plan := SegmentAssignPlan{
				From:    s.Node,
				To:      nodeID,
				Segment: s,
			}
			plans = append(plans, plan)
			rowcnt += int(s.GetNumOfRows())
		}
	}
	return plans, nil
}

func NewRowCountBasedBalancer(
	scheduler *task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
) *RowCountBasedBalancer {
	return &RowCountBasedBalancer{
		RoundRobinBalancer: *NewRoundRobinBalancer(scheduler, nodeManager),
		nodeManager:        nodeManager,
		dist:               dist,
	}
}

type nodeItem struct {
	baseItem
	nodeID int64
}

func newNodeItem(priority int, nodeID int64) nodeItem {
	return nodeItem{
		baseItem: baseItem{
			priority: priority,
		},
		nodeID: nodeID,
	}
}
