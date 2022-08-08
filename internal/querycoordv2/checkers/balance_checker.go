package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

// BalanceChecker checks the cluster distribution and generates balance tasks.
type BalanceChecker struct {
	baseChecker
	balance.Balance
}

func (b *BalanceChecker) Description() string {
	return "BalanceChecker checks the cluster distribution and generates balance tasks"
}

func (b *BalanceChecker) Check(ctx context.Context) []task.Task {
	ret := make([]task.Task, 0)
	segmentPlans, channelPlans := b.Balance.Balance()
	tasks := createSegmentTasksFromPlans(segmentPlans)
	ret = append(ret, tasks...)
	tasks = createChannelTasksFromPlans(channelPlans)
	ret = append(ret, tasks...)
	return ret
}

func createSegmentTasksFromPlans(plans []balance.SegmentAssignPlan) []task.Task {
	// TODO(sunby)
	return nil
}

func createChannelTasksFromPlans(plans []balance.ChannelAssignPlan) []task.Task {
	// TODO(sunby)
	return nil
}
