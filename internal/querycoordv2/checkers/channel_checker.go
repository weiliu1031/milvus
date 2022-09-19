package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
)

// TODO(sunby): have too much similar codes with SegmentChecker
type ChannelChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	balancer  balance.Balance
}

func NewChannelChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	balancer balance.Balance,
) *ChannelChecker {
	return &ChannelChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		balancer:  balancer,
	}
}

func (c *ChannelChecker) Description() string {
	return "DmChannelChecker checks the lack of DmChannels, or some DmChannels are redundant"
}

func (c *ChannelChecker) Check(ctx context.Context) []task.Task {
	collectionIDs := c.meta.CollectionManager.GetAll()
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		replicas := c.meta.ReplicaManager.GetByCollection(cid)
		for _, r := range replicas {
			tasks = append(tasks, c.checkReplica(ctx, r)...)
		}
	}

	channels := c.dist.ChannelDistManager.GetAll()
	released := utils.FilterReleased(channels, collectionIDs)
	tasks = append(tasks, c.createChannelReduceTasks(ctx, released, -1)...)
	return tasks
}

func (c *ChannelChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)

	lacks, redundancies := utils.GetDmChannelDiff(c.targetMgr, c.dist, c.meta, replica.GetCollectionID(), replica.GetID())
	tasks := c.createChannelLoadTask(ctx, lacks, replica)
	ret = append(ret, tasks...)
	tasks = c.createChannelReduceTasks(ctx, redundancies, replica.GetID())
	ret = append(ret, tasks...)

	repeated := utils.FindRepeatedChannels(c.dist, c.meta, replica.GetID())
	tasks = c.createChannelReduceTasks(ctx, repeated, replica.GetID())
	ret = append(ret, tasks...)
	return ret
}

func (c *ChannelChecker) createChannelLoadTask(ctx context.Context, channels []*meta.DmChannel, replica *meta.Replica) []task.Task {
	plans := c.balancer.AssignChannel(channels, replica.Replica.GetNodes())
	for i := range plans {
		plans[i].ReplicaID = replica.GetID()
	}
	// log.Debug("try to subscribe channels",
	// 	zap.Any("channels", channels),
	// 	zap.Any("plans", plans))
	return balance.CreateChannelTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.ChannelTaskTimeout, plans)
}

func (c *ChannelChecker) createChannelReduceTasks(ctx context.Context, channels []*meta.DmChannel, replicaID int64) []task.Task {
	ret := make([]task.Task, 0, len(channels))
	for _, ch := range channels {
		action := task.NewChannelAction(ch.Node, task.ActionTypeReduce, ch.GetChannelName())
		task := task.NewChannelTask(ctx, Params.QueryCoordCfg.ChannelTaskTimeout, c.ID(), ch.GetCollectionID(), replicaID, action)
		ret = append(ret, task)
	}
	return ret
}
