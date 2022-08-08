package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
)

type ChannelChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	nodeMgr   *session.NodeManager
	balancer  balance.Balance
}

func NewChannelChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	nodeMgr *session.NodeManager,
) *ChannelChecker {
	return &ChannelChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		nodeMgr:   nodeMgr,
	}
}

func (checker *ChannelChecker) Description() string {
	return "DmChannelChecker checks the lack of DmChannels, or some DmChannels are redundant"
}

// ChannelName, ReplicaID -> Nodes
type channelSet map[*meta.DmChannel]struct{}
type channelDistribution map[string]map[int64]channelSet

func (c *ChannelChecker) Check(ctx context.Context) []task.Task {
	collections := c.meta.CollectionManager.GetAllCollections()
	partitions := c.meta.CollectionManager.GetAllPartitions()
	collectionIDs := utils.Collect(collections, partitions)
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		replicas := c.meta.ReplicaManager.GetByCollection(cid)
		for _, r := range replicas {
			tasks = append(tasks, c.checkReplica(r)...)
		}
	}
	return tasks
}

func (c *ChannelChecker) checkReplica(replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)
	targets := c.targetMgr.GetDmChannelsByCollection(replica.GetCollectionID())
	dists := c.getChannelDist(replica)

	lacks, redundancies := diffChannels(targets, dists)
	tasks := createChannelLoadTask(c.balancer, lacks, replica)
	ret = append(ret, tasks...)
	tasks = createChannelReduceTasks(redundancies)
	ret = append(ret, tasks...)

	repeated := findRepeatedChannels(dists)
	tasks = createChannelReduceTasks(repeated)
	ret = append(ret, tasks...)
	return nil
}

func (c *ChannelChecker) getChannelDist(replica *meta.Replica) []*meta.DmChannel {
	dists := make([]*meta.DmChannel, 0)
	for _, nodeID := range replica.Nodes.Collect() {
		dists = append(dists, c.dist.ChannelDistManager.GetByNodeAndCollection(nodeID, replica.GetCollectionID())...)
	}
	return dists
}

func diffChannels(targets, dists []*meta.DmChannel) (lacks, redundancies []*meta.DmChannel) {
	distMap := make(map[string]struct{})
	targetMap := make(map[string]struct{})
	for _, ch := range targets {
		targetMap[ch.GetChannelName()] = struct{}{}
	}
	for _, ch := range dists {
		distMap[ch.GetChannelName()] = struct{}{}
		if _, ok := targetMap[ch.GetChannelName()]; !ok {
			redundancies = append(redundancies, ch)
		}
	}
	for _, ch := range targets {
		if _, ok := distMap[ch.GetChannelName()]; !ok {
			lacks = append(lacks, ch)
		}
	}
	return
}

func findRepeatedChannels(dists []*meta.DmChannel) []*meta.DmChannel {
	ret := make([]*meta.DmChannel, 0)
	versionsMap := make(map[string]*meta.DmChannel)
	for _, ch := range dists {
		maxVer, ok := versionsMap[ch.GetChannelName()]
		if !ok {
			versionsMap[ch.GetChannelName()] = ch
			continue
		}
		if maxVer.Version <= ch.Version {
			ret = append(ret, maxVer)
			versionsMap[ch.GetChannelName()] = ch
		} else {
			ret = append(ret, ch)
		}
	}
	return ret
}

func createChannelLoadTask(balancer balance.Balance, channels []*meta.DmChannel, replica *meta.Replica) []task.Task {
	// TODO(sunby)
	return nil
}

func createChannelReduceTasks(channels []*meta.DmChannel) []task.Task {
	// TODO(sunby)
	return nil
}
