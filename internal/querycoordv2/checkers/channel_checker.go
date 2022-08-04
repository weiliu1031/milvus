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
	"go.uber.org/zap"
)

type ChannelChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	nodeMgr   *session.NodeManager
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

func (checker *ChannelChecker) Check(ctx context.Context) []task.Task {
	collections := checker.meta.CollectionManager.GetAll()
	channels := checker.dist.ChannelDistManager.GetAll()

	channelDist := make(channelDistribution)
	for _, channel := range channels {
		replica := checker.meta.ReplicaManager.GetByCollectionAndNode(channel.GetCollectionID(), channel.Node)
		if replica == nil {
			log.Info("failed to get replica for given collection and node",
				zap.Int64("collection-id", channel.GetCollectionID()),
				zap.Int64("node-id", channel.Node))
			continue
		}

		dist, ok := channelDist[channel.GetChannelName()]
		if !ok {
			dist = make(map[int64]channelSet, 0)
			channelDist[channel.GetChannelName()] = dist
		}

		replicaChannels, ok := dist[replica.ID]
		if !ok {
			replicaChannels = make(channelSet)
			dist[replica.ID] = replicaChannels
		}
		replicaChannels[channel] = struct{}{}
	}

	tasks := checker.checkLack(ctx, collections, channelDist)
	tasks = append(tasks, checker.checkRedundancy(ctx, collections, channelDist)...)
	return tasks
}

func (checker *ChannelChecker) checkLack(ctx context.Context, collections []*meta.Collection, channelDist channelDistribution) []task.Task {
	const (
		LackDmChannelTaskTimeout = 60 * time.Second
	)

	tasks := make([]task.Task, 0)
	for _, collection := range collections {
		log := log.With(
			zap.Int64("collection-id", collection.ID),
		)
		replicas := checker.meta.ReplicaManager.GetByCollection(collection.ID)
		targets := checker.targetMgr.GetDmChannelsByCollection(collection.ID)

		// ChannelName -> Replicas
		toAdd := make(map[string][]int64)
		for _, target := range targets {
			for _, replica := range replicas {
				dist, ok := channelDist[target.GetChannelName()]
				if !ok {
					toAdd[target.GetChannelName()] = append(toAdd[target.GetChannelName()], replica.ID)
					continue
				}

				replicaChannels, ok := dist[replica.ID]
				if !ok || len(replicaChannels) == 0 {
					toAdd[target.GetChannelName()] = append(toAdd[target.GetChannelName()], replica.ID)
				}
			}
		}

		replicaNodes := make(map[int64][]*session.NodeInfo)
		for channel, replicas := range toAdd {
			log := log.With(zap.String("channel", channel))

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
					log.Warn("no node to assign channel")
					continue
				}

				channelTask := task.NewChannelTask(task.NewBaseTask(ctx, LackDmChannelTaskTimeout, checker.ID(), collection.ID, replica),
					task.NewChannelAction(nodes[0].ID(), task.ActionTypeGrow, channel))
				channelTask.SetPriority(task.TaskPriorityHigh)
				tasks = append(tasks, channelTask)
			}
		}
	}

	return tasks
}

func (checker *ChannelChecker) checkRedundancy(ctx context.Context, collections []*meta.Collection, channelDist channelDistribution) []task.Task {
	const (
		RedundantChannelTaskTimeout = 60 * time.Second
	)

	tasks := make([]task.Task, 0)
	for channelName, replicaChannels := range channelDist {
		for replicaID, channels := range replicaChannels {
			if !checker.targetMgr.ContainDmChannel(channelName) {
				for channel := range channels {
					channelTask := task.NewChannelTask(task.NewBaseTask(ctx, RedundantChannelTaskTimeout, checker.ID(), channel.CollectionID, replicaID),
						task.NewChannelAction(channel.Node, task.ActionTypeReduce, channel.GetChannelName()))
					channelTask.SetPriority(task.TaskPriorityNormal)
					tasks = append(tasks, channelTask)
				}
			} else if len(channels) > 1 {
				// Unsub the channel with the minimum version
				var toRemove *meta.DmChannel
				for channel := range channels {
					if toRemove == nil || toRemove.Version > channel.Version {
						toRemove = channel
					}
				}
				channelTask := task.NewChannelTask(task.NewBaseTask(ctx, RedundantChannelTaskTimeout, checker.ID(), toRemove.CollectionID, replicaID),
					task.NewChannelAction(toRemove.Node, task.ActionTypeReduce, toRemove.GetChannelName()))
				channelTask.SetPriority(task.TaskPriorityHigh)
				tasks = append(tasks, channelTask)
			}
		}
	}

	return tasks
}
