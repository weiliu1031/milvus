package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

const (
	actionTimeout = 10 * time.Second
)

type actionIndex struct {
	TaskID int64
	Step   int
}

type Executor struct {
	meta      *meta.Meta
	dist      *meta.DistributionManager
	broker    *meta.CoordinatorBroker
	targetMgr *meta.TargetManager
	cluster   *session.Cluster
	nodeMgr   *session.NodeManager

	executingActions sync.Map
}

func NewExecutor(meta *meta.Meta,
	dist *meta.DistributionManager,
	broker *meta.CoordinatorBroker,
	targetMgr *meta.TargetManager,
	cluster *session.Cluster,
	nodeMgr *session.NodeManager) *Executor {
	return &Executor{
		meta:      meta,
		dist:      dist,
		broker:    broker,
		targetMgr: targetMgr,
		cluster:   cluster,
		nodeMgr:   nodeMgr,

		executingActions: sync.Map{},
	}
}

// Execute executes the given action,
// does nothing and returns false if the action is already committed,
// returns true otherwise.
func (ex *Executor) Execute(task Task, step int, action Action) bool {
	log := log.With(
		zap.Int64("source-id", task.SourceID()),
		zap.Int64("replica-id", task.ReplicaID()),
		zap.Int64("task-id", task.ID()),
		zap.Int("step", step),
	)

	index := actionIndex{
		TaskID: task.ID(),
		Step:   step,
	}
	_, exist := ex.executingActions.LoadOrStore(index, struct{}{})
	if exist {
		return false
	}

	go func() {
		log.Info("execute the action of task")
		switch action := action.(type) {
		case *SegmentAction:
			ex.executeSegmentAction(task.(*SegmentTask), action)

		case *ChannelAction:
			ex.executeDmChannelAction(task.(*ChannelTask), action)

		default:
			panic(fmt.Sprintf("forget to process action type: %+v", action))
		}

		ex.executingActions.Delete(index)
	}()

	return true
}

func (ex *Executor) executeSegmentAction(task *SegmentTask, action *SegmentAction) {
	switch action.Type() {
	case ActionTypeGrow:
		ex.loadSegment(task, action)

	case ActionTypeReduce:
		ex.releaseSegment(task, action)

	default:
		panic(fmt.Sprintf("invalid action type: %+v", action.Type()))
	}
}

func (ex *Executor) loadSegment(task *SegmentTask, action *SegmentAction) {
	if action.Type() != ActionTypeGrow {
		panic("load segment action type not matched")
	}

	log := log.With(
		zap.Int64("source-id", task.SourceID()),
		zap.Int64("task-id", task.ID()),
		zap.Int64("collection-id", task.CollectionID()),
		zap.Int64("replica-id", task.ReplicaID()),
		zap.Int64("segment-id", task.segmentID),
		zap.Int64("node-id", action.Node()),
	)

	ctx, cancel := context.WithTimeout(task.Context(), actionTimeout)
	defer cancel()

	collection := ex.meta.CollectionManager.Get(task.CollectionID())
	if collection == nil {
		log.Warn("failed to get collection")
		return
	}

	segment, err := ex.broker.GetSegmentInfo(ctx, task.segmentID)
	if err != nil {
		log.Warn("failed to get segment info from DataCoord", zap.Error(err))
		return
	}
	indexes, err := ex.broker.GetIndexInfo(ctx, collection.Schema, collection.ID, segment.ID)
	if err != nil {
		log.Warn("failed to get index of segment, will load without index")
	}
	loadInfo := packSegmentLoadInfo(segment, indexes)

	// Get shard leader for the given replica and segment
	replica := ex.meta.ReplicaManager.GetByCollectionAndNode(task.CollectionID(), action.Node())
	if replica == nil {
		log.Warn("failed to get replica for given collection and node")
		return
	}
	leader, ok := ex.dist.GetShardLeader(replica, segment.GetInsertChannel())
	if !ok {
		log.Warn("no shard leader for the segment to execute loading", zap.String("shard", segment.GetInsertChannel()))
		return
	}
	log = log.With(zap.Int64("shard-leader", leader))

	// Pre-allocate memory for loading segment
	node := ex.nodeMgr.Get(action.Node())
	if node == nil {
		log.Warn("failed to get node, the task may be stale")
		return
	}
	// TODO(yah01): move to checker
	ok, release := node.PreAllocate(loadInfo.SegmentSize)
	if !ok {
		log.Warn("no enough memory to pre-allocate for loading segment",
			zap.Int64("node-memory-remaining", node.Remaining()),
			zap.Int64("segment-size", loadInfo.SegmentSize))
		return
	}
	//TODO(yah01): move to OnStepDone
	defer release()

	req := packLoadSegmentRequest(task, action, collection, loadInfo)
	status, err := ex.cluster.LoadSegments(ctx, leader, req)
	if err != nil {
		log.Warn("failed to load segment, it may be a false failure", zap.Error(err))
		return
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to load segment", zap.String("reason", status.GetReason()))
		return
	}
}

func (ex *Executor) releaseSegment(task *SegmentTask, action *SegmentAction) {
	if action.Type() != ActionTypeReduce {
		panic("release segment action type not matched")
	}

	log := log.With(
		zap.Int64("source-id", task.SourceID()),
		zap.Int64("task-id", task.ID()),
		zap.Int64("collection-id", task.CollectionID()),
		zap.Int64("replica-id", task.ReplicaID()),
		zap.Int64("segment-id", task.segmentID),
		zap.Int64("node-id", action.Node()),
	)

	ctx, cancel := context.WithTimeout(task.Context(), actionTimeout)
	defer cancel()

	var targetSegment *meta.Segment
	segments := ex.dist.SegmentDistManager.GetByNode(action.Node())
	for _, segment := range segments {
		if segment.ID == task.SegmentID() {
			targetSegment = segment
			break
		}
	}
	if targetSegment == nil {
		log.Warn("segment to release not found in distribution")
		return
	}

	// Get shard leader for the given replica and segment
	replica := ex.meta.ReplicaManager.GetByCollectionAndNode(task.CollectionID(), action.Node())
	if replica == nil {
		log.Warn("failed to get replica for given collection and node")
		return
	}
	leader, ok := ex.dist.GetShardLeader(replica, targetSegment.GetInsertChannel())
	if !ok {
		log.Warn("no shard leader for the segment to execute loading", zap.String("shard", targetSegment.GetInsertChannel()))
		return
	}
	log = log.With(zap.Int64("shard-leader", leader))

	req := packReleaseSegmentRequest(task, action)
	status, err := ex.cluster.ReleaseSegments(ctx, leader, req)
	if err != nil {
		log.Warn("failed to release segment, it may be a false failure", zap.Error(err))
		return
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to release segment", zap.String("reason", status.GetReason()))
		return
	}
}

func (ex *Executor) executeDmChannelAction(task *ChannelTask, action *ChannelAction) {
	ctx, cancel := context.WithTimeout(task.Context(), actionTimeout)
	defer cancel()

	switch action.Type() {
	case ActionTypeGrow:
		collection := ex.meta.CollectionManager.Get(task.CollectionID())
		if collection == nil {
			log.Warn("failed to get collection")
			return
		}

		channels := make([]*datapb.VchannelInfo, 0, len(collection.Partitions))
		for _, partition := range collection.Partitions {
			vchannels, _, err := ex.broker.GetRecoveryInfo(ctx, task.CollectionID(), partition)
			if err != nil {
				log.Warn("failed to get vchannel from DataCoord", zap.Error(err))
				return
			}

			for _, channel := range vchannels {
				if channel.ChannelName == action.ChannelName() {
					channels = append(channels, channel)
				}
			}
		}

		dmChannel := utils.MergeDmChannelInfo(channels)
		req := packSubDmChannelRequest(task, action, collection, dmChannel)
		status, err := ex.cluster.WatchDmChannels(action.ctx, action.Node(), req)
		if err != nil {
			log.Warn("failed to sub DmChannel, it may be a false failure", zap.Error(err))
			return
		}
		if status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("failed to sub DmChannel", zap.String("reason", status.GetReason()))
			return
		}

	case ActionTypeReduce:
		// TODO(yah01): unsub DmChannel

	default:
		panic(fmt.Sprintf("invalid action type: %+v", action.Type()))
	}
}

func (ex *Executor) subDmChannel(task *ChannelTask, action *ChannelAction) {
	log := log.With(
		zap.Int64("source-id", task.SourceID()),
		zap.Int64("task-id", task.ID()),
		zap.Int64("collection-id", task.CollectionID()),
		zap.Int64("replica-id", task.ReplicaID()),
		zap.String("channel", task.Channel()),
		zap.Int64("node-id", action.Node()),
	)

	ctx, cancel := context.WithTimeout(task.Context(), actionTimeout)
	defer cancel()

	collection := ex.meta.CollectionManager.Get(task.CollectionID())
	if collection == nil {
		log.Warn("failed to get collection")
		return
	}

	channels := make([]*datapb.VchannelInfo, 0, len(collection.Partitions))
	for _, partition := range collection.Partitions {
		vchannels, _, err := ex.broker.GetRecoveryInfo(ctx, task.CollectionID(), partition)
		if err != nil {
			log.Warn("failed to get vchannel from DataCoord", zap.Error(err))
			return
		}

		for _, channel := range vchannels {
			if channel.ChannelName == action.ChannelName() {
				channels = append(channels, channel)
			}
		}
	}

	dmChannel := utils.MergeDmChannelInfo(channels)
	req := packSubDmChannelRequest(task, action, collection, dmChannel)
	status, err := ex.cluster.WatchDmChannels(action.ctx, action.Node(), req)
	if err != nil {
		log.Warn("failed to subscribe DmChannel, it may be a false failure", zap.Error(err))
		return
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to subscribe DmChannel", zap.String("reason", status.GetReason()))
		return
	}
}

func (ex *Executor) unsubDmChannel(task *ChannelTask, action *ChannelAction) {
	log := log.With(
		zap.Int64("source-id", task.SourceID()),
		zap.Int64("task-id", task.ID()),
		zap.Int64("collection-id", task.CollectionID()),
		zap.Int64("replica-id", task.ReplicaID()),
		zap.String("channel", task.Channel()),
		zap.Int64("node-id", action.Node()),
	)

	ctx, cancel := context.WithTimeout(task.Context(), actionTimeout)
	defer cancel()

	req := packUnsubDmChannelRequest(task, action)
	status, err := ex.cluster.UnsubDmChannel(ctx, action.Node(), req)
	if err != nil {
		log.Warn("failed to unsubscribe DmChannel, it may be a false failure", zap.Error(err))
		return
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to unsubscribe DmChannel", zap.String("reason", status.GetReason()))
		return
	}
}
