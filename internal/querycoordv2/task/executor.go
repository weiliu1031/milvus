package task

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"go.uber.org/zap"
)

type actionIndex struct {
	TaskID int64
	Step   int
}

type Executor struct {
	cluster *session.Cluster
	broker  *meta.CoordinatorBroker
	meta    *meta.Meta

	executingActions sync.Map
}

func NewExecutor(cluster *session.Cluster, broker *meta.CoordinatorBroker) *Executor {
	return &Executor{
		cluster: cluster,
		broker:  broker,

		executingActions: sync.Map{},
	}
}

// Execute executes the given action,
// does nothing and returns false if the action is already committed,
// returns true otherwise.
func (ex *Executor) Execute(task Task, step int, action Action) bool {
	log := log.With(
		zap.Int64("msg-id", task.MsgID()),
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

		case *DmChannelAction:
			ex.executeDmChannelAction(action)

		case *DeltaChannelAction:
			ex.executeDeltaChannelAction(action)

		default:
			panic(fmt.Sprintf("forget to process action type: %+v", action))
		}

		ex.executingActions.Delete(index)
	}()

	return true
}

func (ex *Executor) executeSegmentAction(task *SegmentTask, action *SegmentAction) {
	log := log.With(
		zap.Int64("msg-id", task.MsgID()),
		zap.Int64("task-id", task.ID()),
		zap.Int64("collection-id", task.collectionID),
		zap.Int64("replica-id", task.ReplicaID()),
		zap.Int64("segment-id", task.segmentID),
	)

	switch action.Type() {
	case ActionTypeGrow:
		collection := ex.meta.CollectionManager.Get(task.CollectionID())
		if collection == nil {
			log.Warn("failed to get collection")
			return
		}

		segment, err := ex.broker.GetSegmentInfo(task.ctx, task.segmentID)
		if err != nil {
			log.Warn("failed to get segment info from DataCoord", zap.Error(err))
			return
		}

		loadInfo := &querypb.SegmentLoadInfo{
			SegmentID:     segment.ID,
			PartitionID:   segment.PartitionID,
			CollectionID:  segment.CollectionID,
			BinlogPaths:   segment.Binlogs,
			NumOfRows:     segment.NumOfRows,
			Statslogs:     segment.Statslogs,
			Deltalogs:     segment.Deltalogs,
			InsertChannel: segment.InsertChannel,
		}
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadSegments,
				MsgID:   task.MsgID(),
			},
			Infos:  []*querypb.SegmentLoadInfo{loadInfo},
			Schema: collection.Schema,
			LoadMeta: &querypb.LoadMetaInfo{
				LoadType:     task.loadType,
				CollectionID: task.CollectionID(),
				PartitionIDs: []int64{segment.PartitionID},
			},
			ReplicaID: task.ReplicaID(),
		}
		status, err := ex.cluster.LoadSegments(action.ctx, action.Node(), req)
		if err != nil {
			log.Warn("failed to load segment, it may be a false failure", zap.Error(err))
			return
		}
		if status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("failed to load segment", zap.String("reason", status.Reason))
			return
		}

	case ActionTypeReduce:
		req := &querypb.ReleaseSegmentsRequest{}
		ex.cluster.ReleaseSegments(action.ctx, action.Node(), req)

	default:
		panic(fmt.Sprintf("invalid action type: %+v", action.Type()))
	}
}

func (ex *Executor) executeDmChannelAction(action *DmChannelAction) {
	switch action.Type() {
	case ActionTypeGrow:
		req := &querypb.LoadSegmentsRequest{}
		ex.cluster.LoadSegments(action.ctx, action.Node(), req)

	case ActionTypeReduce:
		req := &querypb.ReleaseSegmentsRequest{}
		ex.cluster.ReleaseSegments(action.ctx, action.Node(), req)

	default:
		panic(fmt.Sprintf("invalid action type: %+v", action.Type()))
	}
}

func (ex *Executor) executeDeltaChannelAction(action *DeltaChannelAction) {
	switch action.Type() {
	case ActionTypeGrow:
		req := &querypb.LoadSegmentsRequest{}
		ex.cluster.LoadSegments(action.ctx, action.Node(), req)

	case ActionTypeReduce:
		req := &querypb.ReleaseSegmentsRequest{}
		ex.cluster.ReleaseSegments(action.ctx, action.Node(), req)

	default:
		panic(fmt.Sprintf("invalid action type: %+v", action.Type()))
	}
}
