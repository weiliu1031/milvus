package task

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/concurrency"
)

type actionIndex struct {
	TaskID int64
	Step   int
}

type Executor struct {
	cluster *session.Cluster
	broker  *meta.CoordinatorBroker

	pool             *concurrency.Pool
	executingActions sync.Map
}

func NewExecutor(workNum int, cluster *session.Cluster, broker *meta.CoordinatorBroker) *Executor {
	works, _ := concurrency.NewPool(workNum)

	return &Executor{
		cluster: cluster,
		broker:  broker,

		pool:             works,
		executingActions: sync.Map{},
	}
}

// Execute executes the given action,
// does nothing and returns false if the action is already committed,
// returns true otherwise.
func (ex *Executor) Execute(task Task, step int, action Action) bool {
	index := actionIndex{
		TaskID: task.ID(),
		Step:   step,
	}
	_, exist := ex.executingActions.LoadOrStore(index, struct{}{})
	if exist {
		return false
	}

	ex.pool.Submit(func() (interface{}, error) {
		switch action := action.(type) {
		case *SegmentAction:
			ex.executeSegmentAction(task, action)

		case *DmChannelAction:
			ex.executeDmChannelAction(action)

		case *DeltaChannelAction:
			ex.executeDeltaChannelAction(action)

		default:
			panic(fmt.Sprintf("forget to process action type: %+v", action))
		}

		return nil, err
	})

	return true
}

func (ex *Executor) executeSegmentAction(task Task, action *SegmentAction) {
	switch action.Type {
	case ActionTypeGrow:
		_, segmentBinlogs, err := ex.broker.GetRecoveryInfo(task.Context(),)

		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadSegments,
				MsgID:   task.MsgID(),
			},
		}
		ex.cluster.LoadSegments(action.ctx, action.NodeID, req)

	case ActionTypeReduce:
		req := &querypb.ReleaseSegmentsRequest{}
		ex.cluster.ReleaseSegments(action.ctx, action.NodeID, req)

	default:
		panic(fmt.Sprintf("invalid action type: %+v", action.Type()))
	}
}

func (ex *Executor) executeDmChannelAction(action *DmChannelAction) {
	switch action.Type() {
	case ActionTypeGrow:
		req := &querypb.LoadSegmentsRequest{}
		ex.cluster.LoadSegments(action.Context(), action.Node(), req)

	case ActionTypeReduce:
		req := &querypb.ReleaseSegmentsRequest{}
		ex.cluster.ReleaseSegments(action.Context(), action.Node(), req)

	default:
		panic(fmt.Sprintf("invalid action type: %+v", action.Type()))
	}
}

func (ex *Executor) executeDeltaChannelAction(action *DeltaChannelAction) {
	switch action.Type() {
	case ActionTypeGrow:
		req := &querypb.LoadSegmentsRequest{}
		ex.cluster.LoadSegments(action.Context(), action.Node(), req)

	case ActionTypeReduce:
		req := &querypb.ReleaseSegmentsRequest{}
		ex.cluster.ReleaseSegments(action.Context(), action.Node(), req)

	default:
		panic(fmt.Sprintf("invalid action type: %+v", action.Type()))
	}
}
