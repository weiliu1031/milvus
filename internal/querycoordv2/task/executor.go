package task

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/concurrency"
)

type Executor struct {
	cluster *session.Cluster
	works   *concurrency.Pool
}

func NewExecutor(workNum int, cluster *session.Cluster) *Executor {
	works, _ := concurrency.NewPool(workNum)

	return &Executor{
		cluster: cluster,
		works:   works,
	}
}

func (ex *Executor) Execute(action Action) {
	ex.works.Submit(func() (interface{}, error) {
		err := action.Execute(ex.cluster)

		return nil, err
	})
}

func (ex *Executor) executeSegmentAction(action *SegmentAction) {
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
