package task

import (
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/concurrency"
)

type Executor struct {
	cluster *session.Cluster
	works   *concurrency.Pool
}

func NewExecutor(workNum int) *Executor {
	works, _ := concurrency.NewPool(workNum)

	return &Executor{
		works: works,
	}
}

func (ex *Executor) Execute(action Action) {
	ex.works.Submit(func() (interface{}, error) {
		switch action.(type) {
		case *LoadSegmentAction:
			action := action.(*LoadSegmentAction)

			req := &querypb.LoadSegmentsRequest{}
			ex.cluster.LoadSegments(action.ctx, action.Node(), req)
		}

		return nil, nil
	})
}
