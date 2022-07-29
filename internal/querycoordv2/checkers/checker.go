package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type Checker interface {
	Check(ctx context.Context) []task.Task
	Description() string
}
