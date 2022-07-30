package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type Checker interface {
	Description() string
	Check(ctx context.Context) []task.Task
}
