package checkers

import "github.com/milvus-io/milvus/internal/querycoordv2/task"

type Checker interface {
	Check(nodeID int64) []task.Task
}
