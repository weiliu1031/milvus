package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"go.uber.org/zap"
)

var (
	checkRoundTaskNumLimit = 128
)

type CheckerController struct {
	ctx       context.Context
	checkers  []Checker
	scheduler *task.Scheduler
}

// Check run checkers to spawn tasks,
// and adds them into scheduler.
func (controller *CheckerController) Check() {
	select {
	case <-controller.ctx.Done():

	default:
		controller.check()
	}
}

func (controller *CheckerController) check() {
	tasks := make([]task.Task, 0)
	for i, checker := range controller.checkers {
		log := log.With(zap.Int("rank", i))
		log.Debug("run checker", zap.String("description", checker.Description()))

		tasks = append(tasks, checker.Check(controller.ctx)...)
		if len(tasks) >= checkRoundTaskNumLimit {
			log.Info("checkers have spawn too many tasks, won't run subsequent checkers, and truncate the spawned tasks",
				zap.Int("task-num", len(tasks)))
			tasks = tasks[:checkRoundTaskNumLimit]
			break
		}
	}

	for _, task := range tasks {
		err := controller.scheduler.Add(task)
		if err != nil {
			log.Warn("failed to add task into scheduler",
				zap.Int64("collection-id", task.CollectionID()),
				zap.Int64("replica-id", task.ReplicaID()),
				zap.Error(err))
		}
	}
}
