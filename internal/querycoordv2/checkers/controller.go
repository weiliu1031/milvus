package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"go.uber.org/zap"
)

var (
	checkRoundTaskNumLimit = 128
)

type CheckerController struct {
	ctx context.Context

	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	nodeMgr   *session.NodeManager

	scheduler *task.Scheduler
	checkers  []Checker
}

func NewCheckerController(ctx context.Context,
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	nodeMgr *session.NodeManager,
	scheduler *task.Scheduler) *CheckerController {

	// CheckerController runs checkers with the order,
	// the former checker has higher priority
	checkers := []Checker{
		NewDmChannelChecker(meta, dist, targetMgr, nodeMgr),
		NewSegmentChecker(meta, dist, targetMgr, nodeMgr),
		NewDeltaChannelChecker(meta, dist, targetMgr, nodeMgr),
	}

	return &CheckerController{
		ctx: ctx,

		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		nodeMgr:   nodeMgr,

		scheduler: scheduler,
		checkers:  checkers,
	}
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

// check is the real implementation of Check
func (controller *CheckerController) check() {
	tasks := make([]task.Task, 0)
	for id, checker := range controller.checkers {
		log := log.With(zap.Int("checker-id", id))
		log.Debug("run checker", zap.String("description", checker.Description()))

		tasks = append(tasks, checker.Check(controller.ctx)...)
		if len(tasks) >= checkRoundTaskNumLimit {
			log.Info("checkers have spawn too many tasks, won't run subsequent checkers, and truncate the spawned tasks",
				zap.Int("task-num", len(tasks)),
				zap.Int("task-num-limit", checkRoundTaskNumLimit))
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
