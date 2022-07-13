package checkers

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"go.uber.org/zap"
)

var (
	checkRoundTaskNumLimit = 128
	checkPeriod            = 200 * time.Millisecond
)

type CheckerController struct {
	stopCh    chan struct{}
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	broker    *meta.CoordinatorBroker
	nodeMgr   *session.NodeManager

	scheduler *task.Scheduler
	checkers  []Checker
}

func NewCheckerController(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	broker *meta.CoordinatorBroker,
	nodeMgr *session.NodeManager,
	scheduler *task.Scheduler) *CheckerController {

	// CheckerController runs checkers with the order,
	// the former checker has higher priority
	checkers := []Checker{
		NewChannelChecker(meta, dist, targetMgr, nodeMgr),
		NewSegmentChecker(meta, dist, targetMgr, broker, nodeMgr),
	}
	for i, checker := range checkers {
		checker.SetID(int64(i + 1))
	}

	return &CheckerController{
		stopCh:    make(chan struct{}),
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		nodeMgr:   nodeMgr,
		scheduler: scheduler,
		checkers:  checkers,
	}
}

func (controller *CheckerController) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(checkPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Info("CheckerController stopped due to context canceled")
				return

			case <-controller.stopCh:
				log.Info("CheckerController stopped")
				return

			case <-ticker.C:
				controller.check(ctx)
			}
		}
	}()
}

func (controller *CheckerController) Stop() {
	close(controller.stopCh)
}

// check is the real implementation of Check
func (controller *CheckerController) check(ctx context.Context) {
	tasks := make([]task.Task, 0)
	for id, checker := range controller.checkers {
		log := log.With(zap.Int("checker-id", id))
		log.Debug("run checker", zap.String("description", checker.Description()))

		tasks = append(tasks, checker.Check(ctx)...)
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
