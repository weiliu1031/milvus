package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

var (
	ErrConflictTaskExisted = errors.New("ConflictTaskExisted")
	ErrTaskStale           = errors.New("TaskStale")
)

type Scheduler struct {
	rwmutex     sync.RWMutex
	ctx         context.Context
	executor    *Executor
	idAllocator func() UniqueID

	// Meta
	segmentMgr *meta.SegmentManager

	// Session
	nodeMgr *session.NodeManager

	ticker       *time.Ticker
	segmentTasks map[UniqueID]Task
}

func (scheduler *Scheduler) Add(task Task) error {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	err := scheduler.preAdd(task)
	if err != nil {
		return err
	}

	task.SetID(scheduler.idAllocator())
	return nil
}

// check checks whether the task is valid to add,
// must hold lock
func (scheduler *Scheduler) preAdd(task Task) error {
	switch task.(type) {
	case *SegmentTask:
		segmentID := task.(*SegmentTask).segmentID
		if old, ok := scheduler.segmentTasks[segmentID]; ok {
			log.Warn("failed to add task, a task processing the same segment existed",
				zap.Int64("old-id", old.ID()),
				zap.Int32("old-status", old.Status()))

			return ErrConflictTaskExisted
		}

		// todo(yah01): check whether the segment exists

	default:
		panic(fmt.Sprintf("forget to process task type: %+v", task))
	}

	return nil
}

func (scheduler *Scheduler) Schedule() {
	for {
		select {
		case <-scheduler.ctx.Done():

		case <-scheduler.ticker.C:
			scheduler.schedule()
		}
	}
}

// schedule selects some tasks to execute,
// PreExecute() for checks,
// Execute() for processing
// PostExecute() for callback
func (scheduler *Scheduler) schedule() {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	for segmentID, task := range scheduler.segmentTasks {
		log := log.With(
			zap.Int64("msg-id", task.MsgID()),
			zap.Int64("task-id", task.ID()))

		isStale := scheduler.checkStale(task)
		if isStale {
			task.SetStatus(TaskStatusFailed)
			task.SetErr(ErrTaskStale)
		}

		switch task.Status() {
		case TaskStatusStarted:
			actions, step := task.ActionsAndStep()
			if step >= len(actions) {
				task.SetStatus(TaskStatusSucceeded)
			} else {
				scheduler.executor.Execute(actions[step])
			}

		case TaskStatusSucceeded:
			log.Debug("task succeeded")
			delete(scheduler.segmentTasks, segmentID)

		case TaskStatusFailed:
			log.Warn("failed to execute task",
				zap.Error(task.Err()))
			delete(scheduler.segmentTasks, segmentID)

		default:
			panic(fmt.Sprintf("invalid task status: %v", task.Status()))
		}
	}
}

func (scheduler *Scheduler) checkStale(task Task) bool {
	actions, step := task.ActionsAndStep()
	for step, action := range actions[step:] {
		log := log.With(
			zap.Int64("msg-id", task.MsgID()),
			zap.Int64("task-id", task.ID()),
			zap.Int64("node-id", action.Node()),
			zap.Int("step", step))

		if scheduler.nodeMgr.Get(action.Node()) == nil {
			log.Warn("the task is stale, the target node is offline")

			return true
		}

		switch action.(type) {
		case *SegmentAction:
			// segmentID := action.(*SegmentAction).SegmentID()
			// if !scheduler.segmentMgr.ContainTarget(segmentID) {
			// 	log.Warn("the task is stale, the target segment doesn't exist",
			// 		zap.Int64("segment-id", segmentID))

			// 	return true
			// }
		}
	}

	return false
}
