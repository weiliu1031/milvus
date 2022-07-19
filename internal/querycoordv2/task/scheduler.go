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

const (
	executorWorkerPoolSize = 64
	scheduleDuration       = 200 * time.Millisecond
)

var (
	ErrConflictTaskExisted = errors.New("ConflictTaskExisted")

	// The target node is offline,
	// or the target segment is not in TargetManager
	// or the target channel is not in TargetManager
	ErrTaskStale = errors.New("TaskStale")
)

type Scheduler struct {
	rwmutex     sync.RWMutex
	ctx         context.Context
	executor    *Executor
	idAllocator func() UniqueID

	distMgr   *meta.DistributionManager
	targetMgr *meta.TargetManager
	nodeMgr   *session.NodeManager

	ticker       *time.Ticker
	segmentTasks map[UniqueID]Task
	channelTasks map[string]Task
}

func NewScheduler(ctx context.Context,
	distMgr *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	nodeMgr *session.NodeManager,
	cluster *session.Cluster) *Scheduler {
	id := int64(0)
	return &Scheduler{
		ctx:      ctx,
		executor: NewExecutor(executorWorkerPoolSize, cluster),
		idAllocator: func() UniqueID {
			id++
			return id
		},

		distMgr:   distMgr,
		targetMgr: targetMgr,
		nodeMgr:   nodeMgr,

		ticker:       time.NewTicker(scheduleDuration),
		segmentTasks: make(map[int64]Task),
	}
}

func (scheduler *Scheduler) Add(task Task) error {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	err := scheduler.preAdd(task)
	if err != nil {
		return err
	}

	task.SetID(scheduler.idAllocator())

	switch task.(type) {
	case *SegmentTask:
		scheduler.segmentTasks[task.(*SegmentTask).segmentID] = task

	case *ChannelTask:
		scheduler.channelTasks[task.(*ChannelTask).channel] = task
	}

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

	case *ChannelTask:
		channel := task.(*ChannelTask).channel
		if old, ok := scheduler.channelTasks[channel]; ok {
			log.Warn("failed to add task, a task processing the same channel existed",
				zap.Int64("old-id", old.ID()),
				zap.Int32("old-status", old.Status()))

			return ErrConflictTaskExisted
		}

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

// schedule selects some tasks to execute, follow these steps for each started selected tasks:
// 1. check whether this task is stale, set status to failed if stale
// 2. step up the task's actions, set status to succeeded if all actions finished
// 3. execute the current action of task
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
			// Step up actions
			for {
				actions, step := task.ActionsAndStep()
				if step < len(actions) && actions[step].IsFinished(scheduler.distMgr) {
					task.StepUp()
				} else {
					break
				}
			}

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
	actions, _ := task.ActionsAndStep()
	for step, action := range actions {
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
			segmentID := action.(*SegmentAction).SegmentID()
			if !scheduler.targetMgr.ContainSegment(segmentID) {
				log.Warn("the task is stale, the task's segment not exists",
					zap.Int64("segment-id", segmentID))

				return true
			}

		case *DmChannelAction:
			channel := action.(*DmChannelAction).ChannelName()
			if !scheduler.targetMgr.ContainDmChannel(channel) {
				log.Warn("the task is stale, the task's channel not exists",
					zap.String("channel-name", channel))

				return true
			}
		}
	}

	return false
}
