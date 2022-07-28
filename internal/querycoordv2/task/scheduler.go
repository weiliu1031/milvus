package task

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

const (
	taskPoolSize             = 128
	scheduleSegmentTaskLimit = 60
)

var (
	ErrConflictTaskExisted = errors.New("ConflictTaskExisted")

	// The task is canceled or timeout
	ErrTaskCanceled = errors.New("TaskCanceled")

	// The target node is offline,
	// or the target segment is not in TargetManager,
	// or the target channel is not in TargetManager
	ErrTaskStale = errors.New("TaskStale")

	// No enough memory to load segment
	ErrResourceNotEnough = errors.New("ResourceNotEnough")

	ErrTaskQueueFull = errors.New("TaskQueueFull")
)

type replicaSegmentIndex struct {
	ReplicaID int64
	SegmentID int64
}

type replicaChannelIndex struct {
	ReplicaID int64
	Channel   string
}

type taskQueue struct {
	// TaskPriority -> Tasks
	buckets [][]Task

	cap int
}

func newTaskQueue(cap int) *taskQueue {
	return &taskQueue{
		buckets: make([][]Task, len(TaskPriorities)),

		cap: cap,
	}
}

func (queue *taskQueue) Len() int {
	taskNum := 0
	for _, tasks := range queue.buckets {
		taskNum += len(tasks)
	}

	return taskNum
}

func (queue *taskQueue) Cap() int {
	return queue.cap
}

func (queue *taskQueue) Add(task Task) bool {
	if queue.Len() >= queue.Cap() {
		return false
	}

	queue.buckets[task.Priority()] = append(queue.buckets[task.Priority()], task)
	return true
}

func (queue *taskQueue) Remove(task Task) {
	bucket := queue.buckets[task.Priority()]

	for i := range bucket {
		if bucket[i].ID() == task.ID() {
			bucket = append(bucket[:i], bucket[i+1:]...)
			break
		}
	}
}

// Range iterates all tasks in the queue ordered by priority from high to low
func (queue *taskQueue) Range(fn func(task Task) bool) {
	for priority := len(queue.buckets) - 1; priority >= 0; priority-- {
		for i := range queue.buckets[priority] {
			if !fn(queue.buckets[priority][i]) {
				return
			}
		}
	}
}

type Scheduler struct {
	rwmutex     sync.RWMutex
	ctx         context.Context
	executor    *Executor
	idAllocator func() UniqueID

	distMgr   *meta.DistributionManager
	targetMgr *meta.TargetManager
	broker    *meta.CoordinatorBroker
	nodeMgr   *session.NodeManager

	tasks        UniqueSet
	segmentTasks map[replicaSegmentIndex]Task
	channelTasks map[replicaChannelIndex]Task
	processQueue *taskQueue
	waitQueue    *taskQueue
}

func NewScheduler(ctx context.Context,
	meta *meta.Meta,
	distMgr *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	broker *meta.CoordinatorBroker,
	cluster *session.Cluster,
	nodeMgr *session.NodeManager) *Scheduler {
	id := int64(0)
	return &Scheduler{
		ctx:      ctx,
		executor: NewExecutor(meta, distMgr, broker, cluster, nodeMgr),
		idAllocator: func() UniqueID {
			id++
			return id
		},

		distMgr:   distMgr,
		targetMgr: targetMgr,
		broker:    broker,
		nodeMgr:   nodeMgr,

		tasks:        make(UniqueSet),
		segmentTasks: make(map[replicaSegmentIndex]Task),
		channelTasks: make(map[replicaChannelIndex]Task),
		processQueue: newTaskQueue(taskPoolSize),
		waitQueue:    newTaskQueue(taskPoolSize * 10),
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
	scheduler.tasks.Insert(task.ID())

	switch task := task.(type) {
	case *SegmentTask:
		index := replicaSegmentIndex{task.ReplicaID(), task.segmentID}
		scheduler.segmentTasks[index] = task

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.channel}
		scheduler.channelTasks[index] = task
	}

	return nil
}

// check checks whether the task is valid to add,
// must hold lock
func (scheduler *Scheduler) preAdd(task Task) error {
	switch task := task.(type) {
	case *SegmentTask:
		index := replicaSegmentIndex{task.ReplicaID(), task.segmentID}
		if old, ok := scheduler.segmentTasks[index]; ok {
			log.Warn("failed to add task, a task processing the same segment existed",
				zap.Int64("old-id", old.ID()),
				zap.Int32("old-status", old.Status()))

			return ErrConflictTaskExisted
		}

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.channel}
		if old, ok := scheduler.channelTasks[index]; ok {
			log.Warn("failed to add task, a task processing the same channel existed",
				zap.Int64("old-id", old.ID()),
				zap.Int32("old-status", old.Status()))

			return ErrConflictTaskExisted
		}

	default:
		panic(fmt.Sprintf("preAdd: forget to process task type: %+v", task))
	}

	return nil
}

func (scheduler *Scheduler) promote(task Task) error {
	err := scheduler.prePromote(task)
	if err != nil {
		return err
	}

	if scheduler.processQueue.Add(task) {
		task.SetStatus(TaskStatusStarted)
		return nil
	}

	return ErrTaskQueueFull
}

func (scheduler *Scheduler) prePromote(task Task) error {
	if !scheduler.tasks.Contain(task.ID()) || scheduler.checkTimeout(task) {
		return ErrTaskCanceled
	} else if scheduler.checkStale(task) {
		return ErrTaskStale
	}

	return nil
}

func (scheduler *Scheduler) Dispatch(node int64) {
	for {
		select {
		case <-scheduler.ctx.Done():

		default:
			scheduler.schedule(node)
		}
	}
}

// schedule selects some tasks to execute, follow these steps for each started selected tasks:
// 1. check whether this task is stale, set status to failed if stale
// 2. step up the task's actions, set status to succeeded if all actions finished
// 3. execute the current action of task
func (scheduler *Scheduler) schedule(node int64) {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	// Process tasks
	toRemove := make([]Task, 0)
	scheduler.processQueue.Range(func(task Task) bool {
		scheduler.process(task)

		if task.Status() != TaskStatusStarted {
			toRemove = append(toRemove, task)
		}

		return true
	})

	for _, task := range toRemove {
		scheduler.remove(task)
	}

	// Promote waiting tasks
	toRemove = toRemove[:0]
	scheduler.waitQueue.Range(func(task Task) bool {
		err := scheduler.promote(task)
		if err != nil && !errors.Is(err, ErrTaskQueueFull) {
			toRemove = append(toRemove, task)
		}

		return err == nil
	})

	for _, task := range toRemove {
		scheduler.remove(task)
	}
}

// process processes the given task,
// return true if the task is started and succeeds to commit the current action
func (scheduler *Scheduler) process(task Task) bool {
	log := log.With(
		zap.Int64("msg-id", task.MsgID()),
		zap.Int64("task-id", task.ID()))

	if scheduler.checkTimeout(task) {
		task.SetStatus(TaskStatusCanceled)
		task.SetErr(ErrTaskCanceled)
	}

	isFinished := task.IsFinished(scheduler.distMgr)
	if isFinished {
		task.SetStatus(TaskStatusSucceeded)
	}

	isStale := scheduler.checkStale(task)
	if isStale {
		task.SetStatus(TaskStatusStale)
		task.SetErr(ErrTaskStale)
	}

	actions, step := task.Actions(), task.Step()
	log = log.With(zap.Int("step", step))
	switch task.Status() {
	case TaskStatusStarted:
		log.Debug("execute task")
		if scheduler.executor.Execute(task, step, actions[step]) {
			return true
		}

	case TaskStatusSucceeded:
		log.Debug("task succeeded")

	case TaskStatusCanceled, TaskStatusStale:
		log.Warn("failed to execute task", zap.Error(task.Err()))

	default:
		panic(fmt.Sprintf("invalid task status: %v", task.Status()))
	}

	return false
}

func (scheduler *Scheduler) remove(task Task) {
	task.Cancel()
	scheduler.tasks.Remove(task.ID())
	scheduler.waitQueue.Remove(task)
	scheduler.processQueue.Remove(task)

	// Call callbacks
	onSuccess, onFailure := task.callbacks()
	if task.Status() == TaskStatusSucceeded {
		for _, fn := range onSuccess {
			fn()
		}
	} else {
		for _, fn := range onFailure {
			fn()
		}
	}

	switch task := task.(type) {
	case *SegmentTask:
		index := replicaSegmentIndex{task.ReplicaID(), task.segmentID}
		delete(scheduler.segmentTasks, index)

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.channel}
		delete(scheduler.channelTasks, index)
	}

}

func (scheduler *Scheduler) checkTimeout(task Task) bool {
	log := log.With(
		zap.Int64("msg-id", task.MsgID()),
		zap.Int64("task-id", task.ID()))

	select {
	case <-task.Context().Done():
		log.Warn("the task is timeout or canceled")
		return true

	default:
		return false
	}
}

func (scheduler *Scheduler) checkStale(task Task) bool {
	log := log.With(
		zap.Int64("msg-id", task.MsgID()),
		zap.Int64("task-id", task.ID()))

	switch task := task.(type) {
	case *SegmentTask:
		if !scheduler.targetMgr.ContainSegment(task.segmentID) {
			log.Warn("the task is stale, the task's segment not exists",
				zap.Int64("segment-id", task.segmentID))

			return true
		}

	case *ChannelTask:
		if !scheduler.targetMgr.ContainDmChannel(task.channel) {
			log.Warn("the task is stale, the task's channel not exists",
				zap.String("channel-name", task.channel))

			return true
		}

	default:
		panic(fmt.Sprintf("checkStatle: forget to check task type: %+v", task))
	}

	actions := task.Actions()
	for step, action := range actions {
		log := log.With(
			zap.Int64("node-id", action.Node()),
			zap.Int("step", step))

		if scheduler.nodeMgr.Get(action.Node()) == nil {
			log.Warn("the task is stale, the target node is offline")

			return true
		}
	}

	return false
}

// checkStale 检查 ReleaseSegment 合法性
// action 增加 preallocate 逻辑
