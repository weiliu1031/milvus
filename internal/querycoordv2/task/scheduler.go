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
	taskPoolSize = 128
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
		executor: NewExecutor(meta, distMgr, broker, targetMgr, cluster, nodeMgr),
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
	log := log.With(
		zap.Int64("source-id", task.SourceID()),
		zap.Int64("collection-id", task.CollectionID()),
		zap.Int64("replica-id", task.ReplicaID()),
		zap.Int32("priority", task.Priority()),
	)
	switch task := task.(type) {
	case *SegmentTask:
		index := replicaSegmentIndex{task.ReplicaID(), task.segmentID}
		scheduler.segmentTasks[index] = task
		log.Info("task added", zap.Int64("segment-id", task.SegmentID()))

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.channel}
		scheduler.channelTasks[index] = task
		log.Info("task added", zap.String("channel", task.Channel()))
	}
	scheduler.waitQueue.Add(task)

	return nil
}

// check checks whether the task is valid to add,
// must hold lock
func (scheduler *Scheduler) preAdd(task Task) error {
	if scheduler.waitQueue.Len() >= scheduler.waitQueue.Cap() {
		return ErrTaskQueueFull
	}

	switch task := task.(type) {
	case *SegmentTask:
		index := replicaSegmentIndex{task.ReplicaID(), task.segmentID}
		if old, ok := scheduler.segmentTasks[index]; ok {
			if task.Priority() > old.Priority() {
				log.Info("replace old task, the new one with higher priority",
					zap.Int64("old-id", old.ID()),
					zap.Int32("old-priority", old.Priority()),
					zap.Int64("new-id", task.ID()),
					zap.Int32("new-priority", task.Priority()),
				)
				return nil
			}
			log.Warn("failed to add task, a task processing the same segment existed",
				zap.Int64("old-id", old.ID()),
				zap.Int32("old-status", old.Status()))

			return ErrConflictTaskExisted
		}

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.channel}
		if old, ok := scheduler.channelTasks[index]; ok {
			if task.Priority() > old.Priority() {
				log.Info("replace old task, the new one with higher priority",
					zap.Int64("old-id", old.ID()),
					zap.Int32("old-priority", old.Priority()),
					zap.Int64("new-id", task.ID()),
					zap.Int32("new-priority", task.Priority()),
				)
				return nil
			}
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
	log := log.With(
		zap.Int64("source-id", task.SourceID()),
		zap.Int64("collection-id", task.CollectionID()),
		zap.Int64("replica-id", task.ReplicaID()),
		zap.Int64("task-id", task.ID()),
	)
	err := scheduler.prePromote(task)
	if err != nil {
		log.Info("failed to promote task", zap.Error(err))
		return err
	}

	if scheduler.processQueue.Add(task) {
		task.SetStatus(TaskStatusStarted)
		return nil
	}

	log.Info("failed to promote task, will retry later", zap.Error(ErrTaskQueueFull))
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

func (scheduler *Scheduler) GetNodeSegmentDelta(nodeID int64) int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	return calculateNodeDelta(nodeID, scheduler.segmentTasks)
}

func (scheduler *Scheduler) GetNodeChannelDelta(nodeID int64) int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	return calculateNodeDelta(nodeID, scheduler.channelTasks)
}

func calculateNodeDelta[K comparable, T ~map[K]Task](nodeID int64, tasks T) int {
	delta := 0
	for _, task := range tasks {
		for _, action := range task.Actions() {
			if action.Node() != nodeID {
				continue
			}
			if action.Type() == ActionTypeGrow {
				delta++
			} else if action.Type() == ActionTypeReduce {
				delta--
			}
		}
	}
	return delta
}

func (scheduler *Scheduler) GetNodeSegmentCntDelta(nodeID int64) int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	delta := 0
	for _, task := range scheduler.segmentTasks {
		for _, action := range task.Actions() {
			if action.Node() != nodeID {
				continue
			}
			segmentAction := action.(*SegmentAction)
			segment := scheduler.targetMgr.GetSegment(segmentAction.SegmentID())
			if action.Type() == ActionTypeGrow {
				delta += int(segment.GetNumOfRows())
			} else {
				delta -= int(segment.GetNumOfRows())
			}
		}
	}
	return delta
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
		if task.IsRelatedTo(node) {
			scheduler.process(task)
		}

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
		zap.Int64("source-id", task.SourceID()),
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
		log.Info("execute task")
		if scheduler.executor.Execute(task, step, actions[step]) {
			return true
		}

	case TaskStatusSucceeded:
		log.Info("task succeeded")

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
		zap.Int64("source-id", task.SourceID()),
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
		zap.Int64("source-id", task.SourceID()),
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
			log.Warn("the task is stale, target node is offline")

			return true
		}
	}

	return false
}

// checkStale 检查 ReleaseSegment 合法性
// action 增加 preallocate 逻辑
