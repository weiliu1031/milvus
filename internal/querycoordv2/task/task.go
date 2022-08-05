package task

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type TaskStatus = int32
type TaskPriority = int32

const (
	TaskStatusCreated TaskStatus = iota + 1
	TaskStatusStarted
	TaskStatusSucceeded
	TaskStatusCanceled
	TaskStatusStale
)

const (
	TaskPriorityLow = iota
	TaskPriorityNormal
	TaskPriorityHigh
)

var (
	// All task priorities from low to high
	TaskPriorities = []TaskPriority{TaskPriorityLow, TaskPriorityNormal, TaskPriorityHigh}
)

type Task interface {
	Context() context.Context
	SourceID() UniqueID
	ID() UniqueID
	CollectionID() UniqueID
	ReplicaID() UniqueID
	SetID(id UniqueID)
	Status() TaskStatus
	SetStatus(status TaskStatus)
	Err() error
	SetErr(err error)
	Priority() TaskPriority
	SetPriority(priority TaskPriority)

	Cancel()
	Actions() []Action
	Step() int
	IsFinished(dist *meta.DistributionManager) bool
	IsRelatedTo(node UniqueID) bool

	// OnSuccess registers a callback for task succeeded
	OnSuccess(func())
	// OnFailure registers a callback for task failed (canceled, stale)
	OnFailure(func())
	// OnDone is equal to call both of OnSuccess and OnFailure
	OnDone(func())
	// callbacks() returns registered callbacks of OnSuccess and OnFailure
	callbacks() ([]func(), []func())
}

type BaseTask struct {
	ctx    context.Context
	cancel context.CancelFunc

	sourceID     UniqueID // RequestID
	id           UniqueID // Set by scheduler
	collectionID UniqueID
	replicaID    UniqueID
	loadType     querypb.LoadType

	status   TaskStatus
	priority TaskPriority
	err      error
	actions  []Action
	step     int

	successCallbacks []func()
	failureCallbacks []func()
}

func NewBaseTask(ctx context.Context, timeout time.Duration, sourceID, collectionID, replicaID UniqueID) *BaseTask {
	ctx, cancel := context.WithTimeout(ctx, timeout)

	return &BaseTask{
		sourceID:     sourceID,
		collectionID: collectionID,
		replicaID:    replicaID,

		status:   TaskStatusStarted,
		priority: TaskPriorityNormal,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (task *BaseTask) Context() context.Context {
	return task.ctx
}

func (task *BaseTask) SourceID() UniqueID {
	return task.sourceID
}

func (task *BaseTask) ID() UniqueID {
	return task.id
}

func (task *BaseTask) SetID(id UniqueID) {
	task.id = id
}

func (task *BaseTask) CollectionID() UniqueID {
	return task.collectionID
}

func (task *BaseTask) ReplicaID() UniqueID {
	return task.replicaID
}

func (task *BaseTask) LoadType() querypb.LoadType {
	return task.loadType
}

func (task *BaseTask) Status() TaskStatus {
	return atomic.LoadInt32(&task.status)
}

func (task *BaseTask) SetStatus(status TaskStatus) {
	atomic.StoreInt32(&task.status, status)
}

func (task *BaseTask) Priority() TaskPriority {
	return task.priority
}

func (task *BaseTask) SetPriority(priority TaskPriority) {
	task.priority = priority
}

func (task *BaseTask) Err() error {
	return task.err
}

func (task *BaseTask) SetErr(err error) {
	task.err = err
}

func (task *BaseTask) Cancel() {
	task.cancel()

	for step := task.Step(); step < len(task.actions); step++ {
		task.actions[step].Done()
	}
}

func (task *BaseTask) Actions() []Action {
	return task.actions
}

func (task *BaseTask) Step() int {
	return task.step
}

func (task *BaseTask) IsFinished(distMgr *meta.DistributionManager) bool {
	if task.Status() != TaskStatusStarted {
		return false
	}

	actions, step := task.Actions(), task.Step()
	for step < len(actions) && actions[step].IsFinished(distMgr) {
		actions[step].Done()
		task.step++
	}

	return task.Step() >= len(actions)
}

func (task *BaseTask) IsRelatedTo(node UniqueID) bool {
	for _, action := range task.actions {
		if action.Node() == node {
			return true
		}
	}

	return false
}

func (task *BaseTask) PreExecute() error {
	return nil
}

func (task *BaseTask) OnSuccess(fn func()) {
	task.successCallbacks = append(task.successCallbacks, fn)
}

func (task *BaseTask) OnFailure(fn func()) {
	task.failureCallbacks = append(task.failureCallbacks, fn)
}

func (task *BaseTask) OnDone(fn func()) {
	task.OnSuccess(fn)
	task.OnFailure(fn)
}

func (task *BaseTask) callbacks() ([]func(), []func()) {
	return task.successCallbacks, task.failureCallbacks
}

type SegmentTask struct {
	*BaseTask

	segmentID UniqueID
}

// NewSegmentTask creates a SegmentTask with actions,
// all actions must process the same segment,
// empty actions is not allowed
func NewSegmentTask(base *BaseTask, actions ...Action) *SegmentTask {
	if len(actions) == 0 {
		panic("empty actions is not allowed")
	}

	segmentID := int64(-1)
	for _, action := range actions {
		action, ok := action.(*SegmentAction)
		if !ok {
			panic("SegmentTask can only contain SegmentActions")
		}
		if segmentID == -1 {
			segmentID = action.segmentID
		} else if segmentID != action.SegmentID() {
			panic("all actions must process the same segment")
		}
	}

	base.actions = actions
	return &SegmentTask{
		BaseTask: base,

		segmentID: segmentID,
	}
}

func (task *SegmentTask) SegmentID() UniqueID {
	return task.segmentID
}

type ChannelTask struct {
	*BaseTask

	channel string
}

// NewChannelTask creates a ChannelTask with actions,
// all actions must process the same channel, and the same type of channel
// empty actions is not allowed
func NewChannelTask(base *BaseTask, actions ...Action) *ChannelTask {
	if len(actions) == 0 {
		panic("empty actions is not allowed")
	}

	channel := ""
	for _, action := range actions {
		channelAction, ok := action.(interface{ ChannelName() string })
		if !ok {
			panic("ChannelTask must contain only ChannelAction")
		}
		if channel == "" {
			channel = channelAction.ChannelName()
		} else if channel != channelAction.ChannelName() {
			panic("all actions must process the same channel")
		}
	}

	base.actions = actions
	return &ChannelTask{
		BaseTask: base,

		channel: channel,
	}
}

func (task *ChannelTask) Channel() string {
	return task.channel
}

type TaskInfluence interface {
	Task() Task
	Influence() int
}

type BaseTaskInfluence struct {
	task Task
}

func (influence *BaseTaskInfluence) Task() {

}