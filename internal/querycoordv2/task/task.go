package task

import (
	"context"
	"sync/atomic"

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
	MsgID() UniqueID
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
}

type BaseTask struct {
	ctx    context.Context
	cancel context.CancelFunc

	msgID        UniqueID // RequestID
	id           UniqueID // Set by scheduler
	collectionID UniqueID
	replicaID    UniqueID

	status   TaskStatus
	priority TaskPriority
	err      error
	actions  []Action
	step     int
}

func NewBaseTask(ctx context.Context, msgID, replicaID UniqueID) *BaseTask {
	ctx, cancel := context.WithCancel(ctx)

	return &BaseTask{
		msgID:     msgID,
		replicaID: replicaID,

		status:   TaskStatusStarted,
		priority: TaskPriorityNormal,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (task *BaseTask) Context() context.Context {
	return task.ctx
}

func (task *BaseTask) MsgID() UniqueID {
	return task.msgID
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
		task.step++
	}

	return task.Step() >= len(actions)
}

type SegmentTask struct {
	*BaseTask

	segmentID UniqueID
	loadType  querypb.LoadType
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
		_, ok := action.(*SegmentAction)
		if !ok {
			panic("SegmentTask can only contain SegmentActions")
		}
	}

	base.actions = actions
	return &SegmentTask{
		BaseTask: base,

		segmentID: segmentID,
	}
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

	_, isDmChannel := actions[0].(*DmChannelAction)
	var (
		channel string
		ok      bool
	)
	for _, action := range actions {
		if isDmChannel {
			action, ok = action.(*DmChannelAction)
		} else {
			action, ok = action.(*DeltaChannelAction)
		}

		if !ok {
			panic("ChannelTask can only contain actions of the same type channels")
		}

		channelAction := action.(interface{ ChannelName() string })

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
