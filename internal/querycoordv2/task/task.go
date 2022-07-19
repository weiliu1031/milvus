package task

import (
	"context"
	"sync/atomic"

	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type TaskStatus = int32

const (
	TaskStatusStarted TaskStatus = iota + 1
	TaskStatusSucceeded
	TaskStatusFailed
)

type Task interface {
	ActionsAndStep() ([]Action, int)
	StepUp() int

	Context() context.Context
	MsgID() UniqueID
	ID() UniqueID
	ReplicaID() UniqueID
	SetID(id UniqueID)
	Status() TaskStatus
	SetStatus(status TaskStatus)
	Err() error
	SetErr(err error)

	// SetOnSucceeded(func())
	// SetOnFailed(func())
	// SetOnTimeout(func())

	// onSucceeded()
	// onFailed()
	// onTimeout()
}

type BaseTask struct {
	ctx context.Context

	msgID     UniqueID // RequestID
	id        UniqueID // Set by scheduler
	replicaID UniqueID

	status  TaskStatus
	err     error
	actions []Action
	step    int
}

func NewBaseTask(ctx context.Context, msgID, replicaID UniqueID) *BaseTask {
	return &BaseTask{
		msgID:     msgID,
		replicaID: replicaID,

		status: TaskStatusStarted,
		ctx:    ctx,
	}
}

func (task *BaseTask) ActionsAndStep() ([]Action, int) {
	return task.actions, task.step
}

func (task *BaseTask) StepUp() int {
	task.step++
	return task.step
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

func (task *BaseTask) ReplicaID() UniqueID {
	return task.replicaID
}

func (task *BaseTask) Status() TaskStatus {
	return atomic.LoadInt32(&task.status)
}

func (task *BaseTask) SetStatus(status TaskStatus) {
	atomic.StoreInt32(&task.status, status)
}

func (task *BaseTask) Err() error {
	return task.err
}

func (task *BaseTask) SetErr(err error) {
	task.err = err
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
		segmentAction, ok := action.(*SegmentAction)
		if !ok {
			panic("SegmentTask can only contain SegmentActions")
		}

		if segmentID < 0 {
			segmentID = segmentAction.SegmentID()
		} else if segmentID != segmentAction.SegmentID() {
			panic("all actions must process the same segment")
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
