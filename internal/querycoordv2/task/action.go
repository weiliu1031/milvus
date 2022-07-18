package task

import (
	"context"
	"errors"

	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	ErrActionCanceled  = errors.New("ActionCanceled")
	ErrActionRpcFailed = errors.New("ActionRpcFailed")
	ErrActionStale     = errors.New("ActionStale")
)

type Action interface {
	Context() context.Context
	Node() UniqueID
}

type BaseAction struct {
	replicaID UniqueID
	nodeID    UniqueID

	ctx context.Context
}

func NewBaseAction(ctx context.Context, replicaID UniqueID, nodeID UniqueID) *BaseAction {
	return &BaseAction{
		replicaID: replicaID,
		nodeID:    nodeID,

		ctx: ctx,
	}
}

func (action *BaseAction) Context() context.Context {
	return action.ctx
}

func (action *BaseAction) Node() UniqueID {
	return action.nodeID
}

func (action *BaseAction) Execute() error {
	return nil
}

type SegmentAction interface {
	Action
	SegmentID() UniqueID
}

type LoadSegmentAction struct {
	*BaseAction
	segmentID UniqueID
}

func NewLoadSegmentAction() *LoadSegmentAction {
	return &LoadSegmentAction{}
}

func (action *LoadSegmentAction) SegmentID() UniqueID {
	return action.segmentID
}
