package task

import (
	"context"
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	ErrActionCanceled  = errors.New("ActionCanceled")
	ErrActionRpcFailed = errors.New("ActionRpcFailed")
	ErrActionStale     = errors.New("ActionStale")
)

type ActionType = int32

const (
	ActionTypeGrow ActionType = iota + 1
	ActionTypeReduce
)

type Action interface {
	Node() int64
	IsFinished(distMgr *meta.DistributionManager) bool

	setContext(ctx context.Context)
}

type BaseAction struct {
	nodeID UniqueID
	typ    ActionType

	ctx context.Context // Set by executor
}

func NewBaseAction(nodeID UniqueID, typ ActionType) *BaseAction {
	return &BaseAction{
		nodeID: nodeID,
		typ:    typ,
	}
}

func (action *BaseAction) Node() int64 {
	return action.nodeID
}

func (action *BaseAction) Type() ActionType {
	return action.typ
}

func (action *BaseAction) setContext(ctx context.Context) {
	action.ctx = ctx
}

type SegmentAction struct {
	*BaseAction
	segmentID UniqueID
}

func NewSegmentAction(nodeID UniqueID, typ ActionType, segmentID UniqueID) *SegmentAction {
	return &SegmentAction{
		BaseAction: NewBaseAction(nodeID, typ),

		segmentID: segmentID,
	}
}

func (action *SegmentAction) SegmentID() UniqueID {
	return action.segmentID
}

func (action *SegmentAction) IsFinished(distMgr *meta.DistributionManager) bool {
	segments := distMgr.GetByNode(action.Node())

	hasSegment := false
	for _, segment := range segments {
		if segment.GetID() == action.segmentID {
			hasSegment = true
			break
		}
	}

	isGrow := action.Type() == ActionTypeGrow

	return hasSegment == isGrow
}

type ChannelAction struct {
	*BaseAction
	channelName string
}

func NewChannelAction(nodeID UniqueID, typ ActionType, channelName string) *ChannelAction {
	return &ChannelAction{
		BaseAction: NewBaseAction(nodeID, typ),

		channelName: channelName,
	}
}

func (action *ChannelAction) ChannelName() string {
	return action.channelName
}

func (action *ChannelAction) Execute(cluster *session.Cluster) error {
	var (
		status *commonpb.Status
		err    error
	)

	switch action.Type() {
	case ActionTypeGrow:
		req := &querypb.WatchDmChannelsRequest{}
		status, err = cluster.WatchDmChannels(action.ctx, action.Node(), req)

	case ActionTypeReduce:
		// todo(yah01): Add unsub dm channel?

	default:
		panic(fmt.Sprintf("invalid action type: %+v", action.Type()))
	}

	if err != nil {
		return err
	}

	if status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(status.Reason)
	}

	return nil
}

func (action *ChannelAction) IsFinished(distMgr *meta.DistributionManager) bool {
	channels := distMgr.GetDmChannelByNode(action.nodeID)

	hasChannel := false
	for _, channel := range channels {
		if channel.ChannelName == action.ChannelName() {
			hasChannel = true
			break
		}
	}

	isGrow := action.Type() == ActionTypeGrow

	return hasChannel == isGrow
}
