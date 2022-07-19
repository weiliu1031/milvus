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
	Context() context.Context
	Node() UniqueID
	Type() ActionType
	IsFinished(distMgr *meta.DistributionManager) bool
	Execute(cluster *session.Cluster) error
}

type BaseAction struct {
	replicaID UniqueID
	nodeID    UniqueID
	typ       ActionType

	ctx context.Context
}

func NewBaseAction(ctx context.Context, nodeID UniqueID, typ ActionType) *BaseAction {
	return &BaseAction{
		nodeID: nodeID,
		typ:    typ,

		ctx: ctx,
	}
}

func (action *BaseAction) Context() context.Context {
	return action.ctx
}

func (action *BaseAction) Node() UniqueID {
	return action.nodeID
}

func (action *BaseAction) Type() ActionType {
	return action.typ
}

type SegmentAction struct {
	*BaseAction
	segmentID UniqueID
}

func NewSegmentAction(ctx context.Context, nodeID UniqueID, typ ActionType, segmentID UniqueID) *SegmentAction {
	return &SegmentAction{
		BaseAction: NewBaseAction(ctx, nodeID, typ),

		segmentID: segmentID,
	}
}

func (action *SegmentAction) SegmentID() UniqueID {
	return action.segmentID
}

func (action *SegmentAction) Execute(cluster *session.Cluster) error {
	var (
		status *commonpb.Status
		err    error
	)

	switch action.Type() {
	case ActionTypeGrow:
		req := &querypb.LoadSegmentsRequest{}
		status, err = cluster.LoadSegments(action.Context(), action.Node(), req)

	case ActionTypeReduce:
		req := &querypb.ReleaseSegmentsRequest{}
		status, err = cluster.ReleaseSegments(action.Context(), action.Node(), req)

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

func (action *SegmentAction) IsFinished(distMgr *meta.DistributionManager) bool {
	segments := distMgr.GetByNode(action.nodeID)

	hasSegment := false
	for _, segment := range segments {
		if segment.GetID() == action.SegmentID() {
			hasSegment = true
			break
		}
	}

	isGrow := action.Type() == ActionTypeGrow

	return hasSegment == isGrow
}

type DmChannelAction struct {
	*BaseAction
	channelName string
}

func NewDmChannelAction(ctx context.Context, nodeID UniqueID, typ ActionType, channelName string) *DmChannelAction {
	return &DmChannelAction{
		BaseAction: NewBaseAction(ctx, nodeID, typ),

		channelName: channelName,
	}
}

func (action *DmChannelAction) ChannelName() string {
	return action.channelName
}

func (action *DmChannelAction) Execute(cluster *session.Cluster) error {
	var (
		status *commonpb.Status
		err    error
	)

	switch action.Type() {
	case ActionTypeGrow:
		req := &querypb.WatchDmChannelsRequest{}
		status, err = cluster.WatchDmChannels(action.Context(), action.Node(), req)

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

func (action *DmChannelAction) IsFinished(distMgr *meta.DistributionManager) bool {
	channels := distMgr.GetDmChannelByNode(action.nodeID)

	hasChannel := false
	for _, channel := range channels {
		if channel.Channel == action.ChannelName() {
			hasChannel = true
			break
		}
	}

	isGrow := action.Type() == ActionTypeGrow

	return hasChannel == isGrow
}

type DeltaChannelAction struct {
	*BaseAction
	channelName string
}

func NewDeltaChannelAction(ctx context.Context, nodeID UniqueID, typ ActionType, channelName string) *DeltaChannelAction {
	return &DeltaChannelAction{
		BaseAction: NewBaseAction(ctx, nodeID, typ),

		channelName: channelName,
	}
}

func (action *DeltaChannelAction) ChannelName() string {
	return action.channelName
}

func (action *DeltaChannelAction) Execute(cluster *session.Cluster) error {
	var (
		status *commonpb.Status
		err    error
	)

	switch action.Type() {
	case ActionTypeGrow:
		req := &querypb.WatchDeltaChannelsRequest{}
		status, err = cluster.WatchDeltaChannels(action.Context(), action.Node(), req)

	case ActionTypeReduce:
		// todo(yah01): Add unsub delta channel?

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
