package meta

import (
	"context"
	"errors"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"go.uber.org/zap"

	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	brokerRpcTimeout = 5 * time.Second
)

type CoordinatorBroker struct {
	dataCoord types.DataCoord
}

func (broker *CoordinatorBroker) GetRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel()

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_GetRecoveryInfo,
		},
		CollectionID: collectionID,
		PartitionID:  partitionID,
	}
	recoveryInfo, err := broker.dataCoord.GetRecoveryInfo(ctx, getRecoveryInfoRequest)
	if err != nil {
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}

	if recoveryInfo.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(recoveryInfo.Status.Reason)
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}
	log.Info("get recovery info successfully",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int("num channels", len(recoveryInfo.Channels)),
		zap.Int("num segments", len(recoveryInfo.Binlogs)))

	return recoveryInfo.Channels, recoveryInfo.Binlogs, nil
}
