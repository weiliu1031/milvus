package utils

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

// WrapStatus wraps status with given error code, message and errors
func WrapStatus(code commonpb.ErrorCode, msg string, errs ...error) *commonpb.Status {
	status := &commonpb.Status{
		ErrorCode: code,
		Reason:    fmt.Sprintf("%s", msg),
	}

	for _, err := range errs {
		status.Reason = fmt.Sprintf("%s, err=%v", status.Reason, err)
	}

	return status
}

func SegmentBinlogs2SegmentInfo(collectionID int64, partitionID int64, segmentBinlogs *datapb.SegmentBinlogs) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:            segmentBinlogs.GetSegmentID(),
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: segmentBinlogs.GetInsertChannel(),
		NumOfRows:     segmentBinlogs.GetNumOfRows(),
		Binlogs:       segmentBinlogs.GetFieldBinlogs(),
		Statslogs:     segmentBinlogs.GetStatslogs(),
		Deltalogs:     segmentBinlogs.GetDeltalogs(),
	}
}

func MergeDmChannelInfo(infos []*datapb.VchannelInfo) *meta.DmChannel {
	var dmChannel *meta.DmChannel

	for _, info := range infos {
		if dmChannel == nil {
			dmChannel = meta.DmChannelFromVChannel(info)
			continue
		}

		if info.SeekPosition.GetTimestamp() < dmChannel.SeekPosition.GetTimestamp() {
			dmChannel.SeekPosition = info.SeekPosition
		}
		dmChannel.DroppedSegmentIds = append(dmChannel.DroppedSegmentIds, info.DroppedSegmentIds...)
		dmChannel.UnflushedSegmentIds = append(dmChannel.UnflushedSegmentIds, info.UnflushedSegmentIds...)
		dmChannel.FlushedSegmentIds = append(dmChannel.FlushedSegmentIds, info.FlushedSegmentIds...)
	}

	return dmChannel
}
