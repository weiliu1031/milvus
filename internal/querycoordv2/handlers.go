package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

func (s *Server) loadCollection(ctx context.Context, collection *meta.Collection) *commonpb.Status {
	log := log.With(
		zap.Int64("collection-id", collection.CollectionID),
	)
	// Create replicas
	_, err := s.meta.ReplicaManager.Put(collection.GetReplicaNumber(), collection.CollectionID)
	if err != nil {
		msg := "failed to spawn replica for collection"
		log.Error(msg, zap.Error(err))
		return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
	}

	var dmChannels map[string][]*datapb.VchannelInfo
	// Fetch channels and segments from DataCoord
	partitions, err := s.broker.GetPartitions(ctx, collection.CollectionID)
	if err != nil {
		msg := "failed to get partitions from RootCoord"
		log.Error(msg, zap.Error(err))
		return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
	}
	for _, partitionID := range partitions {
		log := log.With(
			zap.Int64("partition-id", partitionID),
		)
		vChannelInfos, binlogs, err := s.broker.GetRecoveryInfo(ctx, collection.CollectionID, partitionID)
		if err != nil {
			msg := "failed to GetRecoveryInfo from DataCoord"
			log.Error(msg, zap.Error(err))
			return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
		}

		for _, segmentBinlogs := range binlogs {
			s.targetMgr.AddSegment(utils.SegmentBinlogs2SegmentInfo(
				collection.CollectionID,
				partitionID,
				segmentBinlogs))
		}

		for _, info := range vChannelInfos {
			channelName := info.GetChannelName()
			dmChannels[channelName] = append(dmChannels[channelName], info)
		}
	}

	// Register channels and segments
	for _, channels := range dmChannels {
		dmChannel := utils.MergeDmChannelInfo(channels)
		s.targetMgr.AddDmChannel(dmChannel)
	}

	return utils.WrapStatus(commonpb.ErrorCode_Success, "")
}
