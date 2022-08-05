package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

func (s *Server) loadCollection(ctx context.Context, collection *meta.Collection) *commonpb.Status {
	log := log.With(
		zap.Int64("collection-id", collection.CollectionID),
	)
	// Create replicas
	replicas, err := s.meta.ReplicaManager.Spawn(collection.GetCollectionID(), collection.GetReplicaNumber())
	if err != nil {
		msg := "failed to spawn replica for collection"
		log.Error(msg, zap.Error(err))
		return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
	}
	utils.AssignNodesToReplicas(s.nodeMgr, replicas...)
	err = s.meta.ReplicaManager.Put(replicas...)
	if err != nil {
		msg := "failed to save replica"
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

func (s *Server) loadPartitions(ctx context.Context, partitions ...*meta.Partition) *commonpb.Status {
	log := log.With(
		zap.Int64("collection-id", partitions[0].CollectionID),
	)

	partitionIDs := lo.Map(partitions, func(partition *meta.Partition, _ int) int64 {
		return partition.GetPartitionID()
	})
	log.Info("load partitions",
		zap.Int64s("partition-ids", partitionIDs))

	// Create replicas
	replicas, err := s.meta.ReplicaManager.Spawn(partitions[0].GetCollectionID(), partitions[0].GetReplicaNumber())
	if err != nil {
		msg := "failed to spawn replica for collection"
		log.Error(msg, zap.Error(err))
		return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
	}
	utils.AssignNodesToReplicas(s.nodeMgr, replicas...)
	err = s.meta.ReplicaManager.Put(replicas...)
	if err != nil {
		msg := "failed to save replica"
		log.Error(msg, zap.Error(err))
		return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
	}

	var dmChannels map[string][]*datapb.VchannelInfo
	// Fetch channels and segments from DataCoord
	for _, partition := range partitions {
		log := log.With(
			zap.Int64("partition-id", partition.GetPartitionID()),
		)
		vChannelInfos, binlogs, err := s.broker.GetRecoveryInfo(ctx, partition.GetCollectionID(), partition.GetPartitionID())
		if err != nil {
			msg := "failed to GetRecoveryInfo from DataCoord"
			log.Error(msg, zap.Error(err))
			return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
		}

		for _, segmentBinlogs := range binlogs {
			s.targetMgr.AddSegment(utils.SegmentBinlogs2SegmentInfo(
				partition.GetCollectionID(),
				partition.GetPartitionID(),
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

// checkAnyReplicaAvailable checks if the collection has enough distinct available shards. These shards
// may come from different replica group. We only need these shards to form a replica that serves query
// requests.
func (s *Server) checkAnyReplicaAvailable(collectionID int64) bool {
	for _, replica := range s.meta.ReplicaManager.GetByCollection(collectionID) {
		isAvailable := true
		for node := range replica.Nodes {
			if s.nodeMgr.Get(node) == nil {
				isAvailable = false
				break
			}
		}
		if isAvailable {
			return true
		}
	}
	return false
}
