package querycoordv2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

func (s *Server) preLoadPartition(req *querypb.LoadPartitionsRequest) *commonpb.Status {
	log := log.With(
		zap.Int64("msg-id", req.Base.GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	if len(req.GetPartitionIDs()) == 0 {
		msg := "the PartitionIDs is empty"
		log.Error(msg)
		return utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg)
	}

	partitions := lo.Map(req.GetPartitionIDs(), func(partition int64, _ int) *meta.Partition {
		return &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID:  req.GetCollectionID(),
				PartitionID:   partition,
				ReplicaNumber: req.GetReplicaNumber(),
				Status:        querypb.LoadStatus_Loading,
			},
			CreatedAt: time.Now(),
		}
	})

	olds, exist, err := s.meta.CollectionManager.GetOrPutPartition(partitions...)
	if err != nil {
		if errors.Is(err, meta.ErrLoadTypeMismatched) {
			msg := "a collection with different LoadType existed"
			log.Error(msg)
			return utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg)
		}
		msg := "failed to store partitions"
		log.Error(msg, zap.Error(err))
		return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
	}
	if exist {
		if olds[0].GetReplicaNumber() != req.GetReplicaNumber() {
			msg := "a collection with different replica number existed, release this collection first before changing its replica number"
			log.Error(msg)
			return utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg)
		} else if len(olds) < len(partitions) {
			msg := fmt.Sprintf("some partitions %v of collection %v has been loaded into QueryNode, please release partitions firstly",
				req.GetPartitionIDs(),
				req.GetCollectionID())
			log.Error(msg)
			return utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg)
		}
	}
	return utils.WrapStatus(commonpb.ErrorCode_Success, "")
}

func (s *Server) loadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) *commonpb.Status {
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	// Create replicas
	err := s.spawnReplicas(req.GetCollectionID(), req.GetReplicaNumber())
	if err != nil {
		msg := "failed to spawn replica for collection"
		log.Error(msg, zap.Error(err))
		return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
	}

	// Fetch channels and segments from DataCoord
	err = s.registerTargets(ctx, req.GetCollectionID(), req.GetPartitionIDs()...)
	if err != nil {
		msg := "failed to register channels and segments"
		log.Error(msg, zap.Error(err))
		return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
	}

	return utils.WrapStatus(commonpb.ErrorCode_Success, "")
}

func (s *Server) registerTargets(ctx context.Context,
	targetMgr *meta.TargetManager,
	broker *meta.CoordinatorBroker,
	collection int64, partitions ...int64) error {
	var dmChannels map[string][]*datapb.VchannelInfo

	for _, partitionID := range partitions {
		vChannelInfos, binlogs, err := s.broker.GetRecoveryInfo(ctx, collection, partitionID)
		if err != nil {
			return err
		}

		// Register segments
		for _, segmentBinlogs := range binlogs {
			s.targetMgr.AddSegment(utils.SegmentBinlogs2SegmentInfo(
				collection,
				partitionID,
				segmentBinlogs))
		}

		for _, info := range vChannelInfos {
			channelName := info.GetChannelName()
			dmChannels[channelName] = append(dmChannels[channelName], info)
		}
	}
	// Merge and register channels
	for _, channels := range dmChannels {
		dmChannel := utils.MergeDmChannelInfo(channels)
		s.targetMgr.AddDmChannel(dmChannel)
	}
	return nil
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

func (s *Server) getAllSegmentInfo() []*querypb.SegmentInfo {
	segments := s.dist.SegmentDistManager.GetAll()
	infos := make(map[int64]*querypb.SegmentInfo)
	for _, segment := range segments {
		info, ok := infos[segment.GetID()]
		if !ok {
			info = &querypb.SegmentInfo{}
			infos[segment.GetID()] = info
		}
		utils.MergeMetaSegmentIntoSegmentInfo(info, segment)
	}

	return lo.Values(infos)
}
