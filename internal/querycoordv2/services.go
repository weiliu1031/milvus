package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

func (s *Server) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	log.Info("received load collection request",
		zap.Any("schema", req.Schema),
		zap.Int32("replica-number", req.ReplicaNumber))

	collection := &meta.Collection{
		ID:            req.GetCollectionID(),
		Schema:        req.GetSchema(),
		ReplicaNumber: req.GetReplicaNumber(),
		LoadType:      querypb.LoadType_LoadCollection,
		Status:        meta.CollectionStatusLoading,
	}
	status := utils.WrapStatus(commonpb.ErrorCode_Success, "")

	old, ok, err := s.meta.CollectionManager.GetOrPut(req.GetCollectionID(), collection)
	if err != nil {
		msg := "failed to store collection"
		log.Error(msg, zap.Error(err))
		status = utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
		return status, nil
	}
	if ok {
		if old.LoadType != collection.LoadType {
			msg := "a collection with different LoadType existed"
			log.Error(msg)
			status = utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg, nil)
		} else if old.ReplicaNumber != collection.ReplicaNumber {
			msg := "a collection with different replica number existed, release this collection first before changing its replica number"
			log.Error(msg)
			status = utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg, nil)
		}

		return status, nil
	}

	defer func() {
		if status.ErrorCode != commonpb.ErrorCode_Success {
			s.meta.CollectionManager.Remove(collection.ID)
			s.targetMgr.RemoveCollection(collection.ID)
		}
	}()

	var (
		dmChannels    map[string][]*datapb.VchannelInfo
		deltaChannels map[string][]*datapb.VchannelInfo
	)

	partitions, err := s.broker.GetPartitions(ctx, collection.ID)
	if err != nil {
		msg := "failed to get partitions from RootCoord"
		log.Error(msg, zap.Error(err))
		status = utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
		return status, nil
	}
	for _, partitionID := range partitions {
		log := log.With(
			zap.Int64("partition-id", partitionID),
		)
		vChannelInfos, binlogs, err := s.broker.GetRecoveryInfo(ctx, collection.ID, partitionID)
		if err != nil {
			msg := "failed to GetRecoveryInfo from DataCoord"
			log.Error(msg, zap.Error(err))
			status = utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
			return status, nil
		}

		for _, segmentBinlogs := range binlogs {
			s.targetMgr.AddSegment(&meta.Segment{
				SegmentInfo: utils.SegmentBinlogs2SegmentInfo(collection.ID, partitionID, segmentBinlogs),
			})
		}

		for _, info := range vChannelInfos {
			channelName := info.GetChannelName()
			dmChannels[channelName] = append(dmChannels[channelName], info)
			deltaChannel, err := utils.SpawnDeltaChannel(info)
			if err != nil {
				msg := "failed to spawn delta channel from vchannel"
				log.Error(msg,
					zap.String("channel", info.ChannelName),
					zap.Error(err),
				)
				status = utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err)
				return status, nil
			}
			deltaChannels[channelName] = append(deltaChannels[channelName], deltaChannel.VchannelInfo)
		}
	}

	for _, channels := range dmChannels {
		dmChannel := utils.MergeDmChannelInfo(channels)
		s.targetMgr.AddDmChannel(dmChannel)
	}
	for _, channels := range deltaChannels {
		deltaChannel := utils.MergeDeltaChannelInfo(channels)
		s.targetMgr.AddDeltaChannel(deltaChannel)
	}

	return status, nil
}

func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	panic("not implemented") // TODO: Implement
}
