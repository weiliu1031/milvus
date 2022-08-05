package querycoordv2

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
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

	if s.meta.CollectionManager.GetLoadType(req.GetCollectionID()) != querypb.LoadType_LoadCollection {
		msg := "a collection with different LoadType existed"
		log.Error(msg)
		return utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg), nil
	}

	collection := &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  req.GetCollectionID(),
			ReplicaNumber: req.GetReplicaNumber(),
			Status:        querypb.LoadStatus_Loading,
		},
		CreatedAt: time.Now(),
	}

	old, ok, err := s.meta.CollectionManager.GetOrPut(req.GetCollectionID(), collection)
	if err != nil {
		msg := "failed to store collection"
		log.Error(msg, zap.Error(err))
		return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err), nil
	}
	if ok {
		if old.GetReplicaNumber() != collection.GetReplicaNumber() {
			msg := "a collection with different replica number existed, release this collection first before changing its replica number"
			log.Error(msg)
			return utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg), nil
		}

		return utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil
	}

	status := s.loadCollection(ctx, collection)
	if status.ErrorCode != commonpb.ErrorCode_Success {
		s.meta.CollectionManager.RemoveCollection(collection.CollectionID)
		s.meta.ReplicaManager.RemoveCollection(collection.CollectionID)
		s.targetMgr.RemoveCollection(collection.CollectionID)
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
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	log.Info("received load collection request",
		zap.Any("schema", req.Schema),
		zap.Int32("replica-number", req.ReplicaNumber))

	if s.meta.CollectionManager.GetLoadType(req.GetCollectionID()) != querypb.LoadType_LoadPartition {
		msg := "a collection with different LoadType existed"
		log.Error(msg)
		return utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg), nil
	}

	partitions := make([]*meta.Partition, len(req.GetPartitionIDs()))
	for i := range partitions {
		partitions[i] = &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID:  req.GetCollectionID(),
				PartitionID:   req.GetPartitionIDs()[i],
				ReplicaNumber: req.GetReplicaNumber(),
				Status:        querypb.LoadStatus_Loading,
			},
			CreatedAt: time.Now(),
		}

		old, ok, err := s.meta.CollectionManager.GetOrPutPartition(partitions[i])
		if err != nil {
			msg := "failed to store partition"
			log.Error(msg, zap.Error(err))
			return utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err), nil
		}
		if ok {
			if old.GetReplicaNumber() != partitions[i].GetReplicaNumber() {
				msg := "a collection with different replica number existed, release this collection first before changing its replica number"
				log.Error(msg)
				return utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg), nil
			}

			return utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil
		}
	}

	status := s.loadPartitions(ctx, partitions...)
	if status.ErrorCode != commonpb.ErrorCode_Success {
		for _, partition := range partitions {
			s.meta.CollectionManager.RemovePartition(partition.GetPartitionID())
		}
		s.meta.ReplicaManager.RemoveCollection(partitions[0].GetCollectionID())
		s.targetMgr.RemoveCollection(partitions[0].GetCollectionID())
	}
	return status, nil
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
