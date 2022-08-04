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

	collection := &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: req.GetCollectionID(),
			Replica:      req.GetReplicaNumber(),
			Status:       querypb.LoadStatus_Loading,
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
		status := utils.WrapStatus(commonpb.ErrorCode_Success, "")
		if s.meta.CollectionManager.GetLoadType(req.GetCollectionID()) != querypb.LoadType_LoadCollection {
			msg := "a collection with different LoadType existed"
			log.Error(msg)
			status = utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg)
		} else if old.GetReplicaNumber() != collection.GetReplicaNumber() {
			msg := "a collection with different replica number existed, release this collection first before changing its replica number"
			log.Error(msg)
			status = utils.WrapStatus(commonpb.ErrorCode_IllegalArgument, msg)
		}

		return status, nil
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
