package querycoordv2

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

func (s *Server) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	log := log.With(zap.Int64("msg-id", req.GetBase().GetMsgID()))

	log.Info("show collections", zap.Int64s("collection-ids", req.GetCollectionIDs()))

	collectionSet := typeutil.NewUniqueSet(req.GetCollectionIDs()...)
	if len(req.GetCollectionIDs()) == 0 {
		for _, collection := range s.meta.GetAllCollections() {
			collectionSet.Insert(collection.GetCollectionID())
		}
		for _, partition := range s.meta.GetAllPartitions() {
			collectionSet.Insert(partition.GetCollectionID())
		}
	}
	collections := collectionSet.Collect()

	resp := &querypb.ShowCollectionsResponse{
		Status:                utils.WrapStatus(commonpb.ErrorCode_Success, ""),
		CollectionIDs:         collections,
		InMemoryPercentages:   make([]int64, len(collectionSet)),
		QueryServiceAvailable: make([]bool, len(collectionSet)),
	}
	for i, collectionID := range collections {
		log := log.With(zap.Int64("collection-id", collectionID))

		percentage := s.meta.CollectionManager.GetLoadPercentage(collectionID)
		if percentage < 0 {
			err := fmt.Errorf("collection %d has not been loaded to memory or load failed", collectionID)
			log.Warn("show collection failed", zap.Error(err))
			return &querypb.ShowCollectionsResponse{
				Status: utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, err.Error()),
			}, nil
		}
		resp.InMemoryPercentages[i] = int64(percentage)
		resp.QueryServiceAvailable[i] = s.checkAnyReplicaAvailable(collectionID)
	}

	return resp, nil
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
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	if !s.meta.CollectionManager.Exist(req.GetCollectionID()) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
		return utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil
	}

	tr := timerecord.NewTimeRecorder("release-collection")
	err := s.meta.CollectionManager.RemoveCollection(req.GetCollectionID())
	if err != nil {
		msg := "failed to remove collection"
		log.Error(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err), nil
	}

	err = s.meta.ReplicaManager.RemoveCollection(req.GetCollectionID())
	if err != nil {
		msg := "failed to remove replicas"
		log.Error(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err), nil
	}

	s.targetMgr.RemoveCollection(req.GetCollectionID())

	log.Info("collection removed")
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(tr.ElapseSpan().Milliseconds()))
	return utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil
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
