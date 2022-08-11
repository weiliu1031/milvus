package querycoordv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

var (
	successStatus = utils.WrapStatus(commonpb.ErrorCode_Success, "")
)

func (s *Server) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	log := log.With(zap.Int64("msg-id", req.GetBase().GetMsgID()))

	log.Info("show collections", zap.Int64s("collections", req.GetCollectionIDs()))

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
		Status:                successStatus,
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

	log.Info("load collection request received",
		zap.Any("schema", req.Schema),
		zap.Int32("replica-number", req.ReplicaNumber))
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel).Inc()

	loadJob := job.NewLoadCollectionJob(ctx,
		req,
		s.dist,
		s.meta,
		s.targetMgr,
		s.broker,
		s.nodeMgr)
	s.jobScheduler.Add(loadJob)
	err := loadJob.Wait()
	if err != nil && !errors.Is(err, job.ErrCollectionLoaded) {
		msg := "failed to load collection"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err), nil
	}

	metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
	return successStatus, nil
}

func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	log.Info("release collection request received")
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("release-collection")

	releaseJob := job.NewReleaseCollectionJob(ctx,
		req,
		s.meta,
		s.targetMgr)
	s.jobScheduler.Add(releaseJob)
	err := releaseJob.Wait()
	if err != nil {
		msg := "failed to release collection"
		log.Error(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err), nil
	}

	log.Info("collection released")
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(tr.ElapseSpan().Milliseconds()))
	return successStatus, nil
}

func (s *Server) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	log.Info("show partitions", zap.Int64s("partitions", req.GetPartitionIDs()))

	// TODO(yah01): now, for load collection, the percentage of partition is equal to the percentage of collection,
	// we can calculates the real percentage of partitions
	percentages := make([]int64, len(req.GetPartitionIDs()))
	isReleased := false
	switch s.meta.GetLoadType(req.GetCollectionID()) {
	case querypb.LoadType_LoadCollection:
		percentage := s.meta.GetLoadPercentage(req.GetCollectionID())
		if percentage < 0 {
			isReleased = true
			break
		}
		for i := range req.GetPartitionIDs() {
			percentages[i] = int64(percentage)
		}

	case querypb.LoadType_LoadPartition:
		for i, partitionID := range req.GetPartitionIDs() {
			partition := s.meta.GetPartition(partitionID)
			if partition == nil {
				isReleased = true
				break
			}
			percentages[i] = int64(partition.LoadPercentage)
		}

	default:
		isReleased = true
	}

	if isReleased {
		msg := fmt.Sprintf("collection %v not loaded", req.GetCollectionID())
		log.Warn(msg)
		return &querypb.ShowPartitionsResponse{
			Status: utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg),
		}, nil
	}

	return &querypb.ShowPartitionsResponse{
		Status:              successStatus,
		PartitionIDs:        req.GetPartitionIDs(),
		InMemoryPercentages: percentages,
	}, nil
}

func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	log.Info("received load partitions request",
		zap.Any("schema", req.Schema),
		zap.Int32("replica-number", req.ReplicaNumber),
		zap.Int64s("partitions", req.GetPartitionIDs()))
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel).Inc()

	loadJob := job.NewLoadPartitionJob(ctx,
		req,
		s.dist,
		s.meta,
		s.targetMgr,
		s.broker,
		s.nodeMgr)
	s.jobScheduler.Add(loadJob)
	err := loadJob.Wait()
	if err != nil && !errors.Is(err, job.ErrCollectionLoaded) {
		msg := "failed to load partitions"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err), nil
	}

	metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
	return successStatus, nil

}

func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	log.Info("release partitions", zap.Int64s("partition-ids", req.GetPartitionIDs()))
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.TotalLabel).Inc()

	if len(req.GetPartitionIDs()) == 0 {
		msg := "partitions is empty"
		log.Warn(msg)
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg), nil
	}

	tr := timerecord.NewTimeRecorder("release-partitions")
	releaseJob := job.NewReleasePartitionJob(ctx,
		req,
		s.meta,
		s.targetMgr)
	s.jobScheduler.Add(releaseJob)
	err := releaseJob.Wait()
	if err != nil {
		msg := "failed to release partitions"
		log.Error(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err), nil
	}

	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(tr.ElapseSpan().Milliseconds()))
	return successStatus, nil
}

func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	log.Info("get partition states", zap.Int64s("partition-ids", req.GetPartitionIDs()))

	msg := "partition not loaded"
	notLoadResp := &querypb.GetPartitionStatesResponse{
		Status: utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg),
	}

	states := make([]*querypb.PartitionStates, 0, len(req.GetPartitionIDs()))
	switch s.meta.GetLoadType(req.GetCollectionID()) {
	case querypb.LoadType_LoadCollection:
		collection := s.meta.GetCollection(req.GetCollectionID())
		state := querypb.PartitionState_PartialInMemory
		if collection.LoadPercentage >= 100 {
			state = querypb.PartitionState_InMemory
		}
		releasedPartitions := typeutil.NewUniqueSet(collection.GetReleasedPartitions()...)
		for _, partition := range req.GetPartitionIDs() {
			if releasedPartitions.Contain(partition) {
				log.Warn(msg)
				return notLoadResp, nil
			}
			states = append(states, &querypb.PartitionStates{
				PartitionID: partition,
				State:       state,
			})
		}

	case querypb.LoadType_LoadPartition:
		for _, partitionID := range req.GetPartitionIDs() {
			partition := s.meta.GetPartition(partitionID)
			if partition == nil {
				log.Warn(msg, zap.Int64("partition-id", partitionID))
				return notLoadResp, nil
			}
			state := querypb.PartitionState_PartialInMemory
			if partition.LoadPercentage >= 100 {
				state = querypb.PartitionState_InMemory
			}
			states = append(states, &querypb.PartitionStates{
				PartitionID: partitionID,
				State:       state,
			})
		}

	default:
		log.Warn(msg)
		return notLoadResp, nil
	}

	return &querypb.GetPartitionStatesResponse{
		Status:                successStatus,
		PartitionDescriptions: states,
	}, nil
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	log.Info("get segment info", zap.Int64s("segment-ids", req.GetSegmentIDs()))

	infos := make([]*querypb.SegmentInfo, 0, len(req.GetSegmentIDs()))
	if len(req.GetSegmentIDs()) == 0 {
		infos = s.getAllSegmentInfo()
	} else {
		for _, segmentID := range req.GetSegmentIDs() {
			segments := s.dist.SegmentDistManager.Get(segmentID)
			if len(segments) == 0 {
				msg := fmt.Sprintf("segment %v not found in any node", segmentID)
				log.Warn(msg, zap.Int64("segment-id", segmentID))
				return &querypb.GetSegmentInfoResponse{
					Status: utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg),
				}, nil
			}
			info := &querypb.SegmentInfo{}
			utils.MergeMetaSegmentIntoSegmentInfo(info, segments...)
			infos = append(infos, info)
		}
	}

	return &querypb.GetSegmentInfoResponse{
		Status: successStatus,
		Infos:  infos,
	}, nil
}

func (s *Server) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log := log.With(zap.Int64("msg-id", req.Base.GetMsgID()))

	log.Info("get metrics request received",
		zap.String("metric-type", req.GetRequest()))
	resp := &milvuspb.GetMetricsResponse{
		Status: successStatus,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole,
			utils.Params.QueryCoordCfg.GetNodeID()),
	}

	metricType, err := metricsinfo.ParseMetricType(req.GetRequest())
	if err != nil {
		msg := "failed to parse metric type"
		log.Warn(msg, zap.Error(err))
		resp.Status = utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err)
		return resp, nil
	}

	if metricType != metricsinfo.SystemInfoMetrics {
		msg := "invalid metric type"
		err := errors.New(metricsinfo.MsgUnimplementedMetric)
		log.Warn(msg, zap.Error(err))
		resp.Status = utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err)
		return resp, nil
	}

	metrics, err := s.metricsCacheManager.GetSystemInfoMetrics()
	if err != nil {
		log.Warn("failed to read metrics from cache, re-calculate it", zap.Error(err))
		metrics = resp
		metrics.Response, err = s.getSystemInfoMetrics(ctx, req)
		if err != nil {
			msg := "failed to get system info metrics"
			log.Warn(msg, zap.Error(err))
			resp.Status = utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err)
			return resp, nil
		}
	}

	s.metricsCacheManager.UpdateSystemInfoMetrics(metrics)
	return metrics, nil
}

func (s *Server) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	log := log.With(
		zap.Int64("msg-id", req.Base.GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	log.Info("get replicas request received", zap.Bool("with-shard-nodes", req.GetWithShardNodes()))
	resp := &milvuspb.GetReplicasResponse{
		Status:   successStatus,
		Replicas: make([]*milvuspb.ReplicaInfo, 0),
	}

	replicas := s.meta.ReplicaManager.GetByCollection(req.GetCollectionID())
	if len(replicas) == 0 {
		msg := "failed to get replicas, collection not loaded"
		log.Warn(msg)
		resp.Status = utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg)
		return resp, nil
	}

	for _, replica := range replicas {
		info, err := s.fillReplicaInfo(replica, req.GetWithShardNodes())
		if err != nil {
			msg := "failed to get replica info"
			log.Warn(msg,
				zap.Int64("replica-id", replica.GetID()),
				zap.Error(err))
			resp.Status = utils.WrapStatus(commonpb.ErrorCode_MetaFailed, msg, err)
		}
		resp.Replicas = append(resp.Replicas, info)
	}
	return resp, nil
}

func (s *Server) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	panic("not implemented") // TODO: Implement
}
