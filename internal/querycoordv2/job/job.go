package job

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// Job is request of loading/releasing collection/partitions,
// the execution flow is:
// 1. PreExecute()
// 2. Execute(), skip this step if PreExecute() failed
// 3. PostExecute()
type Job interface {
	MsgID() int64
	CollectionID() int64
	// PreExecute does checks, DO NOT persists any thing within this stage,
	PreExecute() error
	// Execute processes the request
	Execute() error
	// PostExecute clears resources, it will be always processed
	PostExecute()
	Error() error
	SetError(err error)
	Done()
	Wait() error
}

type BaseJob struct {
	ctx          context.Context
	msgID        int64
	collectionID int64
	err          error
	doneCh       chan struct{}
}

func NewBaseJob(ctx context.Context, msgID, collectionID int64) *BaseJob {
	return &BaseJob{
		ctx:          ctx,
		msgID:        msgID,
		collectionID: collectionID,
		doneCh:       make(chan struct{}),
	}
}

func (job *BaseJob) MsgID() int64 {
	return job.msgID
}

func (job *BaseJob) CollectionID() int64 {
	return job.collectionID
}

func (job *BaseJob) Error() error {
	return job.err
}

func (job *BaseJob) SetError(err error) {
	job.err = err
}

func (job *BaseJob) Done() {
	close(job.doneCh)
}

func (job *BaseJob) Wait() error {
	<-job.doneCh
	return job.err
}

func (job *BaseJob) PreExecute() error {
	return nil
}

func (job *BaseJob) Execute() error {
	return nil
}

func (job *BaseJob) PostExecute() {}

type LoadCollectionJob struct {
	*BaseJob
	req *querypb.LoadCollectionRequest

	dist      *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	broker    *meta.CoordinatorBroker
	nodeMgr   *session.NodeManager
}

func NewLoadCollectionJob(
	ctx context.Context,
	req *querypb.LoadCollectionRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
	broker *meta.CoordinatorBroker,
	nodeMgr *session.NodeManager,
) *LoadCollectionJob {
	return &LoadCollectionJob{
		BaseJob:   NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:       req,
		dist:      dist,
		meta:      meta,
		targetMgr: targetMgr,
		broker:    broker,
		nodeMgr:   nodeMgr,
	}
}

func (job *LoadCollectionJob) PreExecute() error {
	req := job.req
	log := log.With(
		zap.Int64("msg-id", req.Base.GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	if req.GetReplicaNumber() <= 0 {
		log.Info("request doesn't indicate the number of replicas, set it to 1",
			zap.Int32("replica-number", req.GetReplicaNumber()))
		req.ReplicaNumber = 1
	}

	if job.meta.Exist(req.GetCollectionID()) {
		old := job.meta.GetCollection(req.GetCollectionID())
		if old == nil {
			msg := "collection with different load type existed, please release it first"
			log.Warn(msg)
			return utils.WrapError(msg, ErrLoadParameterMismatched)
		}
		if old.GetReplicaNumber() != req.GetReplicaNumber() {
			msg := "collection with different replica number existed, release this collection first before changing its replica number"
			log.Warn(msg)
			return utils.WrapError(msg, ErrLoadParameterMismatched)
		}
		return ErrCollectionLoaded
	}

	return nil
}

func (job *LoadCollectionJob) Execute() error {
	req := job.req
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	// Create replicas
	// TODO(yah01): store replicas and collection atomically
	err := utils.SpawnReplicas(job.meta.ReplicaManager,
		job.nodeMgr,
		req.GetCollectionID(),
		req.GetReplicaNumber())
	if err != nil {
		msg := "failed to spawn replica for collection"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}

	// Fetch channels and segments from DataCoord
	partitions, err := job.broker.GetPartitions(job.ctx, req.GetCollectionID())
	if err != nil {
		msg := "failed to get partitions from RootCoord"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}
	err = utils.RegisterTargets(job.ctx,
		job.targetMgr,
		job.broker,
		req.GetCollectionID(),
		partitions...)
	if err != nil {
		msg := "failed to register channels and segments"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}

	err = job.meta.CollectionManager.PutCollection(&meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  req.GetCollectionID(),
			ReplicaNumber: req.GetReplicaNumber(),
			Status:        querypb.LoadStatus_Loading,
		},
		CreatedAt: time.Now(),
	})
	if err != nil {
		msg := "failed to store collection"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}

	return nil
}

func (job *LoadCollectionJob) PostExecute() {
	if job.Error() != nil {
		job.targetMgr.RemoveCollection(job.req.GetCollectionID())
	}
}

type ReleaseCollectionJob struct {
	*BaseJob
	req       *querypb.ReleaseCollectionRequest
	meta      *meta.Meta
	targetMgr *meta.TargetManager
}

func NewReleaseCollectionJob(ctx context.Context,
	req *querypb.ReleaseCollectionRequest,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
) *ReleaseCollectionJob {
	return &ReleaseCollectionJob{
		BaseJob:   NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:       req,
		meta:      meta,
		targetMgr: targetMgr,
	}
}

func (job *ReleaseCollectionJob) Execute() error {
	req := job.req
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)
	if !job.meta.CollectionManager.Exist(req.GetCollectionID()) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
		return nil
	}

	err := job.meta.CollectionManager.RemoveCollection(req.GetCollectionID())
	if err != nil {
		msg := "failed to remove collection"
		log.Error(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return utils.WrapError(msg, err)
	}

	err = job.meta.ReplicaManager.RemoveCollection(req.GetCollectionID())
	if err != nil {
		msg := "failed to remove replicas"
		log.Error(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return utils.WrapError(msg, err)
	}

	job.targetMgr.RemoveCollection(req.GetCollectionID())
	return nil
}

type LoadPartitionJob struct {
	*BaseJob
	req *querypb.LoadPartitionsRequest

	dist      *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	broker    *meta.CoordinatorBroker
	nodeMgr   *session.NodeManager
}

func NewLoadPartitionJob(
	ctx context.Context,
	req *querypb.LoadPartitionsRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
	broker *meta.CoordinatorBroker,
	nodeMgr *session.NodeManager,
) *LoadPartitionJob {
	return &LoadPartitionJob{
		BaseJob:   NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:       req,
		dist:      dist,
		meta:      meta,
		targetMgr: targetMgr,
		broker:    broker,
		nodeMgr:   nodeMgr,
	}
}

func (job *LoadPartitionJob) PreExecute() error {
	req := job.req
	log := log.With(
		zap.Int64("msg-id", req.Base.GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	if req.GetReplicaNumber() <= 0 {
		log.Info("request doesn't indicate the number of replicas, set it to 1",
			zap.Int32("replica-number", req.GetReplicaNumber()))
		req.ReplicaNumber = 1
	}

	if job.meta.Exist(req.GetCollectionID()) {
		old := job.meta.GetCollection(req.GetCollectionID())
		if old != nil {
			msg := "collection with different load type existed, please release it first"
			log.Warn(msg)
			return utils.WrapError(msg, ErrLoadParameterMismatched)
		}
		if old.GetReplicaNumber() != req.GetReplicaNumber() {
			msg := "collection with different replica number existed, release this collection first before changing its replica number"
			log.Warn(msg)
			return utils.WrapError(msg, ErrLoadParameterMismatched)
		}

		// Check whether one of the given partitions not loaded
		for _, partitionID := range req.GetPartitionIDs() {
			partition := job.meta.GetPartition(partitionID)
			if partition == nil {
				msg := fmt.Sprintf("some partitions %v of collection %v has been loaded into QueryNode, please release partitions firstly",
					req.GetPartitionIDs(),
					req.GetCollectionID())
				log.Warn(msg)
				return utils.WrapError(msg, ErrLoadParameterMismatched)
			}
		}
		return ErrCollectionLoaded
	}

	return nil
}

func (job *LoadPartitionJob) Execute() error {
	req := job.req
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	// Create replicas
	// TODO(yah01): store replicas and collection atomically
	err := utils.SpawnReplicas(job.meta.ReplicaManager,
		job.nodeMgr,
		req.GetCollectionID(),
		req.GetReplicaNumber())
	if err != nil {
		msg := "failed to spawn replica for collection"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}

	err = utils.RegisterTargets(job.ctx,
		job.targetMgr,
		job.broker,
		req.GetCollectionID(),
		req.GetPartitionIDs()...)
	if err != nil {
		msg := "failed to register channels and segments"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
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
	err = job.meta.CollectionManager.PutPartition(partitions...)
	if err != nil {
		msg := "failed to store partitions"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}

	return nil
}

func (job *LoadPartitionJob) PostExecute() {
	if job.Error() != nil {
		job.targetMgr.RemoveCollection(job.req.GetCollectionID())
	}
}

type ReleasePartitionJob struct {
	*BaseJob
	req       *querypb.ReleasePartitionsRequest
	meta      *meta.Meta
	targetMgr *meta.TargetManager
}

func NewReleasePartitionJob(ctx context.Context,
	req *querypb.ReleasePartitionsRequest,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
) *ReleasePartitionJob {
	return &ReleasePartitionJob{
		BaseJob:   NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:       req,
		meta:      meta,
		targetMgr: targetMgr,
	}
}

func (job *ReleasePartitionJob) Execute() error {
	req := job.req
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("collection-id", req.GetCollectionID()),
	)

	if !job.meta.CollectionManager.Exist(req.GetCollectionID()) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		return nil
	}

	if job.meta.CollectionManager.GetLoadType(req.GetCollectionID()) == querypb.LoadType_LoadCollection {
		msg := "releasing some partitions after load collection is not supported"
		log.Warn(msg)
		return utils.WrapError(msg, ErrLoadParameterMismatched)
	}

	loadedPartitions := job.meta.CollectionManager.GetPartitionsByCollection(req.GetCollectionID())
	partitionIDs := typeutil.NewUniqueSet(req.GetPartitionIDs()...)
	toRelease := make([]int64, 0)
	for _, partition := range loadedPartitions {
		if partitionIDs.Contain(partition.GetPartitionID()) {
			toRelease = append(toRelease, partition.GetPartitionID())
		}
	}

	if len(toRelease) == len(loadedPartitions) { // All partitions are released, clear all
		err := job.meta.CollectionManager.RemoveCollection(req.GetCollectionID())
		if err != nil {
			msg := "failed to release partitions from store"
			log.Warn(msg, zap.Error(err))
			metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
			return utils.WrapError(msg, err)
		}
		job.meta.ReplicaManager.RemoveCollection(req.GetCollectionID())
		job.targetMgr.RemoveCollection(req.GetCollectionID())
	} else {
		err := job.meta.CollectionManager.RemovePartition(toRelease...)
		if err != nil {
			msg := "failed to release partitions from store"
			log.Warn(msg, zap.Error(err))
			metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
			return utils.WrapError(msg, err)
		}
		for _, partition := range toRelease {
			job.targetMgr.RemovePartition(partition)
		}
	}
	return nil
}
