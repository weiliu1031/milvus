package job

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

// Job is request of loading/releasing collection/partitions,
// the execution flow is:
// 1. PreExecute()
// 2. Execute(), skip this step if PreExecute() failed
// 3. PostExecute()
type Job interface {
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
	collectionID int64
	err          error
	doneCh       chan struct{}
}

func NewBaseJob(ctx context.Context) *BaseJob {
	return &BaseJob{
		ctx:    ctx,
		doneCh: make(chan struct{}),
	}
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
		BaseJob:   NewBaseJob(ctx),
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

func (job *LoadCollectionJob) Execute(ctx context.Context) error {
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
	partitions, err := job.broker.GetPartitions(ctx, req.GetCollectionID())
	if err != nil {
		msg := "failed to get partitions from RootCoord"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}
	err = utils.RegisterTargets(ctx,
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

func (job *LoadCollectionJob) PostExecute() error {
	if job.Error() != nil {
		job.targetMgr.RemoveCollection(job.req.GetCollectionID())
	}
	return nil
}
