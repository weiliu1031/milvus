// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoordv2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/util/timerecord"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/dist"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// Only for re-export
	Params = params.Params
)

type Server struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	status              atomic.Value
	etcdCli             *clientv3.Client
	address             string
	session             *sessionutil.Session
	kv                  kv.MetaKv
	idAllocator         func() (int64, error)
	factory             dependency.Factory
	metricsCacheManager *metricsinfo.MetricsCacheManager

	// Coordinators
	dataCoord  types.DataCoord
	rootCoord  types.RootCoord
	indexCoord types.IndexCoord

	// Meta
	store     meta.Store
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	broker    meta.Broker

	// Session
	cluster session.Cluster
	nodeMgr *session.NodeManager

	// Schedulers
	jobScheduler  *job.Scheduler
	taskScheduler task.Scheduler

	// HeartBeat
	distController *dist.Controller

	// Checkers
	checkerController *checkers.CheckerController

	// Observers
	collectionObserver *observers.CollectionObserver
	leaderObserver     *observers.LeaderObserver
	targetObserver     *observers.TargetObserver

	balancer balance.Balance

	// Active-standby
	enableActiveStandBy bool
	activateFunc        func()
}

func NewQueryCoord(ctx context.Context, factory dependency.Factory) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)
	server := &Server{
		ctx:     ctx,
		cancel:  cancel,
		factory: factory,
	}
	server.UpdateStateCode(commonpb.StateCode_Abnormal)
	return server, nil
}

func (s *Server) Register() error {
	s.session.Register()
	if s.enableActiveStandBy {
		s.session.ProcessActiveStandBy(s.activateFunc)
	}
	go s.session.LivenessCheck(s.ctx, func() {
		log.Error("QueryCoord disconnected from etcd, process will exit", zap.Int64("serverID", s.session.ServerID))
		if err := s.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		// manually send signal to starter goroutine
		if s.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})
	return nil
}

func (s *Server) Init() error {
	log.Info("QueryCoord start init",
		zap.String("meta-root-path", Params.EtcdCfg.MetaRootPath.GetValue()),
		zap.String("address", s.address))

	// Init QueryCoord session
	s.session = sessionutil.NewSession(s.ctx, Params.EtcdCfg.MetaRootPath.GetValue(), s.etcdCli)
	if s.session == nil {
		return fmt.Errorf("failed to create session")
	}
	s.session.Init(typeutil.QueryCoordRole, s.address, true, true)
	s.enableActiveStandBy = Params.QueryCoordCfg.EnableActiveStandby.GetAsBool()
	s.session.SetEnableActiveStandBy(s.enableActiveStandBy)
	s.factory.Init(Params)

	// Init KV
	etcdKV := etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue())
	s.kv = etcdKV
	log.Info("query coordinator try to connect etcd success")

	// Init ID allocator
	idAllocatorKV := tsoutil.NewTSOKVBase(s.etcdCli, Params.EtcdCfg.KvRootPath.GetValue(), "querycoord-id-allocator")
	idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", idAllocatorKV)
	err := idAllocator.Initialize()
	if err != nil {
		log.Error("query coordinator id allocator initialize failed", zap.Error(err))
		return err
	}
	s.idAllocator = func() (int64, error) {
		return idAllocator.AllocOne()
	}

	// Init metrics cache manager
	s.metricsCacheManager = metricsinfo.NewMetricsCacheManager()

	// Init meta
	err = s.initMeta()
	if err != nil {
		return err
	}
	// Init session
	log.Info("init session")
	s.nodeMgr = session.NewNodeManager()
	s.cluster = session.NewCluster(s.nodeMgr)

	// Init schedulers
	log.Info("init schedulers")
	s.jobScheduler = job.NewScheduler()
	s.taskScheduler = task.NewScheduler(
		s.ctx,
		s.meta,
		s.dist,
		s.targetMgr,
		s.broker,
		s.cluster,
		s.nodeMgr,
	)

	// Init heartbeat
	log.Info("init dist controller")
	s.distController = dist.NewDistController(
		s.cluster,
		s.nodeMgr,
		s.dist,
		s.targetMgr,
		s.taskScheduler,
	)

	// Init balancer
	log.Info("init balancer")
	s.balancer = balance.NewRowCountBasedBalancer(
		s.taskScheduler,
		s.nodeMgr,
		s.dist,
		s.meta,
		s.targetMgr,
	)

	// Init checker controller
	log.Info("init checker controller")
	s.checkerController = checkers.NewCheckerController(
		s.meta,
		s.dist,
		s.targetMgr,
		s.balancer,
		s.taskScheduler,
	)

	// Init observers
	s.initObserver()

	log.Info("QueryCoord init success")
	return err
}

func (s *Server) initMeta() error {
	record := timerecord.NewTimeRecorder("querycoord")

	log.Info("init meta")
	s.store = meta.NewMetaStore(s.kv)
	s.meta = meta.NewMeta(s.idAllocator, s.store, s.nodeMgr)

	log.Info("recover meta...")
	err := s.meta.CollectionManager.Recover()
	if err != nil {
		log.Error("failed to recover collections")
		return err
	}
	metrics.QueryCoordNumCollections.WithLabelValues().Set(float64(len(s.meta.GetAll())))

	err = s.meta.ReplicaManager.Recover(s.meta.CollectionManager.GetAll())
	if err != nil {
		log.Error("failed to recover replicas")
		return err
	}

	err = s.meta.ResourceManager.Recover()
	if err != nil {
		log.Error("failed to recover resource groups")
		return err
	}

	s.dist = &meta.DistributionManager{
		SegmentDistManager: meta.NewSegmentDistManager(),
		ChannelDistManager: meta.NewChannelDistManager(),
		LeaderViewManager:  meta.NewLeaderViewManager(),
	}
	s.broker = meta.NewCoordinatorBroker(
		s.dataCoord,
		s.rootCoord,
		s.indexCoord,
	)
	s.targetMgr = meta.NewTargetManager(s.broker, s.meta)

	record.Record("Server initMeta")
	return nil
}

func (s *Server) initObserver() {
	log.Info("init observers")
	s.collectionObserver = observers.NewCollectionObserver(
		s.dist,
		s.meta,
		s.targetMgr,
	)
	s.leaderObserver = observers.NewLeaderObserver(
		s.dist,
		s.meta,
		s.targetMgr,
		s.cluster,
	)
	s.targetObserver = observers.NewTargetObserver(
		s.meta,
		s.targetMgr,
		s.dist,
		s.broker,
	)
}

func (s *Server) afterStart() {
}

func (s *Server) Start() error {
	log.Info("start watcher...")
	sessions, revision, err := s.session.GetSessions(typeutil.QueryNodeRole)
	if err != nil {
		return err
	}
	for _, node := range sessions {
		s.nodeMgr.Add(session.NewNodeInfo(node.ServerID, node.Address))
		s.taskScheduler.AddExecutor(node.ServerID)
	}

	s.meta.ResourceManager.CheckNodeStatus()
	for _, node := range sessions {
		s.handleNodeUp(node.ServerID)
	}
	s.wg.Add(1)
	go s.watchNodes(revision)

	log.Info("start recovering dist and target")
	err = s.recover()
	if err != nil {
		return err
	}

	log.Info("start cluster...")
	s.cluster.Start(s.ctx)

	log.Info("start job scheduler...")
	s.jobScheduler.Start(s.ctx)

	log.Info("start task scheduler...")
	s.taskScheduler.Start(s.ctx)

	log.Info("start checker controller...")
	s.checkerController.Start(s.ctx)

	log.Info("start observers...")
	s.collectionObserver.Start(s.ctx)
	s.leaderObserver.Start(s.ctx)
	s.targetObserver.Start(s.ctx)

	if s.enableActiveStandBy {
		s.activateFunc = func() {
			// todo to complete
			log.Info("querycoord switch from standby to active, activating")
			s.initMeta()
			s.UpdateStateCode(commonpb.StateCode_Healthy)
		}
		s.UpdateStateCode(commonpb.StateCode_StandBy)
	} else {
		s.UpdateStateCode(commonpb.StateCode_Healthy)
	}
	log.Info("QueryCoord started")

	s.afterStart()

	return nil
}

func (s *Server) Stop() error {
	s.cancel()
	if s.session != nil {
		s.session.Revoke(time.Second)
	}

	if s.session != nil {
		log.Info("stop cluster...")
		s.cluster.Stop()
	}

	if s.distController != nil {
		log.Info("stop dist controller...")
		s.distController.Stop()
	}

	if s.checkerController != nil {
		log.Info("stop checker controller...")
		s.checkerController.Stop()
	}

	if s.taskScheduler != nil {
		log.Info("stop task scheduler...")
		s.taskScheduler.Stop()
	}

	if s.jobScheduler != nil {
		log.Info("stop job scheduler...")
		s.jobScheduler.Stop()
	}

	log.Info("stop observers...")
	if s.collectionObserver != nil {
		s.collectionObserver.Stop()
	}
	if s.leaderObserver != nil {
		s.leaderObserver.Stop()
	}
	if s.targetObserver != nil {
		s.targetObserver.Stop()
	}

	s.wg.Wait()
	log.Info("QueryCoord stop successfully")
	return nil
}

// UpdateStateCode updates the status of the coord, including healthy, unhealthy
func (s *Server) UpdateStateCode(code commonpb.StateCode) {
	s.status.Store(code)
}

func (s *Server) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	nodeID := common.NotRegisteredID
	if s.session != nil && s.session.Registered() {
		nodeID = s.session.ServerID
	}
	serviceComponentInfo := &milvuspb.ComponentInfo{
		// NodeID:    Params.QueryCoordID, // will race with QueryCoord.Register()
		NodeID:    nodeID,
		StateCode: s.status.Load().(commonpb.StateCode),
	}

	return &milvuspb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		State: serviceComponentInfo,
		//SubcomponentStates: subComponentInfos,
	}, nil
}

func (s *Server) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (s *Server) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.CommonCfg.QueryCoordTimeTick.GetValue(),
	}, nil
}

func (s *Server) SetAddress(address string) {
	s.address = address
}

// SetEtcdClient sets etcd's client
func (s *Server) SetEtcdClient(etcdClient *clientv3.Client) {
	s.etcdCli = etcdClient
}

// SetRootCoord sets root coordinator's client
func (s *Server) SetRootCoord(rootCoord types.RootCoord) error {
	if rootCoord == nil {
		return errors.New("null RootCoord interface")
	}

	s.rootCoord = rootCoord
	return nil
}

// SetDataCoord sets data coordinator's client
func (s *Server) SetDataCoord(dataCoord types.DataCoord) error {
	if dataCoord == nil {
		return errors.New("null DataCoord interface")
	}

	s.dataCoord = dataCoord
	return nil
}

// SetIndexCoord sets index coordinator's client
func (s *Server) SetIndexCoord(indexCoord types.IndexCoord) error {
	if indexCoord == nil {
		return errors.New("null IndexCoord interface")
	}

	s.indexCoord = indexCoord
	return nil
}

func (s *Server) recover() error {
	// Recover target managers
	group, ctx := errgroup.WithContext(s.ctx)
	for _, collection := range s.meta.GetAll() {
		collection := collection
		group.Go(func() error {
			return s.recoverCollectionTargets(ctx, collection)
		})
	}
	err := group.Wait()
	if err != nil {
		return err
	}

	// Recover dist
	s.distController.SyncAll(s.ctx)

	return nil
}

func (s *Server) recoverCollectionTargets(ctx context.Context, collection int64) error {
	err := s.targetMgr.UpdateCollectionNextTarget(collection)
	if err != nil {
		s.meta.CollectionManager.RemoveCollection(collection)
		s.meta.ReplicaManager.RemoveCollection(collection)
		log.Error("failed to recover collection due to update next target failed",
			zap.Int64("collectionID", collection),
			zap.Error(err),
		)
	}
	return nil
}

func (s *Server) watchNodes(revision int64) {
	defer s.wg.Done()

	eventChan := s.session.WatchServices(typeutil.QueryNodeRole, revision+1, nil)
	for {
		select {
		case <-s.ctx.Done():
			log.Info("stop watching nodes, QueryCoord stopped")
			return

		case event, ok := <-eventChan:
			if !ok {
				// ErrCompacted is handled inside SessionWatcher
				log.Error("Session Watcher channel closed", zap.Int64("serverID", s.session.ServerID))
				go s.Stop()
				if s.session.TriggerKill {
					if p, err := os.FindProcess(os.Getpid()); err == nil {
						p.Signal(syscall.SIGINT)
					}
				}
				return
			}

			switch event.EventType {
			case sessionutil.SessionAddEvent:
				nodeID := event.Session.ServerID
				addr := event.Session.Address
				log.Info("add node to NodeManager",
					zap.Int64("nodeID", nodeID),
					zap.String("nodeAddr", addr),
				)
				s.nodeMgr.Add(session.NewNodeInfo(nodeID, addr))
				s.handleNodeUp(nodeID)
				s.metricsCacheManager.InvalidateSystemInfoMetrics()

			case sessionutil.SessionUpdateEvent:
				nodeID := event.Session.ServerID
				addr := event.Session.Address
				log.Info("stopping the node",
					zap.Int64("nodeID", nodeID),
					zap.String("nodeAddr", addr),
				)
				s.nodeMgr.Stopping(nodeID)
				s.checkerController.Check()

			case sessionutil.SessionDelEvent:
				nodeID := event.Session.ServerID
				log.Info("a node down, remove it", zap.Int64("nodeID", nodeID))
				s.nodeMgr.Remove(nodeID)
				s.handleNodeDown(nodeID)
				s.metricsCacheManager.InvalidateSystemInfoMetrics()
			}
		}
	}
}

func (s *Server) handleNodeUp(node int64) {
	log := log.With(zap.Int64("nodeID", node))
	s.taskScheduler.AddExecutor(node)
	s.distController.StartDistInstance(s.ctx, node)

	rgName, err := s.meta.ResourceManager.HandleNodeUp(node)
	if err != nil {
		log.Warn("HandleNodeUp: failed to assign node to resource group",
			zap.Int64("nodeID", node),
			zap.Error(err),
		)
	}

	log.Info("HandleNodeUp: assign node to resource group",
		zap.Int64("nodeID", node),
		zap.String("resourceGroup", rgName),
	)
}

func (s *Server) handleNodeDown(node int64) {
	log := log.With(zap.Int64("nodeID", node))
	s.taskScheduler.RemoveExecutor(node)
	s.distController.Remove(node)

	// Clear dist
	s.dist.LeaderViewManager.Update(node)
	s.dist.ChannelDistManager.Update(node)
	s.dist.SegmentDistManager.Update(node)

	// Clear meta
	rgName, err := s.meta.ResourceManager.HandleNodeDown(node)
	if err != nil {
		log.Warn("HandleNodeDown: failed to remove node from resource group",
			zap.Int64("nodeID", node),
			zap.Error(err),
		)
	}

	log.Info("HandleNodeDown: remove node from resource group",
		zap.Int64("nodeID", node),
		zap.String("resourceGroup", rgName),
	)

	// Clear tasks
	s.taskScheduler.RemoveByNode(node)
}
