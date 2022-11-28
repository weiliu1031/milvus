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

package observers

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type CollectionObserverSuite struct {
	suite.Suite

	// Data
	collections    []int64
	partitions     map[int64][]int64 // CollectionID -> PartitionIDs
	channels       map[int64][]*meta.DmChannel
	segments       map[int64][]*datapb.SegmentInfo // CollectionID -> []datapb.SegmentInfo
	loadTypes      map[int64]querypb.LoadType
	replicaNumber  map[int64]int32
	loadPercentage map[int64]int32
	nodes          []int64

	// Mocks
	idAllocator func() (int64, error)
	etcd        *clientv3.Client
	kv          kv.MetaKv
	store       meta.Store

	// Dependencies
	dist      *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	broker    *meta.MockBroker
	handoffOb *HandoffObserver

	// Test object
	ob *CollectionObserver
}

func (suite *CollectionObserverSuite) SetupSuite() {
	Params.Init()

	suite.collections = []int64{100, 101}
	suite.partitions = map[int64][]int64{
		100: {10},
		101: {11, 12},
	}
	suite.channels = map[int64][]*meta.DmChannel{
		100: {
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "100-dmc0",
			}),
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "100-dmc1",
			}),
		},
		101: {
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 101,
				ChannelName:  "101-dmc0",
			}),
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 101,
				ChannelName:  "101-dmc1",
			}),
		},
	}
	suite.segments = map[int64][]*datapb.SegmentInfo{
		100: {
			&datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				InsertChannel: "100-dmc0",
			},
			&datapb.SegmentInfo{
				ID:            2,
				CollectionID:  100,
				PartitionID:   10,
				InsertChannel: "100-dmc1",
			},
		},
		101: {
			&datapb.SegmentInfo{
				ID:            3,
				CollectionID:  101,
				PartitionID:   11,
				InsertChannel: "101-dmc0",
			},
			&datapb.SegmentInfo{
				ID:            4,
				CollectionID:  101,
				PartitionID:   12,
				InsertChannel: "101-dmc1",
			},
		},
	}
	suite.loadTypes = map[int64]querypb.LoadType{
		100: querypb.LoadType_LoadCollection,
		101: querypb.LoadType_LoadPartition,
	}
	suite.replicaNumber = map[int64]int32{
		100: 1,
		101: 1,
	}
	suite.loadPercentage = map[int64]int32{
		100: 0,
		101: 50,
	}
	suite.nodes = []int64{1, 2, 3}
}

func (suite *CollectionObserverSuite) SetupTest() {
	// Mocks
	var err error
	suite.idAllocator = RandomIncrementIDAllocator()
	log.Debug("create embedded etcd KV...")
	config := GenerateEtcdConfig()
	client, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(client, Params.EtcdCfg.MetaRootPath+"-"+RandomMetaRootPath())
	suite.Require().NoError(err)
	log.Debug("create meta store...")
	suite.store = meta.NewMetaStore(suite.kv)

	// Dependencies
	suite.dist = meta.NewDistributionManager()
	suite.meta = meta.NewMeta(suite.idAllocator, suite.store)
	suite.targetMgr = meta.NewTargetManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.handoffOb = NewHandoffObserver(
		suite.store,
		suite.meta,
		suite.dist,
		suite.targetMgr,
		suite.broker,
	)

	// Test object
	suite.ob = NewCollectionObserver(
		suite.dist,
		suite.meta,
		suite.targetMgr,
		suite.broker,
		suite.handoffOb,
	)

	Params.QueryCoordCfg.LoadTimeoutSeconds = 600 * time.Second
	Params.QueryCoordCfg.RefreshTargetsIntervalSeconds = 600 * time.Second

	suite.loadAll()
}

func (suite *CollectionObserverSuite) TearDownTest() {
	suite.ob.Stop()
	suite.kv.Close()
}

func (suite *CollectionObserverSuite) TestObserveCollectionTimeout() {
	const (
		timeout = 2 * time.Second
	)
	// Not timeout
	Params.QueryCoordCfg.LoadTimeoutSeconds = timeout
	suite.ob.Start(context.Background())

	// Collection 100 timeout,
	// collection 101 loaded timeout
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: 101,
		Channel:      "101-dmc0",
		Segments:     map[int64]*querypb.SegmentDist{3: {NodeID: 1, Version: 0}},
	})
	suite.dist.LeaderViewManager.Update(2, &meta.LeaderView{
		ID:           2,
		CollectionID: 101,
		Channel:      "101-dmc1",
		Segments:     map[int64]*querypb.SegmentDist{4: {NodeID: 2, Version: 0}},
	})
	suite.Eventually(func() bool {
		return suite.isCollectionTimeout(suite.collections[0]) &&
			suite.isCollectionLoaded(suite.collections[1])
	}, timeout*2, timeout/10)
}

func (suite *CollectionObserverSuite) TestObservePartitionsTimeout() {
	const (
		timeout = 2 * time.Second
	)
	// Not timeout
	Params.QueryCoordCfg.LoadTimeoutSeconds = timeout
	suite.ob.Start(context.Background())

	// Collection 100 loaded before timeout,
	// collection 101 timeout
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: 100,
		Channel:      "100-dmc0",
		Segments:     map[int64]*querypb.SegmentDist{1: {NodeID: 1, Version: 0}},
	})
	suite.dist.LeaderViewManager.Update(2, &meta.LeaderView{
		ID:           2,
		CollectionID: 100,
		Channel:      "100-dmc1",
		Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 2, Version: 0}},
	})
	suite.Eventually(func() bool {
		return suite.isCollectionLoaded(suite.collections[0]) &&
			suite.isCollectionTimeout(suite.collections[1])
	}, timeout*2, timeout/10)
}

func (suite *CollectionObserverSuite) TestObserveCollectionRefresh() {
	const (
		timeout = 2 * time.Second
	)
	// Not timeout
	Params.QueryCoordCfg.RefreshTargetsIntervalSeconds = timeout
	suite.broker.EXPECT().GetPartitions(mock.Anything, int64(100)).Return(suite.partitions[100], nil)
	for _, partition := range suite.partitions[100] {
		suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, int64(100), partition).Return(nil, nil, nil)
	}
	suite.ob.Start(context.Background())

	// Collection 100 refreshed,
	// collection 101 loaded
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: 101,
		Channel:      "101-dmc0",
		Segments:     map[int64]*querypb.SegmentDist{3: {NodeID: 1, Version: 0}},
	})
	suite.dist.LeaderViewManager.Update(2, &meta.LeaderView{
		ID:           2,
		CollectionID: 101,
		Channel:      "101-dmc1",
		Segments:     map[int64]*querypb.SegmentDist{4: {NodeID: 2, Version: 0}},
	})
	time.Sleep(timeout * 2)
}

func (suite *CollectionObserverSuite) TestObservePartitionsRefresh() {
	const (
		timeout = 2 * time.Second
	)
	// Not timeout
	Params.QueryCoordCfg.RefreshTargetsIntervalSeconds = timeout
	for _, partition := range suite.partitions[101] {
		suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, int64(101), partition).Return(nil, nil, nil)
	}
	suite.ob.Start(context.Background())

	// Collection 100 loaded,
	// collection 101 refreshed
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: 100,
		Channel:      "100-dmc0",
		Segments:     map[int64]*querypb.SegmentDist{1: {NodeID: 1, Version: 0}},
	})
	suite.dist.LeaderViewManager.Update(2, &meta.LeaderView{
		ID:           2,
		CollectionID: 100,
		Channel:      "100-dmc1",
		Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 2, Version: 0}},
	})
	time.Sleep(timeout * 2)
}

func (suite *CollectionObserverSuite) isCollectionLoaded(collection int64) bool {
	exist := suite.meta.Exist(collection)
	percentage := suite.meta.GetLoadPercentage(collection)
	status := suite.meta.GetStatus(collection)
	replicas := suite.meta.ReplicaManager.GetByCollection(collection)
	channels := suite.targetMgr.GetDmChannelsByCollection(collection)
	segments := suite.targetMgr.GetSegmentsByCollection(collection)

	return exist &&
		percentage == 100 &&
		status == querypb.LoadStatus_Loaded &&
		len(replicas) == int(suite.replicaNumber[collection]) &&
		len(channels) == len(suite.channels[collection]) &&
		len(segments) == len(suite.segments[collection])
}

func (suite *CollectionObserverSuite) isCollectionTimeout(collection int64) bool {
	exist := suite.meta.Exist(collection)
	replicas := suite.meta.ReplicaManager.GetByCollection(collection)
	channels := suite.targetMgr.GetDmChannelsByCollection(collection)
	segments := suite.targetMgr.GetSegmentsByCollection(collection)

	return !(exist ||
		len(replicas) > 0 ||
		len(channels) > 0 ||
		len(segments) > 0)
}

func (suite *CollectionObserverSuite) loadAll() {
	for _, collection := range suite.collections {
		suite.load(collection)
	}
}

func (suite *CollectionObserverSuite) load(collection int64) {
	// Mock meta data
	replicas, err := suite.meta.ReplicaManager.Spawn(collection, suite.replicaNumber[collection])
	suite.NoError(err)
	for _, replica := range replicas {
		replica.AddNode(suite.nodes...)
	}
	err = suite.meta.ReplicaManager.Put(replicas...)
	suite.NoError(err)

	if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
		suite.meta.PutCollection(&meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  collection,
				ReplicaNumber: suite.replicaNumber[collection],
				Status:        querypb.LoadStatus_Loading,
			},
			LoadPercentage: 0,
			CreatedAt:      time.Now(),
		})
	} else {
		for _, partition := range suite.partitions[collection] {
			suite.meta.PutPartition(&meta.Partition{
				PartitionLoadInfo: &querypb.PartitionLoadInfo{
					CollectionID:  collection,
					PartitionID:   partition,
					ReplicaNumber: suite.replicaNumber[collection],
					Status:        querypb.LoadStatus_Loading,
				},
				LoadPercentage: 0,
				CreatedAt:      time.Now(),
			})
		}
	}

	suite.targetMgr.AddDmChannel(suite.channels[collection]...)
	suite.targetMgr.AddSegment(suite.segments[collection]...)
}

func (suite *CollectionObserverSuite) TestRecoverTimeout() {
	const (
		timeout = 1 * time.Second
	)

	Params.QueryCoordCfg.LoadTimeoutSeconds = timeout
	suite.ob.Start(context.Background())

	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: 100,
		Channel:      "100-dmc0",
		Segments:     map[int64]*querypb.SegmentDist{},
	})

	suite.Eventually(func() bool {
		return suite.isCollectionTimeout(100)
	}, timeout*2, timeout/10)
}

func TestCollectionObserver(t *testing.T) {
	suite.Run(t, new(CollectionObserverSuite))
}
