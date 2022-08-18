package observers

import (
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/mocks"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/suite"
)

type CollectionObserverSuite struct {
	suite.Suite

	// Data
	collections    []int64
	partitions     map[int64][]int64 // CollectionID -> PartitionIDs
	channels       map[int64][]*meta.DmChannel
	segments       map[int64][]*datapb.SegmentInfo // CollectionID -> []datapb.SegmentInfo
	loadTypes      []querypb.LoadType
	replicaNumber  []int32
	loadPercentage []int32
	nodes          []int64

	// Mocks
	idAllocator func() (int64, error)
	store       meta.Store

	// Dependencies
	dist      *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager

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
	suite.loadTypes = []querypb.LoadType{
		querypb.LoadType_LoadCollection,
		querypb.LoadType_LoadPartition,
	}
	suite.replicaNumber = []int32{1, 2}
	suite.loadPercentage = []int32{0, 50}
	suite.nodes = []int64{1, 2, 3}
}

func (suite *CollectionObserverSuite) SetupTest() {
	config := mocks.GenerateEtcdConfig()
	etcd, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	kv := etcdkv.NewEtcdKV(etcd, config.MetaRootPath)

	// Mocks
	suite.idAllocator = mocks.RandomIncrementIDAllocator()
	suite.store = meta.NewMetaStore(kv)

	// Dependencies
	suite.dist = meta.NewDistributionManager()
	suite.meta = meta.NewMeta(suite.idAllocator, suite.store)
	suite.targetMgr = meta.NewTargetManager()

	// Test object
	suite.ob = NewCollectionObserver(
		suite.dist,
		suite.meta,
		suite.targetMgr,
	)
}

func (suite *CollectionObserverSuite) TestObserveTimeout() {

}

func (suite *CollectionObserverSuite) loadAll() {
	for i, collection := range suite.collections {
		status := querypb.LoadStatus_Loaded
		if suite.loadPercentage[i] < 100 {
			status = querypb.LoadStatus_Loading
		}

		if suite.loadTypes[i] == querypb.LoadType_LoadCollection {
			suite.meta.PutCollection(&meta.Collection{
				CollectionLoadInfo: &querypb.CollectionLoadInfo{
					CollectionID:  collection,
					ReplicaNumber: suite.replicaNumber[i],
					Status:        status,
				},
				LoadPercentage: suite.loadPercentage[i],
				CreatedAt:      time.Now(),
			})
		} else {
			for _, partition := range suite.partitions[collection] {
				suite.meta.PutPartition(&meta.Partition{
					PartitionLoadInfo: &querypb.PartitionLoadInfo{
						CollectionID:  collection,
						PartitionID:   partition,
						ReplicaNumber: suite.replicaNumber[i],
						Status:        status,
					},
					LoadPercentage: suite.loadPercentage[i],
					CreatedAt:      time.Now(),
				})
			}
		}
	}
}
