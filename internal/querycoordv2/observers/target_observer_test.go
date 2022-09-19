package observers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

type TargetObserverSuite struct {
	suite.Suite

	kv *etcdkv.EtcdKV
	//dependency
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	distMgr   *meta.DistributionManager
	broker    *meta.MockBroker

	observer *TargetObserver

	collectionID       int64
	partitionID        int64
	nextTargetSegments []*datapb.SegmentBinlogs
	nextTargetChannels []*datapb.VchannelInfo
}

func (suite *TargetObserverSuite) SetupSuite() {
	Params.Init()
}

func (suite *TargetObserverSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)

	// meta
	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.meta = meta.NewMeta(idAllocator, store)

	suite.targetMgr = meta.NewTargetManager()
	suite.distMgr = meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.observer = NewTargetObserver(suite.meta, suite.targetMgr, suite.distMgr, suite.broker)

	suite.observer.Start(context.TODO())

	suite.collectionID = int64(1000)
	suite.partitionID = int64(100)

	err = suite.meta.CollectionManager.PutCollection(utils.CreateTestCollection(suite.collectionID, 1))
	suite.NoError(err)
	err = suite.meta.CollectionManager.PutPartition(utils.CreateTestPartition(suite.collectionID, suite.partitionID))
	suite.NoError(err)

	suite.targetMgr.Current.AddDmChannel(utils.CreateTestChannel(suite.collectionID, 2, 0, "channel-0"))
	suite.targetMgr.Current.AddSegment(&datapb.SegmentInfo{
		ID:            10,
		CollectionID:  suite.collectionID,
		PartitionID:   suite.partitionID,
		InsertChannel: "channel-0",
	})

	suite.nextTargetChannels = []*datapb.VchannelInfo{
		{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-1",
		},
		{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-2",
		},
	}

	suite.nextTargetSegments = []*datapb.SegmentBinlogs{
		{
			SegmentID:     11,
			InsertChannel: "channel-1",
		},
		{
			SegmentID:     12,
			InsertChannel: "channel-2",
		},
	}

	suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything, mock.Anything).Return(suite.nextTargetChannels, suite.nextTargetSegments, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return([]int64{suite.partitionID}, nil)
}

func (suite *TargetObserverSuite) TestTriggerUpdateTarget() {
	suite.Eventually(func() bool {
		return len(suite.targetMgr.Next.GetSegmentsByCollection(suite.collectionID)) == 2 &&
			len(suite.targetMgr.Next.GetDmChannelsByCollection(suite.collectionID)) == 2
	}, 5*time.Second, 1*time.Second)

	suite.distMgr.SegmentDistManager.Update(2, utils.CreateTestSegment(suite.collectionID, suite.partitionID, 11, 2, 0, "channel-1"))
	suite.distMgr.SegmentDistManager.Update(2, utils.CreateTestSegment(suite.collectionID, suite.partitionID, 12, 2, 1, "channel-2"))
	suite.distMgr.ChannelDistManager.Update(2, utils.CreateTestChannel(suite.collectionID, 2, 0, "channel-1"))
	suite.distMgr.ChannelDistManager.Update(2, utils.CreateTestChannel(suite.collectionID, 2, 1, "channel-2"))

	suite.Eventually(func() bool {
		return len(suite.targetMgr.Current.GetSegmentsByCollection(suite.collectionID)) == 2 &&
			len(suite.targetMgr.Current.GetDmChannelsByCollection(suite.collectionID)) == 2
	}, 5*time.Second, 1*time.Second)
}

func (suite *TargetObserverSuite) TearDownSuite() {
	suite.kv.Close()
	suite.observer.Stop()
}

func TestTargetManager(t *testing.T) {
	suite.Run(t, new(TargetObserverSuite))
}
