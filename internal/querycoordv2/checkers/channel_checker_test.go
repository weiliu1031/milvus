package checkers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

type ChannelCheckerTestSuite struct {
	suite.Suite
	kv      *etcdkv.EtcdKV
	checker *ChannelChecker
	meta    *meta.Meta
	broker  *meta.MockBroker
}

func (suite *ChannelCheckerTestSuite) SetupSuite() {
	Params.Init()
}

func (suite *ChannelCheckerTestSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)

	// meta
	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.meta = meta.NewMeta(idAllocator, store)
	suite.broker = meta.NewMockBroker(suite.T())
	targetManager := meta.NewTargetManager(suite.meta, suite.broker)

	distManager := meta.NewDistributionManager()

	balancer := suite.createMockBalancer()
	suite.checker = NewChannelChecker(suite.meta, distManager, targetManager, balancer)
}

func (suite *ChannelCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *ChannelCheckerTestSuite) createMockBalancer() balance.Balance {
	balancer := balance.NewMockBalancer(suite.T())
	balancer.EXPECT().AssignChannel(mock.Anything, mock.Anything).Maybe().Return(func(channels []*meta.DmChannel, nodes []int64) []balance.ChannelAssignPlan {
		plans := make([]balance.ChannelAssignPlan, 0, len(channels))
		for i, c := range channels {
			plan := balance.ChannelAssignPlan{
				Channel:   c,
				From:      -1,
				To:        nodes[i%len(nodes)],
				ReplicaID: -1,
			}
			plans = append(plans, plan)
		}
		return plans
	})
	return balancer
}

func (suite *ChannelCheckerTestSuite) TestLoadChannel() {
	checker := suite.checker
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1}))

	checker.targetMgr.AddDmChannel(utils.CreateTestChannel(1, 1, 1, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Len(tasks[0].Actions(), 1)
	suite.IsType((*task.ChannelAction)(nil), tasks[0].Actions()[0])
	action := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Equal(task.ActionTypeGrow, action.Type())
	suite.EqualValues(1, action.Node())
	suite.EqualValues("test-insert-channel", action.ChannelName())
}

func (suite *ChannelCheckerTestSuite) TestReduceChannel() {
	checker := suite.checker
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1}))

	checker.dist.ChannelDistManager.Update(1, utils.CreateTestChannel(1, 1, 1, "test-insert-channel"))
	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Len(tasks[0].Actions(), 1)
	suite.IsType((*task.ChannelAction)(nil), tasks[0].Actions()[0])
	action := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.Node())
	suite.EqualValues("test-insert-channel", action.ChannelName())
}

func (suite *ChannelCheckerTestSuite) TestRepeatedChannels() {
	checker := suite.checker
	err := checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	suite.NoError(err)
	err = checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	suite.NoError(err)

	checker.targetMgr.AddDmChannel(utils.CreateTestChannel(1, 1, 1, "test-insert-channel"))
	checker.dist.ChannelDistManager.Update(1, utils.CreateTestChannel(1, 1, 1, "test-insert-channel"))
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 2, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Len(tasks[0].Actions(), 1)
	suite.IsType((*task.ChannelAction)(nil), tasks[0].Actions()[0])
	action := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.Node())
	suite.EqualValues("test-insert-channel", action.ChannelName())
}

func TestChannelCheckerSuite(t *testing.T) {
	suite.Run(t, new(ChannelCheckerTestSuite))
}
