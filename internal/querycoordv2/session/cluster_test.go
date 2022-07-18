package session

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

const bufSize = 1024 * 1024

type ClusterTestSuite struct {
	suite.Suite
	svrs     []*grpc.Server
	listners []net.Listener
	cluster  *Cluster
}

func (suite *ClusterTestSuite) SetupSuite() {
	suite.setupServers()
	suite.setupCluster()
}

func (suite *ClusterTestSuite) setupServers() {
	svrs := suite.createTestServers()
	for _, svr := range svrs {
		lis, err := net.Listen("tcp", ":0")
		suite.NoError(err)
		suite.listners = append(suite.listners, lis)
		s := grpc.NewServer()
		querypb.RegisterQueryNodeServer(s, svr)
		go func() {
			suite.Eventually(func() bool {
				return s.Serve(lis) == nil
			}, 10*time.Second, 100*time.Millisecond)
		}()
		suite.svrs = append(suite.svrs, s)
	}

	// check server ready to serve
	for _, lis := range suite.listners {
		conn, err := grpc.Dial(lis.Addr().String(), grpc.WithBlock(), grpc.WithInsecure())
		suite.NoError(err)
		suite.NoError(conn.Close())
	}
}

func (suite *ClusterTestSuite) setupCluster() {
	nodeManager := NewNodeManager()
	for i, lis := range suite.listners {
		node := NewNodeInfo(int64(i), lis.Addr().String())
		nodeManager.Add(&node)
	}
	suite.cluster = NewCluster(&nodeManager)
}

func (suite *ClusterTestSuite) createTestServers() []querypb.QueryNodeServer {
	// create 2 mock servers with 1 always return error
	ret := make([]querypb.QueryNodeServer, 0, 2)
	ret = append(ret, suite.createDefaultMockServer())
	ret = append(ret, suite.createFailedMockServer())
	return ret
}

func (suite *ClusterTestSuite) createDefaultMockServer() querypb.QueryNodeServer {
	succStatus := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	svr := mocks.NewMockQueryNodeServer(suite.T())
	// TODO: register more mock methods
	svr.EXPECT().LoadSegments(
		mock.MatchedBy(func(context.Context) bool { return true }),
		mock.AnythingOfType("*querypb.LoadSegmentsRequest"),
	).Return(succStatus, nil)
	return svr
}

func (suite *ClusterTestSuite) createFailedMockServer() querypb.QueryNodeServer {
	failStatus := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}
	svr := mocks.NewMockQueryNodeServer(suite.T())
	// TODO: register more mock methods
	svr.EXPECT().LoadSegments(
		mock.MatchedBy(func(context.Context) bool { return true }),
		mock.AnythingOfType("*querypb.LoadSegmentsRequest"),
	).Return(failStatus, nil)
	return svr
}

func (suite *ClusterTestSuite) TearDownSuite() {
	for _, svr := range suite.svrs {
		svr.GracefulStop()
	}

	suite.cluster.Close()
}

func (suite *ClusterTestSuite) TestLoadSegments() {
	ctx := context.TODO()
	status, err := suite.cluster.LoadSegments(ctx, 0, &querypb.LoadSegmentsRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, status)

	status, err = suite.cluster.LoadSegments(ctx, 1, &querypb.LoadSegmentsRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}, status)

	_, err = suite.cluster.LoadSegments(ctx, 3, &querypb.LoadSegmentsRequest{})
	suite.Error(err)
	suite.IsType(newErrNodeNotFound(3), err)
}

func TestClusterSuite(t *testing.T) {
	suite.Run(t, new(ClusterTestSuite))
}
