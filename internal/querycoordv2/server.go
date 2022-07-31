package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type Server struct {
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	broker    *meta.CoordinatorBroker
	cluster   *session.Cluster
	nodeMgr   *session.NodeManager

	scheduler *task.Scheduler
}

func (s *Server) Init() error {
	panic("not implemented") // TODO: Implement
}

func (s *Server) Start() error {
	panic("not implemented") // TODO: Implement
}

func (s *Server) Stop() error {
	panic("not implemented") // TODO: Implement
}

func (s *Server) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) Register() error {
	panic("not implemented") // TODO: Implement
}

func (s *Server) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	panic("not implemented") // TODO: Implement
}
