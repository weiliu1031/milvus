package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

type Server struct {
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
