package dist

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/querycoordv2/session"
)

type DistController struct {
	mu       sync.RWMutex
	handlers map[int64]*distHandler
	client   *session.Cluster
}

func (dc *DistController) StartDistInstance(ctx context.Context, nodeID int64) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if _, ok := dc.handlers[nodeID]; ok {
		return
	}
	h := newDistHandler(ctx, nodeID, dc.client)
	dc.handlers[nodeID] = h
}

func (dc *DistController) Remove(nodeID int64) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if h, ok := dc.handlers[nodeID]; ok {
		h.close()
		delete(dc.handlers, nodeID)
	}
}

func (dc *DistController) Close() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for _, h := range dc.handlers {
		h.close()
	}
}

func NewDistController(client *session.Cluster) *DistController {
  return &DistController{
    handlers: make(map[int64]*distHandler),
    client: client,
  }
}
