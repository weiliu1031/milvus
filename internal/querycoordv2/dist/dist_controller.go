package dist

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type DistController struct {
	mu          sync.RWMutex
	handlers    map[int64]*distHandler
	client      *session.Cluster
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	scheduler   *task.Scheduler
}

func (dc *DistController) StartDistInstance(ctx context.Context, nodeID int64) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if _, ok := dc.handlers[nodeID]; ok {
		return
	}
	h := newDistHandler(ctx, nodeID, dc.client, dc.nodeManager, dc.scheduler, dc.dist)
	dc.handlers[nodeID] = h
}

func (dc *DistController) Remove(nodeID int64) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if h, ok := dc.handlers[nodeID]; ok {
		h.stop()
		delete(dc.handlers, nodeID)
	}
}

func (dc *DistController) SyncAll(ctx context.Context) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	for _, h := range dc.handlers {
		h.getDistribution(ctx)
	}
}

func (dc *DistController) Stop() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for _, h := range dc.handlers {
		h.stop()
	}
}

func NewDistController(
	client *session.Cluster,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	scheduler *task.Scheduler,
) *DistController {
	return &DistController{
		handlers:    make(map[int64]*distHandler),
		client:      client,
		nodeManager: nodeManager,
		dist:        dist,
		scheduler:   scheduler,
	}
}
