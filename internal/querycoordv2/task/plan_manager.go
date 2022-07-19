package task

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	TransTypeGrow PlanType = iota + 1
	TransTypeReduce

	InvalidID = -1
)

type PlanType int32

type Plan struct {
	ID        UniqueID
	Prev      UniqueID
	JobID     UniqueID
	TaskID    UniqueID
	ReplicaID UniqueID
	NodeID    UniqueID
	Type      PlanType
}

func newPlan(id, prev, jobID, replicaID, nodeID UniqueID, typ PlanType) *Plan {
	return &Plan{
		ID:        id,
		Prev:      prev,
		JobID:     jobID,
		TaskID:    InvalidID,
		ReplicaID: replicaID,
		NodeID:    nodeID,
		Type:      typ,
	}
}

type PlanOption func(trans *Plan)

func WithPrev(prev UniqueID) PlanOption {
	return func(trans *Plan) {
		trans.Prev = prev
	}
}

func WithNodeID(nodeID UniqueID) PlanOption {
	return func(trans *Plan) {
		trans.NodeID = nodeID
	}
}

type IdAllocator func() UniqueID
type PlanQueue []*Plan

type PlanManager struct {
	ctx     context.Context
	ticker  *time.Ticker
	rwmutex sync.RWMutex

	idAllocator IdAllocator
	executor    *Executor

	segmentManager *meta.SegmentDistManager
	channelManager *meta.ChannelManager
	replicaManager *meta.ReplicaManager

	// Auxiliary map for seek plans
	plans map[UniqueID]*Plan

	// ReplicaID, SegmentID -> Queue
	segmentTrans map[UniqueID]PlanQueue
	channelTrans map[UniqueID]PlanQueue
}

// AddSegmentTrans commits a trans,
// which would be apply into meta later.
func (m *PlanManager) AddSegmentPlan(segmentID, jobID, replicaID UniqueID, typ PlanType, opts ...PlanOption) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	plan := newPlan(
		m.idAllocator(),
		InvalidID,
		jobID,
		replicaID,
		InvalidID,
		typ,
	)

	// Apply options
	for _, opt := range opts {
		opt(plan)
	}

	// Enqueue
	queue, ok := m.segmentTrans[segmentID]
	if !ok {
		queue = make(PlanQueue, 0, 1)
	}
	queue = append(queue, plan)
	m.segmentTrans[segmentID] = queue
}

func (m *PlanManager) ApplyState() {
	for {
		select {
		case <-m.ctx.Done():
			return

		case <-m.ticker.C:

		}
	}
}

// func (m *PlanManager) applyState() {
// 	m.rwmutex.Lock()
// 	defer m.rwmutex.Unlock()

// 	// Spawn tasks for segment trans
// 	for segmentID, queue := range m.segmentTrans {
// 		for _, trans := range queue {
// 			task := SpawnTaskFromSegmentTrans(segmentID, trans)
// 			task.SetOnDone(func() {
// 			})
// 			m.executor.Commit(task)
// 		}
// 	}

// 	// todo(yah01): spawn the other tasks
// }
