package task

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	TransTypeGrow TransType = iota + 1
	TransTypeReduce

	InvalidID = -1
)

type TransType int32

type Trans struct {
	ID        UniqueID
	Prev      UniqueID
	JobID     UniqueID
	TaskID    UniqueID
	ReplicaID UniqueID
	NodeID    UniqueID
	Type      TransType
}

func newTrans(id, prev, jobID, replicaID, nodeID UniqueID, typ TransType) *Trans {
	return &Trans{
		ID:        id,
		Prev:      prev,
		JobID:     jobID,
		TaskID:    InvalidID,
		ReplicaID: replicaID,
		NodeID:    nodeID,
		Type:      typ,
	}
}

type TransOption func(trans *Trans)

func WithPrev(prev UniqueID) TransOption {
	return func(trans *Trans) {
		trans.Prev = prev
	}
}

func WithNodeID(nodeID UniqueID) TransOption {
	return func(trans *Trans) {
		trans.NodeID = nodeID
	}
}

type IdAllocator func() UniqueID
type TransQueue []*Trans

type StateManager struct {
	ctx     context.Context
	ticker  *time.Ticker
	rwmutex sync.RWMutex

	idAllocator IdAllocator
	executor    *Executor

	segmentManager *meta.SegmentManager
	channelManager *meta.ChannelManager
	replicaManager *meta.ReplicaManager

	// Auxiliary map for seek trans
	trans        map[UniqueID]*Trans

	// ReplicaID, SegmentID -> Queue
	segmentTrans map[UniqueID]map[UniqueID]TransQueue
	channelTrans map[UniqueID]TransQueue
}

// AddSegmentTrans commits a trans,
// which would be apply into meta later.
func (m *StateManager) AddSegmentTrans(segmentID, jobID, replicaID UniqueID, typ TransType, opts ...TransOption) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	trans := newTrans(
		m.idAllocator(),
		InvalidID,
		jobID,
		replicaID,
		InvalidID,
		typ,
	)

	// Apply options
	for _, opt := range opts {
		opt(trans)
	}

	// Enqueue
	queue, ok := m.segmentTrans[segmentID]
	if !ok {
		queue = make(TransQueue, 0, 1)
	}
	queue = append(queue, trans)
	m.segmentTrans[segmentID] = queue
}

func (m *StateManager) ApplyState() {
	for {
		select {
		case <-m.ctx.Done():
			return

		case <-m.ticker.C:

		}
	}
}

func (m *StateManager) applyState() {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	// Spawn tasks for segment trans
	for segmentID, queue := range m.segmentTrans {
		for _, trans := range queue {
			task := SpawnTaskFromSegmentTrans(segmentID, trans)
			task.SetOnDone(func() {
			})
			m.executor.Commit(task)
		}
	}

	// todo(yah01): spawn the other tasks
}

func SpawnTaskFromSegmentTrans(segmentID UniqueID, trans *Trans) Task {
	switch trans.Type {
	case TransTypeGrow:
		return &LoadSegmentTask{
			SegmentID: segmentID,
		}

	case TransTypeReduce:
		return &ReleaseSegmentTask{
			SegmentID: segmentID,
		}
	}

	return nil
}
