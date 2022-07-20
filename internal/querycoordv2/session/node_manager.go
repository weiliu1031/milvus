package session

import "sync"

type Manager interface {
	Add(node *NodeInfo)
	Remove(nodeID int64)
	Get(nodeID int64) *NodeInfo
	GetAll() []*NodeInfo
}

type NodeManager struct {
	mu    sync.RWMutex
	nodes map[int64]*NodeInfo
}

func (m *NodeManager) Add(node *NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID()] = node
}

func (m *NodeManager) Remove(nodeID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
}

func (m *NodeManager) Get(nodeID int64) *NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodes[nodeID]
}

func (m *NodeManager) GetAll() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]*NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		ret = append(ret, n)
	}
	return ret
}

func NewNodeManager() NodeManager {
	return NodeManager{
		nodes: make(map[int64]*NodeInfo),
	}
}

type NodeInfo struct {
	stats
	mu   sync.RWMutex
	id   int64
	addr string
}

func (n *NodeInfo) ID() int64 {
	return n.id
}

func (n *NodeInfo) Addr() string {
	return n.addr
}

func (n *NodeInfo) UpdateStats(opts ...StatsOption) {
	n.mu.Lock()
	for _, opt := range opts {
		opt(n)
	}
	n.mu.Unlock()
}

// PreAllocate allocates space and return the result and cleaner.
// Caller should call the cleaner after the related task completes.
func (n *NodeInfo) PreAllocate(space int64) (bool, func()) {
	n.mu.Lock()
	succ := n.preAllocate(space)
	n.mu.Unlock()
	return succ, func() { n.releaseAllocation(space) }
}

func (n *NodeInfo) releaseAllocation(space int64) {
	n.mu.Lock()
	n.release(space)
	n.mu.Unlock()
}

// GetScore return the score calculated by the current stats.
// The higher score means the node has lighter load.
func (n *NodeInfo) GetScore() int {
	// TODO: impl
	return -1
}

func NewNodeInfo(id int64, addr string) NodeInfo {
	return NodeInfo{
		stats: newStats(),
		id:    id,
		addr:  addr,
	}
}

type StatsOption func(*NodeInfo)

func WithCapacity(cap int64) StatsOption {
	return func(n *NodeInfo) {
		n.setCapacity(cap)
	}
}

func WithAvailable(available int64) StatsOption {
	return func(n *NodeInfo) {
		n.setAvailable(available)
	}
}
