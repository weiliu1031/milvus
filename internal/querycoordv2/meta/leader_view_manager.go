package meta

import "sync"

type LeaderView struct {
	ID           int64
	CollectionID int64
	Channel      string
	Segments     map[int64]int64 // SegmentID -> NodeID
}

func (view *LeaderView) Clone() *LeaderView {
	segments := make(map[int64]int64)
	for k, v := range view.Segments {
		segments[k] = v
	}

	return &LeaderView{
		ID:           view.ID,
		CollectionID: view.CollectionID,
		Channel:      view.Channel,
		Segments:     segments,
	}
}

type LeaderViewManager struct {
	rwmutex sync.RWMutex
	views   map[int64]*LeaderView
}

func NewLeaderViewManager() *LeaderViewManager {
	return &LeaderViewManager{
		views: make(map[int64]*LeaderView),
	}
}

func (mgr *LeaderViewManager) GetSegmentByNode(nodeID int64) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	segments := make([]int64, 0)
	for _, view := range mgr.views {
		for segment, node := range view.Segments {
			if node == nodeID {
				segments = append(segments, segment)
			}
		}
	}
	return segments
}

func (mgr *LeaderViewManager) Update(leaderID int64, channel string, segments map[int64]int64) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	mgr.views[leaderID] = &LeaderView{
		ID:       leaderID,
		Channel:  channel,
		Segments: segments,
	}
}

// GetSegmentDist returns the list of nodes the given segment on
func (mgr *LeaderViewManager) GetSegmentDist(segmentID int64) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	nodes := make([]int64, 0)
	for _, view := range mgr.views {
		node, ok := view.Segments[segmentID]
		if ok {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// GetSegmentDist returns the list of nodes the given segment on
func (mgr *LeaderViewManager) GetChannelDist(channel string) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	nodes := make([]int64, 0)
	for leaderID, view := range mgr.views {
		if view.Channel == channel {
			nodes = append(nodes, leaderID)
		}
	}
	return nodes
}

func (mgr *LeaderViewManager) GetLeaderView(id int64) *LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.views[id]
}

func (mgr *LeaderViewManager) GetLeadersByShard(shard string) []*LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	views := make([]*LeaderView,0)
	for _,view := range mgr.views {
		if view.Channel == shard {
			views = append(views, view)
		}
	}
	return views
}
