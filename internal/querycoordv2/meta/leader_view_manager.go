package meta

import (
	"sync"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type LeaderView struct {
	ID              int64
	CollectionID    int64
	Channel         string
	Segments        map[int64]int64 // SegmentID -> NodeID
	GrowingSegments typeutil.UniqueSet
}

func (view *LeaderView) Clone() *LeaderView {
	segments := make(map[int64]int64)
	for k, v := range view.Segments {
		segments[k] = v
	}
  growings := typeutil.NewUniqueSet(view.GrowingSegments.Collect()...)

	return &LeaderView{
		ID:           view.ID,
		CollectionID: view.CollectionID,
		Channel:      view.Channel,
		Segments:     segments,
    GrowingSegments: growings,
	}
}

type channelViews map[string]*LeaderView

type LeaderViewManager struct {
	rwmutex sync.RWMutex
	views   map[int64]channelViews // LeaderID -> Views (one per shard)
}

func NewLeaderViewManager() *LeaderViewManager {
	return &LeaderViewManager{
		views: make(map[int64]channelViews),
	}
}

// Unused, may remove it
func (mgr *LeaderViewManager) GetSegmentByNode(nodeID int64) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	segments := make([]int64, 0)
	for _, views := range mgr.views {
		for _, view := range views {
			for segment, node := range view.Segments {
				if node == nodeID {
					segments = append(segments, segment)
				}
			}
		}

	}
	return segments
}

// Update updates the leader's views, all views have to be with the same leader ID
func (mgr *LeaderViewManager) Update(views ...*LeaderView) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	leaderID := views[0].ID
	mgr.views[leaderID] = make(channelViews, len(views))
	for _, view := range views {
		mgr.views[leaderID][view.Channel] = view
	}
}

// GetSegmentDist returns the list of nodes the given segment on
func (mgr *LeaderViewManager) GetSegmentDist(segmentID int64) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	nodes := make([]int64, 0)
	for _, views := range mgr.views {
		for _, view := range views {
			node, ok := view.Segments[segmentID]
			if ok {
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

// GetSegmentDist returns the list of nodes the given segment on
func (mgr *LeaderViewManager) GetChannelDist(channel string) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	nodes := make([]int64, 0)
	for leaderID, views := range mgr.views {
		for _, view := range views {
			if view.Channel == channel {
				nodes = append(nodes, leaderID)
			}
		}
	}
	return nodes
}

func (mgr *LeaderViewManager) GetLeaderView(id int64) map[string]*LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.views[id]
}

func (mgr *LeaderViewManager) GetLeaderShardView(id int64, shard string) *LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.views[id][shard]
}

func (mgr *LeaderViewManager) GetLeadersByShard(shard string) map[int64]*LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	ret := make(map[int64]*LeaderView, 0)
	for _, views := range mgr.views {
		for _, view := range views {
			if view.Channel == shard {
				ret[view.ID] = view
			}
		}
	}
	return ret
}
