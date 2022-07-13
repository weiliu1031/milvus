package meta

import (
	"testing"

	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/suite"
)

type LeaderViewManagerSuite struct {
	suite.Suite
	mgr        *LeaderViewManager
	collection int64
	leaders    map[int64]*LeaderView
}

func (suite *LeaderViewManagerSuite) SetupSuite() {
	suite.collection = 3999
	// Leader 1: channel dmc0, segment 1, 2
	// Leader 2: channel dmc1, segment 3, 4
	// Node 3: segment 1, 3
	// Node 4: segment 2, 4
	suite.leaders = map[int64]*LeaderView{
		1: {
			ID:           1,
			CollectionID: suite.collection,
			Channel:      "dmc0",
			Segments: map[int64]int64{
				1: 3,
				2: 4,
			},
		},
		2: {
			ID:           2,
			CollectionID: suite.collection,
			Channel:      "dmc1",
			Segments: map[int64]int64{
				3: 3,
				4: 4,
			},
		},
	}
}

func (suite *LeaderViewManagerSuite) SetupTest() {
	suite.mgr = NewLeaderViewManager()
	for _, view := range suite.leaders {
		suite.mgr.Update(view)
	}
}

func (suite *LeaderViewManagerSuite) TestGetDist() {
	mgr := suite.mgr

	// Test GetSegmentDist
	for segmentID := int64(1); segmentID <= 4; segmentID++ {
		nodes := mgr.GetSegmentDist(segmentID)
		suite.AssertSegmentDist(segmentID, nodes)
	}

	// Test GetChannelDist
	for _, shard := range []string{"dmc0", "dmc1"} {
		nodes := mgr.GetChannelDist(shard)
		suite.AssertChannelDist(shard, nodes)
	}
}

func (suite *LeaderViewManagerSuite) TestGetLeader() {
	mgr := suite.mgr

	// Test GetLeaderView
	for leader, view := range suite.leaders {
		leaderView := mgr.GetLeaderView(leader)
		suite.Equal(view, leaderView)
	}

	// Test GetLeadersByShard
	for _, view := range suite.leaders {
		leaderView := mgr.GetLeadersByShard(view.Channel)
		suite.Len(leaderView, 1)
		suite.Equal(view, leaderView[0])
	}
}

func (suite *LeaderViewManagerSuite) AssertSegmentDist(segment int64, nodes []int64) bool {
	nodeSet := typeutil.NewUniqueSet(nodes...)
	for _, view := range suite.leaders {
		node, ok := view.Segments[segment]
		if ok {
			if !suite.True(nodeSet.Contain(node)) {
				return false
			}
		}
	}
	return true
}

func (suite *LeaderViewManagerSuite) AssertChannelDist(channel string, nodes []int64) bool {
	nodeSet := typeutil.NewUniqueSet(nodes...)
	for leader, view := range suite.leaders {
		if view.Channel == channel &&
			!suite.True(nodeSet.Contain(leader)) {
			return false
		}
	}
	return true
}

func TestLeaderViewManager(t *testing.T) {
	suite.Run(t, new(LeaderViewManagerSuite))
}
