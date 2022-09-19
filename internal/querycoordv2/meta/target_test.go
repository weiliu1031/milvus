package meta

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type TargetSuite struct {
	suite.Suite

	// Data
	collections []int64
	partitions  map[int64][]int64
	channels    map[int64][]string
	segments    map[int64]map[int64][]int64 // CollectionID, PartitionID -> Segments
	// Derived data
	allChannels []string
	allSegments []int64

	// Test object
	t *Target
}

func (suite *TargetSuite) SetupSuite() {
	suite.collections = []int64{1000, 1001}
	suite.partitions = map[int64][]int64{
		1000: {100, 101},
		1001: {102, 103},
	}
	suite.channels = map[int64][]string{
		1000: {"1000-dmc0", "1000-dmc1"},
		1001: {"1001-dmc0", "1001-dmc1"},
	}
	suite.segments = map[int64]map[int64][]int64{
		1000: {
			100: {1, 2},
			101: {3, 4},
		},
		1001: {
			102: {5, 6},
			103: {7, 8},
		},
	}

	suite.allChannels = make([]string, 0)
	suite.allSegments = make([]int64, 0)
	for _, channels := range suite.channels {
		suite.allChannels = append(suite.allChannels, channels...)
	}
	for _, partitions := range suite.segments {
		for _, segments := range partitions {
			suite.allSegments = append(suite.allSegments, segments...)
		}
	}
}

func (suite *TargetSuite) SetupTest() {
	suite.t = NewTarget()
	for collection, channels := range suite.channels {
		for _, channel := range channels {
			suite.t.AddDmChannel(DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: collection,
				ChannelName:  channel,
			}))
		}
	}
	for collection, partitions := range suite.segments {
		for partition, segments := range partitions {
			for _, segment := range segments {
				suite.t.AddSegment(&datapb.SegmentInfo{
					ID:           segment,
					CollectionID: collection,
					PartitionID:  partition,
				})
			}
		}
	}
}

func (suite *TargetSuite) TestGet() {
	t := suite.t

	for collection, channels := range suite.channels {
		results := t.GetDmChannelsByCollection(collection)
		suite.assertChannels(channels, results)
		for _, channel := range channels {
			suite.True(t.ContainDmChannel(channel))
		}
	}

	for collection, partitions := range suite.segments {
		collectionSegments := make([]int64, 0)
		for partition, segments := range partitions {
			results := t.GetSegmentsByCollection(collection, partition)
			suite.assertSegments(segments, results)
			for _, segment := range segments {
				suite.True(t.ContainSegment(segment))
			}
			collectionSegments = append(collectionSegments, segments...)
		}
		results := t.GetSegmentsByCollection(collection)
		suite.assertSegments(collectionSegments, results)
	}
}

func (suite *TargetSuite) TestRemove() {
	t := suite.t

	for collection, partitions := range suite.segments {
		// Remove first segment of each partition
		for _, segments := range partitions {
			t.RemoveSegment(segments[0])
			suite.False(t.ContainSegment(segments[0]))
		}

		// Remove first partition of each collection
		firstPartition := suite.partitions[collection][0]
		t.RemovePartition(firstPartition)
		segments := t.GetSegmentsByCollection(collection, firstPartition)
		suite.Empty(segments)
	}

	// Remove first collection
	firstCollection := suite.collections[0]
	t.RemoveCollection(firstCollection)
	channels := t.GetDmChannelsByCollection(firstCollection)
	suite.Empty(channels)
	segments := t.GetSegmentsByCollection(firstCollection)
	suite.Empty(segments)
}

func (suite *TargetSuite) assertChannels(expected []string, actual []*DmChannel) bool {
	if !suite.Equal(len(expected), len(actual)) {
		return false
	}

	set := typeutil.NewSet(expected...)
	for _, channel := range actual {
		set.Remove(channel.ChannelName)
	}

	return suite.Len(set, 0)
}

func (suite *TargetSuite) assertSegments(expected []int64, actual []*datapb.SegmentInfo) bool {
	if !suite.Equal(len(expected), len(actual)) {
		return false
	}

	set := typeutil.NewUniqueSet(expected...)
	for _, segment := range actual {
		set.Remove(segment.ID)
	}

	return suite.Len(set, 0)
}

func TestTargetr(t *testing.T) {
	suite.Run(t, new(TargetSuite))
}
