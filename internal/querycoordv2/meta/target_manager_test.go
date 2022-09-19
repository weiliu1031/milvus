package meta

import (
	"context"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type TargetManagerSuite struct {
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
	mgr *TargetManager
}

func (suite *TargetManagerSuite) SetupSuite() {
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

func (suite *TargetManagerSuite) SetupTest() {
	suite.mgr = NewTargetManager()
	for collection, channels := range suite.channels {
		for _, channel := range channels {
			suite.mgr.Next.AddDmChannel(DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: collection,
				ChannelName:  channel,
			}))
		}
	}
	for collection, partitions := range suite.segments {
		for partition, segments := range partitions {
			for _, segment := range segments {
				suite.mgr.Next.AddSegment(&datapb.SegmentInfo{
					ID:           segment,
					CollectionID: collection,
					PartitionID:  partition,
				})
			}
		}
	}
}

func (suite *TargetManagerSuite) TestUpdateCurrentTarget() {
	collectionID := int64(1000)
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments([]int64{}, suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Current.GetDmChannelsByCollection(collectionID))

	suite.mgr.UpdateCollectionCurrentTarget(collectionID)
	suite.assertSegments([]int64{}, suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.Current.GetDmChannelsByCollection(collectionID))
}

func (suite *TargetManagerSuite) TestUpdateNextTarget() {
	collectionID := int64(1003)
	suite.assertSegments([]int64{}, suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments([]int64{}, suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Current.GetDmChannelsByCollection(collectionID))

	nextTargetChannels := []*datapb.VchannelInfo{
		{
			CollectionID: collectionID,
			ChannelName:  "channel-1",
		},
		{
			CollectionID: collectionID,
			ChannelName:  "channel-2",
		},
	}

	nextTargetSegments := []*datapb.SegmentBinlogs{
		{
			SegmentID:     11,
			InsertChannel: "channel-1",
		},
		{
			SegmentID:     12,
			InsertChannel: "channel-2",
		},
	}
	broker := NewMockBroker(suite.T())
	broker.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything, mock.Anything).Return(nextTargetChannels, nextTargetSegments, nil)

	err := suite.mgr.UpdateNextTarget(context.TODO(), collectionID, []int64{105}, broker)
	suite.NoError(err)
	suite.assertSegments([]int64{11, 12}, suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{"channel-1", "channel-2"}, suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments([]int64{}, suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Current.GetDmChannelsByCollection(collectionID))

	nextTargetChannels2 := []*datapb.VchannelInfo{
		{
			CollectionID: collectionID,
			ChannelName:  "channel-1",
		},
		{
			CollectionID: collectionID,
			ChannelName:  "channel-2",
		},
	}

	nextTargetSegments2 := []*datapb.SegmentBinlogs{
		{
			SegmentID:     13,
			InsertChannel: "channel-1",
		},
		{
			SegmentID:     14,
			InsertChannel: "channel-2",
		},
	}
	broker1 := NewMockBroker(suite.T())
	broker1.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything, mock.Anything).Return(nextTargetChannels2, nextTargetSegments2, nil)
	err = suite.mgr.UpdateNextTarget(context.TODO(), collectionID, []int64{105}, broker1)
	suite.NoError(err)
	suite.assertSegments([]int64{13, 14}, suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{"channel-1", "channel-2"}, suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments([]int64{}, suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Current.GetDmChannelsByCollection(collectionID))

}

func (suite *TargetManagerSuite) TestRemovePartition() {
	collectionID := int64(1000)
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments([]int64{}, suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Current.GetDmChannelsByCollection(collectionID))

	suite.mgr.RemovePartition(100)
	suite.assertSegments(suite.getAllSegment(collectionID, []int64{101}), suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments([]int64{}, suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Current.GetDmChannelsByCollection(collectionID))

	collectionID = int64(1001)
	suite.mgr.UpdateCollectionCurrentTarget(collectionID)
	suite.assertSegments([]int64{}, suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.Current.GetDmChannelsByCollection(collectionID))

	suite.mgr.RemovePartition(102)
	suite.assertSegments([]int64{}, suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments(suite.getAllSegment(collectionID, []int64{103}), suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.Current.GetDmChannelsByCollection(collectionID))
}

func (suite *TargetManagerSuite) TestRemoveCollection() {
	collectionID := int64(1000)
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments([]int64{}, suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Current.GetDmChannelsByCollection(collectionID))

	suite.mgr.RemoveCollection(collectionID)
	suite.assertSegments([]int64{}, suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments([]int64{}, suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Current.GetDmChannelsByCollection(collectionID))

	collectionID = int64(1001)
	suite.mgr.UpdateCollectionCurrentTarget(collectionID)
	suite.assertSegments([]int64{}, suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.Current.GetDmChannelsByCollection(collectionID))

	suite.mgr.RemoveCollection(collectionID)
	suite.assertSegments([]int64{}, suite.mgr.Next.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Next.GetDmChannelsByCollection(collectionID))
	suite.assertSegments([]int64{}, suite.mgr.Current.GetSegmentsByCollection(collectionID))
	suite.assertChannels([]string{}, suite.mgr.Current.GetDmChannelsByCollection(collectionID))
}

func (suite *TargetManagerSuite) getAllSegment(collectionID int64, partitionIDs []int64) []int64 {
	allSegments := make([]int64, 0)
	for collection, partitions := range suite.segments {
		if collectionID == collection {
			for partition, segments := range partitions {
				if lo.Contains(partitionIDs, partition) {
					allSegments = append(allSegments, segments...)
				}
			}
		}
	}

	return allSegments
}

func (suite *TargetManagerSuite) assertChannels(expected []string, actual []*DmChannel) bool {
	if !suite.Equal(len(expected), len(actual)) {
		return false
	}

	set := typeutil.NewSet(expected...)
	for _, channel := range actual {
		set.Remove(channel.ChannelName)
	}

	return suite.Len(set, 0)
}

func (suite *TargetManagerSuite) assertSegments(expected []int64, actual []*datapb.SegmentInfo) bool {
	if !suite.Equal(len(expected), len(actual)) {
		return false
	}

	set := typeutil.NewUniqueSet(expected...)
	for _, segment := range actual {
		set.Remove(segment.ID)
	}

	return suite.Len(set, 0)
}

func TestTargetManager(t *testing.T) {
	suite.Run(t, new(TargetManagerSuite))
}
