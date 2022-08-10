package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/samber/lo"
)

func (s *Server) registerTargets(ctx context.Context,
	targetMgr *meta.TargetManager,
	broker *meta.CoordinatorBroker,
	collection int64, partitions ...int64) error {
	var dmChannels map[string][]*datapb.VchannelInfo

	for _, partitionID := range partitions {
		vChannelInfos, binlogs, err := s.broker.GetRecoveryInfo(ctx, collection, partitionID)
		if err != nil {
			return err
		}

		// Register segments
		for _, segmentBinlogs := range binlogs {
			s.targetMgr.AddSegment(utils.SegmentBinlogs2SegmentInfo(
				collection,
				partitionID,
				segmentBinlogs))
		}

		for _, info := range vChannelInfos {
			channelName := info.GetChannelName()
			dmChannels[channelName] = append(dmChannels[channelName], info)
		}
	}
	// Merge and register channels
	for _, channels := range dmChannels {
		dmChannel := utils.MergeDmChannelInfo(channels)
		s.targetMgr.AddDmChannel(dmChannel)
	}
	return nil
}

// checkAnyReplicaAvailable checks if the collection has enough distinct available shards. These shards
// may come from different replica group. We only need these shards to form a replica that serves query
// requests.
func (s *Server) checkAnyReplicaAvailable(collectionID int64) bool {
	for _, replica := range s.meta.ReplicaManager.GetByCollection(collectionID) {
		isAvailable := true
		for node := range replica.Nodes {
			if s.nodeMgr.Get(node) == nil {
				isAvailable = false
				break
			}
		}
		if isAvailable {
			return true
		}
	}
	return false
}

func (s *Server) getAllSegmentInfo() []*querypb.SegmentInfo {
	segments := s.dist.SegmentDistManager.GetAll()
	infos := make(map[int64]*querypb.SegmentInfo)
	for _, segment := range segments {
		info, ok := infos[segment.GetID()]
		if !ok {
			info = &querypb.SegmentInfo{}
			infos[segment.GetID()] = info
		}
		utils.MergeMetaSegmentIntoSegmentInfo(info, segment)
	}

	return lo.Values(infos)
}
