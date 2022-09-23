package utils

import (
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// GetStreamingSegmentDiff get streaming segment diff between leader view and target
func GetStreamingSegmentDiff(targetMgr *meta.TargetManager,
	distMgr *meta.DistributionManager,
	metaInfo *meta.Meta,
	collectionID int64,
	replicaID int64) (toLoad []*datapb.SegmentInfo, toRelease []*meta.Segment) {
	dist := GetStreamingSegmentsDist(distMgr, metaInfo, replicaID)
	distMap := typeutil.NewUniqueSet()
	for _, s := range dist {
		distMap.Insert(s.GetID())
	}

	nextTargetMap := targetMgr.GetStreamingSegmentsByCollection(collectionID, meta.NextTarget)
	currentTargetMap := targetMgr.GetStreamingSegmentsByCollection(collectionID, meta.CurrentTarget)

	//get segment which exist on next target, but not on dist
	for segmentID, segment := range nextTargetMap {
		if !distMap.Contain(segmentID) {
			toLoad = append(toLoad, segment)
		}
	}

	nextTargetChannelMap := targetMgr.GetDmChannelsByCollection(collectionID, meta.NextTarget)

	// get segment which exist on dist, but not on current target and next target
	for _, segment := range dist {
		_, existOnCurrent := currentTargetMap[segment.GetID()]
		_, existOnNext := nextTargetMap[segment.GetID()]

		if !existOnNext && !existOnCurrent {
			if channel, ok := nextTargetChannelMap[segment.InsertChannel]; ok {
				timestampInSegment := segment.GetStartPosition().GetTimestamp()
				timestampInTarget := channel.GetSeekPosition().GetTimestamp()
				// filter toRelease which seekPosition is newer than next target dmChannel
				if timestampInSegment < timestampInTarget {
					toRelease = append(toRelease, segment)
				}
			}
		}
	}

	return
}

func GetStreamingSegmentsDist(distMgr *meta.DistributionManager, metaInfo *meta.Meta, replicaID int64) map[int64]*meta.Segment {
	segments := make(map[int64]*meta.Segment, 0)
	replica := metaInfo.Get(replicaID)
	for _, node := range replica.Nodes.Collect() {
		segmentsOnNodes := distMgr.LeaderViewManager.GetGrowingSegmentDistByCollectionAndNode(replica.CollectionID, node)
		for k, v := range segmentsOnNodes {
			segments[k] = v
		}
	}

	return segments
}

// GetHistoricalSegmentDiff get historical segment diff between target and dist
func GetHistoricalSegmentDiff(targetMgr *meta.TargetManager,
	distMgr *meta.DistributionManager,
	metaInfo *meta.Meta,
	collectionID int64,
	replicaID int64,
	partitionIDs ...int64) (toLoad []*datapb.SegmentInfo, toRelease []*meta.Segment) {
	dist := GetHistoricalSegmentsDist(distMgr, metaInfo, replicaID)
	distMap := typeutil.NewUniqueSet()
	for _, s := range dist {
		distMap.Insert(s.GetID())
	}

	nextTargetMap := targetMgr.GetHistoricalSegmentsByCollection(collectionID, meta.NextTarget)
	currentTargetMap := targetMgr.GetHistoricalSegmentsByCollection(collectionID, meta.CurrentTarget)

	//get segment which exist on next target, but not on dist
	for segmentID, segment := range nextTargetMap {
		if !distMap.Contain(segmentID) {
			toLoad = append(toLoad, segment)
		}
	}

	// get segment which exist on dist, but not on current target and next target
	for _, segment := range dist {
		_, existOnCurrent := currentTargetMap[segment.GetID()]
		_, existOnNext := nextTargetMap[segment.GetID()]

		if !existOnNext && !existOnCurrent {
			toRelease = append(toRelease, segment)
		}
	}

	return
}

func GetHistoricalSegmentsDist(distMgr *meta.DistributionManager, metaInfo *meta.Meta, replicaID int64) []*meta.Segment {
	replica := metaInfo.Get(replicaID)
	ret := make([]*meta.Segment, 0)
	for _, node := range replica.Nodes.Collect() {
		ret = append(ret, distMgr.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, node)...)
	}
	return ret
}

// GetDmChannelDiff get channel diff between target and dist
func GetDmChannelDiff(targetMgr *meta.TargetManager,
	distMgr *meta.DistributionManager,
	metaInfo *meta.Meta,
	collectionID int64,
	replicaID int64) (toLoad, toRelease []*meta.DmChannel) {
	dist := GetChannelDist(distMgr, metaInfo, replicaID)
	distMap := make(map[string]struct{})
	for _, ch := range dist {
		distMap[ch.GetChannelName()] = struct{}{}
	}

	nextTargetMap := targetMgr.GetDmChannelsByCollection(collectionID, meta.NextTarget)
	currentTargetMap := targetMgr.GetDmChannelsByCollection(collectionID, meta.CurrentTarget)

	// get channels which exists on dist, but not exist on current and next
	for _, ch := range dist {
		_, existOnCurrent := currentTargetMap[ch.GetChannelName()]
		_, existOnNext := nextTargetMap[ch.GetChannelName()]
		if !existOnNext && !existOnCurrent {
			toRelease = append(toRelease, ch)
		}
	}

	//get channels which exists on next target, but not on dist
	for name, channel := range nextTargetMap {
		_, existOnDist := distMap[name]
		if !existOnDist {
			toLoad = append(toLoad, channel)
		}
	}

	return
}

func GetChannelDist(distMgr *meta.DistributionManager, metaInfo *meta.Meta, replicaID int64) []*meta.DmChannel {
	replica := metaInfo.Get(replicaID)
	dist := make([]*meta.DmChannel, 0)
	for _, nodeID := range replica.Nodes.Collect() {
		dist = append(dist, distMgr.ChannelDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nodeID)...)
	}
	return dist
}

// IsNextTargetValid check whether collection's next target resource is available
// todo: access minio ton judge whether this could be load successfully
func IsNextTargetValid(collectionID int64) bool {
	return true
}

func IsNextTargetReadyForCollection(targetMgr *meta.TargetManager, distMgr *meta.DistributionManager, metaInfo *meta.Meta, collectionID int64) bool {
	replicaNum := len(metaInfo.ReplicaManager.GetByCollection(collectionID))

	// check channel first
	channelNames := targetMgr.GetDmChannelsByCollection(collectionID, meta.NextTarget)
	if len(channelNames) == 0 {
		// next target is empty, no need to update
		return false
	}
	for name := range channelNames {
		if replicaNum > len(distMgr.GetChannelDist(name)) {
			return false
		}
	}
	// then check streaming segment
	streamingSegments := targetMgr.GetStreamingSegmentsByCollection(collectionID, meta.NextTarget)
	for ID := range streamingSegments {
		if replicaNum > len(distMgr.GetGrowingSegmentDist(ID)) {
			return false
		}
	}

	// and last check historical segment
	historicalSegments := targetMgr.GetHistoricalSegmentsByCollection(collectionID, meta.NextTarget)
	for ID := range historicalSegments {
		if replicaNum > len(distMgr.GetSealedSegmentDist(ID)) {
			return false
		}
	}

	return true
}

func IsNextTargetExist(mgr *meta.TargetManager, collectionID int64) bool {
	newHistoricalSegments := mgr.GetHistoricalSegmentsByCollection(collectionID, meta.NextTarget)
	newChannels := mgr.GetDmChannelsByCollection(collectionID, meta.NextTarget)

	if len(newHistoricalSegments) > 0 || len(newChannels) > 0 {
		return false
	}

	return true
}
