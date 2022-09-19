package utils

import "github.com/milvus-io/milvus/internal/querycoordv2/meta"

func FindRepeatedChannels(distMgr *meta.DistributionManager,
	_meta *meta.Meta,
	replicaID int64) []*meta.DmChannel {
	dist := GetChannelDist(distMgr, _meta, replicaID)

	ret := make([]*meta.DmChannel, 0)
	versionsMap := make(map[string]*meta.DmChannel)
	for _, ch := range dist {
		maxVer, ok := versionsMap[ch.GetChannelName()]
		if !ok {
			versionsMap[ch.GetChannelName()] = ch
			continue
		}
		if maxVer.Version <= ch.Version {
			ret = append(ret, maxVer)
			versionsMap[ch.GetChannelName()] = ch
		} else {
			ret = append(ret, ch)
		}
	}
	return ret
}

func FindRepeatedHistoricalSegments(distMgr *meta.DistributionManager,
	_meta *meta.Meta,
	replicaID int64) []*meta.Segment {

	dist := GetHistoricalSegmentsDist(distMgr, _meta, replicaID)
	segments := make([]*meta.Segment, 0)
	versions := make(map[int64]*meta.Segment)
	for _, s := range dist {
		maxVer, ok := versions[s.GetID()]
		if !ok {
			versions[s.GetID()] = s
			continue
		}
		if maxVer.Version <= s.Version {
			segments = append(segments, maxVer)
			versions[s.GetID()] = s
		} else {
			segments = append(segments, s)
		}
	}

	return segments
}
