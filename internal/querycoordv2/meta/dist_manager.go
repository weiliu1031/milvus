package meta

type distributionManager struct {
	*SegmentDistManager
	*ChannelDistManager
}

func newDistributionManager() *distributionManager {
	return &distributionManager{
		SegmentDistManager: NewSegmentDistManager(),
		ChannelDistManager: NewChannelDistManager(),
	}
}

type DistributionManager struct {
	*SegmentDistManager
	*ChannelDistManager
	LeaderDistribution *distributionManager
}

func NewDistributionManager() *DistributionManager {
	return &DistributionManager{
		SegmentDistManager: NewSegmentDistManager(),
		ChannelDistManager: NewChannelDistManager(),
		LeaderDistribution: newDistributionManager(),
	}
}
