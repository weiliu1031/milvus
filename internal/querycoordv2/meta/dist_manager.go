package meta

type DistributionManager struct {
	*SegmentDistManager
	*ChannelDistManager
}

func NewDistributionManager() *DistributionManager {
	return &DistributionManager{
		SegmentDistManager: NewSegmentDistManager(),
		ChannelDistManager: NewChannelDistManager(),
	}
}
