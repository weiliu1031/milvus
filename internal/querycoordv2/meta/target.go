package meta

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

// CollectionTarget partition target is immutable,
type CollectionTarget struct {
	segments   map[int64]*datapb.SegmentInfo
	dmChannels map[string]*DmChannel
}

func NewCollectionTarget(segments map[int64]*datapb.SegmentInfo, dmChannels map[string]*DmChannel) *CollectionTarget {
	return &CollectionTarget{
		segments:   segments,
		dmChannels: dmChannels,
	}
}

func (p *CollectionTarget) GetAllSegments() map[int64]*datapb.SegmentInfo {
	return p.segments
}

func (p *CollectionTarget) GetAllDmChannels() map[string]*DmChannel {
	return p.dmChannels
}

func (p *CollectionTarget) GetAllSegmentIDs() []int64 {
	return lo.MapToSlice(p.segments, func(k int64, v *datapb.SegmentInfo) int64 { return k })
}

func (p *CollectionTarget) GetAllDmChannelNames() []string {
	return lo.MapToSlice(p.dmChannels, func(k string, v *DmChannel) string { return k })
}

func (p *CollectionTarget) isEmpty() bool {
	return len(p.dmChannels) == 0
}

type target struct {
	// just maintain target at collection level
	collectionTargetMap map[int64]*CollectionTarget
}

func NewTarget() *target {
	return &target{
		collectionTargetMap: make(map[int64]*CollectionTarget),
	}
}

func (t *target) updateCollectionTarget(collectionID int64, target *CollectionTarget) {
	t.collectionTargetMap[collectionID] = target
}

func (t *target) removeCollectionTarget(collectionID int64) {
	delete(t.collectionTargetMap, collectionID)
}

func (t *target) getCollectionTarget(collectionID int64) *CollectionTarget {
	return t.collectionTargetMap[collectionID]
}
