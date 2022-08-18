package observers

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
)

const interval = 1 * time.Second

// LeaderObserver is to sync the distribution with leader
type LeaderObserver struct {
	wg      sync.WaitGroup
	closeCh chan struct{}
	dist    *meta.DistributionManager
	meta    *meta.Meta
	target  *meta.TargetManager
}

func (o *LeaderObserver) Start(ctx context.Context) {
	go func() {
		o.wg.Add(1)
		defer o.wg.Done()
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-o.closeCh:
				log.Info("stop leader observer")
				return
			case <-ctx.Done():
				log.Info("stop leader observer due to ctx done")
				return
			case <-ticker.C:
				o.observe()
			}
		}
	}()
}

func (o *LeaderObserver) Stop() {
	close(o.closeCh)
	o.wg.Wait()
}

func (o *LeaderObserver) observe() {
	o.observeSegmentsDist()
}

func (o *LeaderObserver) observeSegmentsDist() {
	collections := o.meta.CollectionManager.GetAllCollections()
	partitions := o.meta.CollectionManager.GetAllPartitions()
	ids := utils.Collect(collections, partitions)
	for _, cid := range ids {
		o.observeCollection(cid)
	}
}

func (o *LeaderObserver) observeCollection(collection int64) {
	replicas := o.meta.ReplicaManager.GetByCollection(collection)
	for _, replica := range replicas {
		leaders := o.dist.ChannelDistManager.GetShardLeadersByReplica(replica)
		for ch, leaderID := range leaders {
			leaderViews := o.dist.LeaderViewManager.GetLeaderShardView(leaderID, ch)
			dists := o.dist.SegmentDistManager.GetByShard(ch)
			needLoaded, needRemoved := o.findNeedLoadedSegments(leaderViews, dists),
				o.findNeedRemovedSegments(leaderViews, dists)
			o.sync(append(needLoaded, needRemoved...))
		}
	}
}

func (o *LeaderObserver) findNeedLoadedSegments(leaderView *meta.LeaderView, dists []*meta.Segment) []*viewSync {
	ret := make([]*viewSync, 0)
	dists = utils.FindMaxVersionSegments(dists)
	for _, s := range dists {
		node, ok := leaderView.Segments[s.GetID()]
		consistentOnLeader := ok && node == s.Node
		if consistentOnLeader || !o.target.ContainSegment(s.GetID()) {
			continue
		}
		ret = append(ret, &viewSync{
			dType:     set,
			segmentID: s.GetID(),
			node:      node,
		})
	}
	return ret
}

func (o *LeaderObserver) findNeedRemovedSegments(leaderView *meta.LeaderView, dists []*meta.Segment) []*viewSync {
	ret := make([]*viewSync, 0)
	distMap := make(map[int64]struct{})
	for _, s := range dists {
		distMap[s.GetID()] = struct{}{}
	}
	for sid := range leaderView.Segments {
		_, ok := distMap[sid]
		if ok || o.target.ContainSegment(sid) {
			continue
		}
		ret = append(ret, &viewSync{
			dType:     remove,
			segmentID: sid,
		})
	}
	return ret
}

func (o *LeaderObserver) sync(diffs []*viewSync) {
	// TODO(sunby)
}

func NewLeaderObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
) *LeaderObserver {
	return &LeaderObserver{
		closeCh: make(chan struct{}),
		dist:    dist,
		meta:    meta,
		target:  targetMgr,
	}
}

const (
	remove = 1 // to remove segments on leader
	set    = 2 // to set version for segments on leader
)

type viewSync struct {
	dType     int
	segmentID int64
	node      int64
}
