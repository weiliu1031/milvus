package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type SegmentChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	broker    *meta.CoordinatorBroker
	nodeMgr   *session.NodeManager
	balancer  balance.Balance
}

func NewSegmentChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	broker *meta.CoordinatorBroker,
	nodeMgr *session.NodeManager,
) *SegmentChecker {
	return &SegmentChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		broker:    broker,
		nodeMgr:   nodeMgr,
	}
}

func (c *SegmentChecker) Description() string {
	return "SegmentChecker checks the lack of segments, or some segments are redundant"
}

func (c *SegmentChecker) Check(ctx context.Context) []task.Task {
	collections := c.meta.CollectionManager.GetAllCollections()
	partitions := c.meta.CollectionManager.GetAllPartitions()
	collectionIDs := collect(collections, partitions)
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		replicas := c.meta.ReplicaManager.GetByCollection(cid)
		for _, r := range replicas {
			tasks = append(tasks, c.checkReplica(r)...)
		}
	}
	return tasks
}

func collect(collections []*meta.Collection, partitions []*meta.Partition) []int64 {
	var ret []int64
	m := make(map[int64]struct{})
	ret, m = ids(collections, ret, m)
	ret, m = ids(partitions, ret, m)
	return ret
}

func ids[E interface{ GetCollectionID() int64 }](elems []E, res []int64, filter map[int64]struct{}) ([]int64, map[int64]struct{}) {
	for _, e := range elems {
		id := e.GetCollectionID()
		if _, ok := filter[id]; ok {
			continue
		}
		res = append(res, id)
		filter[id] = struct{}{}
	}
	return res, filter
}

func (c *SegmentChecker) checkReplica(replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)
	targets := c.targetMgr.GetSegmentsByCollection(replica.CollectionID)
	dists := c.getSegmentsDist(replica)

	// compare with targets to find the lack and redundancy of segments
	lacks, redundancies := diffSet(targets, dists)
	tasks := createLoadTasks(c.balancer, lacks, replica)
	ret = append(ret, tasks...)

	tasks = createReduceTasks(redundancies)
	ret = append(ret, tasks...)

	// compare inner dists to find repeated loaded segments
	redundancies = findRepeatedSegments(dists)
	tasks = createReduceTasks(redundancies)
	ret = append(ret, tasks...)
	return ret
}

func (c *SegmentChecker) getSegmentsDist(replica *meta.Replica) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	for _, node := range replica.Nodes.Collect() {
		ret = append(ret, c.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, node)...)
	}
	return ret
}

func diffSet(targets []*datapb.SegmentInfo, dists []*meta.Segment) (lacks []*datapb.SegmentInfo, redundancies []*meta.Segment) {
	distMap := make(map[int64]struct{})
	targetMap := make(map[int64]struct{})
	for _, s := range targets {
		targetMap[s.GetID()] = struct{}{}
	}
	for _, s := range dists {
		distMap[s.GetID()] = struct{}{}
		if _, ok := targetMap[s.GetID()]; !ok {
			redundancies = append(redundancies, s)
		}
	}
	for _, s := range targets {
		if _, ok := distMap[s.GetID()]; !ok {
			lacks = append(lacks, s)
		}
	}
	return
}

func findRepeatedSegments(dists []*meta.Segment) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	versions := make(map[int64]*meta.Segment)
	for _, s := range dists {
		maxVer, ok := versions[s.GetID()]
		if !ok {
			versions[s.GetID()] = s
			continue
		}
		if maxVer.Version <= s.Version {
			ret = append(ret, maxVer)
			versions[s.GetID()] = s
		} else {
			ret = append(ret, s)
		}
	}
	return ret
}

func createLoadTasks(balancer balance.Balance, segments []*datapb.SegmentInfo, replica *meta.Replica) []task.Task {
	// TODO(sunby)
	return nil
}

func createReduceTasks(segments []*meta.Segment) []task.Task {
	// TODO(sunby)
	return nil

}
