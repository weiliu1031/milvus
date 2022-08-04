package task

import (
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

func packLoadSegmentRequest(task *SegmentTask, action Action, collection *meta.Collection, loadInfo *querypb.SegmentLoadInfo) *querypb.LoadSegmentsRequest {
	return &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadSegments,
			MsgID:   task.SourceID(),
		},
		Infos:  []*querypb.SegmentLoadInfo{loadInfo},
		Schema: collection.Schema,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     collection.LoadType,
			CollectionID: collection.ID,
			PartitionIDs: collection.Partitions,
		},
		CollectionID: task.CollectionID(),
		ReplicaID:    task.ReplicaID(),
		DstNodeID:    action.Node(),
		Version:      time.Now().UnixNano(),
	}
}

func packReleaseSegmentRequest(task *SegmentTask, action Action) *querypb.ReleaseSegmentsRequest {
	return &querypb.ReleaseSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseSegments,
			MsgID:   task.SourceID(),
		},

		NodeID:       action.Node(),
		CollectionID: task.CollectionID(),
		SegmentIDs:   []int64{task.SegmentID()},
		Scope:        querypb.DataScope_All,
	}
}

func packSubDmChannelRequest(task *ChannelTask, action Action, collection *meta.Collection, channel *meta.DmChannel) *querypb.WatchDmChannelsRequest {
	return &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDmChannels,
			MsgID:   task.SourceID(),
		},
		NodeID:       action.Node(),
		CollectionID: task.CollectionID(),
		Infos:        []*datapb.VchannelInfo{channel.VchannelInfo},
		Schema:       collection.Schema,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     collection.LoadType,
			CollectionID: collection.ID,
			PartitionIDs: collection.Partitions,
		},
		ReplicaID: task.ReplicaID(),
	}
}

func packUnsubDmChannelRequest(task *ChannelTask, action Action) *querypb.UnsubDmChannelRequest {
	return &querypb.UnsubDmChannelRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_UnsubDmChannel,
			MsgID:   task.SourceID(),
		},
		NodeID:       action.Node(),
		CollectionID: task.CollectionID(),
		ChannelName:  task.Channel(),
	}
}

func getShardLeader(replicaMgr *meta.ReplicaManager, distMgr *meta.DistributionManager, collectionID, nodeID int64, channel string) (int64, bool) {
	replica := replicaMgr.GetByCollectionAndNode(collectionID, nodeID)
	if replica == nil {
		return 0, false
	}
	return distMgr.GetShardLeader(replica, channel)
}
