package task

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

// packSegmentLoadInfo packs SegmentLoadInfo for given segment,
// packs with index if withIndex is true, this fetch indexes from IndexCoord
func packSegmentLoadInfo(segment *datapb.SegmentInfo, indexes []*querypb.FieldIndexInfo) *querypb.SegmentLoadInfo {
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:     segment.ID,
		PartitionID:   segment.PartitionID,
		CollectionID:  segment.CollectionID,
		BinlogPaths:   segment.Binlogs,
		NumOfRows:     segment.NumOfRows,
		Statslogs:     segment.Statslogs,
		Deltalogs:     segment.Deltalogs,
		InsertChannel: segment.InsertChannel,
		IndexInfos:    indexes,
	}
	loadInfo.SegmentSize = calculateSegmentSize(loadInfo)
	return loadInfo
}

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
	}
}

func calculateSegmentSize(segmentLoadInfo *querypb.SegmentLoadInfo) int64 {
	segmentSize := int64(0)

	fieldIndex := make(map[int64]*querypb.FieldIndexInfo)
	for _, index := range segmentLoadInfo.IndexInfos {
		if index.EnableIndex {
			fieldID := index.FieldID
			fieldIndex[fieldID] = index
		}
	}

	for _, fieldBinlog := range segmentLoadInfo.BinlogPaths {
		fieldID := fieldBinlog.FieldID
		if index, ok := fieldIndex[fieldID]; ok {
			segmentSize += index.IndexSize
		} else {
			segmentSize += getFieldSizeFromBinlog(fieldBinlog)
		}
	}

	// Get size of state data
	for _, fieldBinlog := range segmentLoadInfo.Statslogs {
		segmentSize += getFieldSizeFromBinlog(fieldBinlog)
	}

	// Get size of delete data
	for _, fieldBinlog := range segmentLoadInfo.Deltalogs {
		segmentSize += getFieldSizeFromBinlog(fieldBinlog)
	}

	return segmentSize
}

func getFieldSizeFromBinlog(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.LogSize
	}

	return fieldSize
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

func mergeDmChannelInfo(infos []*datapb.VchannelInfo) *meta.DmChannel {
	var dmChannel *meta.DmChannel

	for _, info := range infos {
		if dmChannel == nil {
			dmChannel = meta.DmChannelFromVChannel(info)
			continue
		}

		if info.SeekPosition.GetTimestamp() < dmChannel.SeekPosition.GetTimestamp() {
			dmChannel.SeekPosition = info.SeekPosition
		}
		dmChannel.DroppedSegmentIds = append(dmChannel.DroppedSegmentIds, info.DroppedSegmentIds...)
		dmChannel.UnflushedSegmentIds = append(dmChannel.UnflushedSegmentIds, info.UnflushedSegmentIds...)
		dmChannel.FlushedSegmentIds = append(dmChannel.FlushedSegmentIds, info.FlushedSegmentIds...)
	}

	return dmChannel
}

func packSubDeltaChannelRequest(task *ChannelTask, action Action, collection *meta.Collection, channel *meta.DeltaChannel) *querypb.WatchDeltaChannelsRequest {
	return &querypb.WatchDeltaChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDeltaChannels,
			MsgID:   task.SourceID(),
		},

		CollectionID: task.CollectionID(),
		Infos:        []*datapb.VchannelInfo{channel.VchannelInfo},
		ReplicaId:    task.ReplicaID(),
		NodeID:       action.Node(),
	}
}

func spawnDeltaChannel(info *datapb.VchannelInfo) (*meta.DeltaChannel, error) {
	channelName, err := funcutil.ConvertChannelName(info.ChannelName, utils.Params.CommonCfg.RootCoordDml, utils.Params.CommonCfg.RootCoordDelta)
	if err != nil {
		return nil, err
	}
	channel := proto.Clone(info).(*datapb.VchannelInfo)
	channel.ChannelName = channelName
	channel.UnflushedSegmentIds = nil
	channel.FlushedSegmentIds = nil
	channel.DroppedSegmentIds = nil
	return meta.DeltaChannelFromVChannel(channel), nil
}

func mergeDeltaChannelInfo(infos []*datapb.VchannelInfo) *meta.DeltaChannel {
	var deltaChannel *meta.DeltaChannel

	for _, info := range infos {
		if deltaChannel == nil || deltaChannel.SeekPosition.GetTimestamp() > info.SeekPosition.GetTimestamp() {
			deltaChannel = meta.DeltaChannelFromVChannel(info)
		}
	}

	return deltaChannel
}

func getShardLeader(replicaMgr *meta.ReplicaManager, distMgr *meta.DistributionManager, collectionID, nodeID int64, channel string) (int64, bool) {
	replica := replicaMgr.GetByCollectionAndNode(collectionID, nodeID)
	if replica == nil {
		return 0, false
	}
	return distMgr.GetShardLeader(replica, channel)
}
