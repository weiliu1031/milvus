package task

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
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

func packLoadSegmentRequest(task *SegmentTask, action Action, schema *schemapb.CollectionSchema, segment *datapb.SegmentInfo, indexes []*querypb.FieldIndexInfo) *querypb.LoadSegmentsRequest {
	return &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadSegments,
			MsgID:   task.MsgID(),
		},
		Infos:  []*querypb.SegmentLoadInfo{packSegmentLoadInfo(segment, indexes)},
		Schema: schema,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     task.loadType,
			CollectionID: task.CollectionID(),
			PartitionIDs: []int64{segment.PartitionID},
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
