package meta

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	brokerRpcTimeout = 5 * time.Second
)

type CoordinatorBroker struct {
	dataCoord  types.DataCoord
	rootCoord  types.RootCoord
	indexCoord types.IndexCoord

	cm storage.ChunkManager
}

func NewCoordinatorBroker(
	dataCoord types.DataCoord,
	rootCoord types.RootCoord,
	indexCoord types.IndexCoord,
	cm storage.ChunkManager) *CoordinatorBroker {
	return &CoordinatorBroker{
		dataCoord,
		rootCoord,
		indexCoord,
		cm,
	}
}

func (broker *CoordinatorBroker) GetCollectionSchema(ctx context.Context, collectionID UniqueID) (*schemapb.CollectionSchema, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel()

	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeCollection,
		},
		CollectionID: collectionID,
	}
	resp, err := broker.rootCoord.DescribeCollection(ctx, req)
	return resp.GetSchema(), err
}

func (broker *CoordinatorBroker) GetPartitions(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel()
	req := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
		},
		CollectionID: collectionID,
	}
	resp, err := broker.rootCoord.ShowPartitions(ctx, req)
	if err != nil {
		log.Error("showPartition failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(resp.Status.Reason)
		log.Error("showPartition failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}
	log.Info("show partition successfully", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", resp.PartitionIDs))

	return resp.PartitionIDs, nil
}

func (broker *CoordinatorBroker) GetRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel()

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_GetRecoveryInfo,
		},
		CollectionID: collectionID,
		PartitionID:  partitionID,
	}
	recoveryInfo, err := broker.dataCoord.GetRecoveryInfo(ctx, getRecoveryInfoRequest)
	if err != nil {
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}

	if recoveryInfo.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(recoveryInfo.Status.Reason)
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}
	log.Info("get recovery info successfully",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int("num channels", len(recoveryInfo.Channels)),
		zap.Int("num segments", len(recoveryInfo.Binlogs)))

	return recoveryInfo.Channels, recoveryInfo.Binlogs, nil
}

func (broker *CoordinatorBroker) GetSegmentInfo(ctx context.Context, segmentID UniqueID) (*datapb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel()

	req := &datapb.GetSegmentInfoRequest{
		SegmentIDs: []int64{segmentID},
	}
	resp, err := broker.dataCoord.GetSegmentInfo(ctx, req)
	if err != nil {
		log.Error("failed to get segment info from DataCoord",
			zap.Int64("segment-id", segmentID),
			zap.Error(err))
		return nil, err
	}

	if len(resp.Infos) == 0 {
		log.Warn("No such segment in DataCoord",
			zap.Int64("segment-id", segmentID))
		return nil, fmt.Errorf("no such segment in DataCoord")
	}

	return resp.GetInfos()[0], nil
}

func (broker *CoordinatorBroker) GetIndexInfo(ctx context.Context, collectionID UniqueID, segmentID UniqueID) ([]*querypb.FieldIndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel()

	resp, err := broker.rootCoord.DescribeSegments(ctx, &rootcoordpb.DescribeSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeSegments,
		},
		CollectionID: collectionID,
		SegmentIDs:   []int64{segmentID},
	})
	if err != nil {
		log.Error("failed to describe segments",
			zap.Int64("collection", collectionID),
			zap.Int64("segment", segmentID),
			zap.Error(err))
		return nil, err
	}

	infos, ok := resp.GetSegmentInfos()[segmentID]
	if !ok {
		log.Warn("segment not found",
			zap.Int64("collection", collectionID),
			zap.Int64("segment", segmentID))
		return nil, fmt.Errorf("segment not found, collection: %d, segment: %d", collectionID, segmentID)
	}

	indexes := make([]*querypb.FieldIndexInfo, 0)
	for _, info := range infos.IndexInfos {
		extraInfo, ok := infos.GetExtraIndexInfos()[info.IndexID]
		indexInfo := &querypb.FieldIndexInfo{
			FieldID:        info.FieldID,
			EnableIndex:    info.EnableIndex,
			IndexName:      "",
			IndexID:        info.IndexID,
			BuildID:        info.BuildID,
			IndexParams:    nil,
			IndexFilePaths: nil,
			IndexSize:      0,
		}

		if !info.EnableIndex {
			indexes = append(indexes, indexInfo)
			continue
		}

		paths, err := broker.GetIndexFilePaths(ctx, info.BuildID)
		//TODO:: returns partially successful index
		if err != nil {
			log.Warn("failed to get index file paths",
				zap.Int64("collection", collectionID),
				zap.Int64("segment", segmentID),
				zap.Int64("buildID", info.BuildID),
				zap.Error(err))
			return nil, err
		}

		if len(paths) <= 0 || len(paths[0].IndexFilePaths) <= 0 {
			log.Warn("index not ready", zap.Int64("index_build_id", info.BuildID))
			return nil, fmt.Errorf("index not ready, index build id: %d", info.BuildID)
		}

		indexInfo.IndexFilePaths = paths[0].IndexFilePaths
		indexInfo.IndexSize = int64(paths[0].SerializedSize)

		if ok {
			indexInfo.IndexName = extraInfo.IndexName
			indexInfo.IndexParams = extraInfo.IndexParams
		} else {
			// get index name, index params from binlog.
			extra, err := broker.LoadIndexExtraInfo(ctx, paths[0])
			if err != nil {
				log.Error("failed to load index extra info",
					zap.Int64("index build id", info.BuildID),
					zap.Error(err))
				return nil, err
			}
			indexInfo.IndexName = extra.indexName
			indexInfo.IndexParams = extra.indexParams
		}
		indexes = append(indexes, indexInfo)
	}

	return indexes, nil
}

func (broker *CoordinatorBroker) GetIndexFilePaths(ctx context.Context, buildID int64) ([]*indexpb.IndexFilePathInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel()

	indexFilePathRequest := &indexpb.GetIndexFilePathsRequest{
		IndexBuildIDs: []UniqueID{buildID},
	}
	pathResponse, err := broker.indexCoord.GetIndexFilePaths(ctx, indexFilePathRequest)
	if err != nil {
		log.Error("get index info from indexCoord failed",
			zap.Int64("indexBuildID", buildID),
			zap.Error(err))
		return nil, err
	}

	if pathResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = fmt.Errorf("get index info from indexCoord failed, buildID = %d, reason = %s", buildID, pathResponse.Status.Reason)
		log.Error(err.Error())
		return nil, err
	}
	log.Info("get index info from indexCoord successfully", zap.Int64("buildID", buildID))

	return pathResponse.FilePaths, nil
}

type extraIndexInfo struct {
	indexID        UniqueID
	indexName      string
	indexParams    []*commonpb.KeyValuePair
	indexSize      uint64
	indexFilePaths []string
}

func (broker *CoordinatorBroker) LoadIndexExtraInfo(ctx context.Context, fieldPathInfo *indexpb.IndexFilePathInfo) (*extraIndexInfo, error) {
	indexCodec := storage.NewIndexFileBinlogCodec()
	for _, indexFilePath := range fieldPathInfo.IndexFilePaths {
		// get index params when detecting indexParamPrefix
		if path.Base(indexFilePath) == storage.IndexParamsKey {
			content, err := broker.cm.MultiRead([]string{indexFilePath})
			if err != nil {
				return nil, err
			}

			if len(content) <= 0 {
				return nil, fmt.Errorf("failed to read index file binlog, path: %s", indexFilePath)
			}

			indexPiece := content[0]
			_, indexParams, indexName, _, err := indexCodec.Deserialize([]*storage.Blob{{Key: storage.IndexParamsKey, Value: indexPiece}})
			if err != nil {
				return nil, err
			}

			return &extraIndexInfo{
				indexName:   indexName,
				indexParams: funcutil.Map2KeyValuePair(indexParams),
			}, nil
		}
	}
	return nil, errors.New("failed to load index extra info")
}
