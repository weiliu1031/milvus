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
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	brokerRpcTimeout = 5 * time.Second
)

// Params is param table of query coordinator
var Params paramtable.ComponentParam

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

func (broker *CoordinatorBroker) AcquireSegmentsReferLock(ctx context.Context, segmentIDs []UniqueID) error {
	ctx, cancel := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel()
	acquireSegLockReq := &datapb.AcquireSegmentLockRequest{
		SegmentIDs: segmentIDs,
		NodeID:     Params.QueryCoordCfg.GetNodeID(),
	}
	status, err := broker.dataCoord.AcquireSegmentLock(ctx, acquireSegLockReq)
	if err != nil {
		log.Error("QueryCoord acquire the segment reference lock error", zap.Int64s("segIDs", segmentIDs),
			zap.Error(err))
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("QueryCoord acquire the segment reference lock error", zap.Int64s("segIDs", segmentIDs),
			zap.String("failed reason", status.Reason))
		return fmt.Errorf(status.Reason)
	}

	return nil
}

func (broker *CoordinatorBroker) ReleaseSegmentReferLock(ctx context.Context, segmentIDs []UniqueID) error {
	ctx, cancel := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel()

	releaseSegReferLockReq := &datapb.ReleaseSegmentLockRequest{
		NodeID:     Params.QueryCoordCfg.GetNodeID(),
		SegmentIDs: segmentIDs,
	}

	if err := retry.Do(ctx, func() error {
		status, err := broker.dataCoord.ReleaseSegmentLock(ctx, releaseSegReferLockReq)
		if err != nil {
			log.Error("QueryCoord release reference lock on segments failed", zap.Int64s("segmentIDs", segmentIDs),
				zap.Error(err))
			return err
		}

		if status.ErrorCode != commonpb.ErrorCode_Success {
			log.Error("QueryCoord release reference lock on segments failed", zap.Int64s("segmentIDs", segmentIDs),
				zap.String("failed reason", status.Reason))
			return errors.New(status.Reason)
		}
		return nil
	}, retry.Attempts(100)); err != nil {
		return err
	}

	return nil
}

func (broker *CoordinatorBroker) getIndexInfo(ctx context.Context, collectionID UniqueID, segmentID UniqueID) ([]*querypb.FieldIndexInfo, error) {
	segmentIndexInfos, err := broker.getFullIndexInfos(ctx, collectionID, []UniqueID{segmentID})
	if err != nil {
		return nil, err
	}
	if infos, ok := segmentIndexInfos[segmentID]; ok {
		return infos, nil
	}
	return nil, fmt.Errorf("failed to get segment index infos, collection: %d, segment: %d", collectionID, segmentID)
}

// return: segment_id -> segment_index_infos
func (broker *CoordinatorBroker) getFullIndexInfos(ctx context.Context, collectionID UniqueID, segmentIDs []UniqueID) (map[UniqueID][]*querypb.FieldIndexInfo, error) {
	resp, err := broker.describeSegments(ctx, collectionID, segmentIDs)
	if err != nil {
		return nil, err
	}

	ret := make(map[UniqueID][]*querypb.FieldIndexInfo)
	for _, segmentID := range segmentIDs {
		infos, ok := resp.GetSegmentInfos()[segmentID]
		if !ok {
			log.Warn("segment not found",
				zap.Int64("collection", collectionID),
				zap.Int64("segment", segmentID))
			return nil, fmt.Errorf("segment not found, collection: %d, segment: %d", collectionID, segmentID)
		}

		if _, ok := ret[segmentID]; !ok {
			ret[segmentID] = make([]*querypb.FieldIndexInfo, 0, len(infos.IndexInfos))
		}

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
				ret[segmentID] = append(ret[segmentID], indexInfo)
				continue
			}

			paths, err := broker.getIndexFilePaths(ctx, info.BuildID)
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
				extra, err := broker.loadIndexExtraInfo(ctx, paths[0])
				if err != nil {
					log.Error("failed to load index extra info",
						zap.Int64("index build id", info.BuildID),
						zap.Error(err))
					return nil, err
				}
				indexInfo.IndexName = extra.indexName
				indexInfo.IndexParams = extra.indexParams
			}
			ret[segmentID] = append(ret[segmentID], indexInfo)
		}
	}

	return ret, nil
}

func (broker *CoordinatorBroker) describeSegments(ctx context.Context, collectionID UniqueID, segmentIDs []UniqueID) (*rootcoordpb.DescribeSegmentsResponse, error) {
	resp, err := broker.rootCoord.DescribeSegments(ctx, &rootcoordpb.DescribeSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeSegments,
		},
		CollectionID: collectionID,
		SegmentIDs:   segmentIDs,
	})
	if err != nil {
		log.Error("failed to describe segments",
			zap.Int64("collection", collectionID),
			zap.Int64s("segments", segmentIDs),
			zap.Error(err))
		return nil, err
	}

	log.Info("describe segments successfully",
		zap.Int64("collection", collectionID),
		zap.Int64s("segments", segmentIDs))

	return resp, nil
}

func (broker *CoordinatorBroker) getIndexFilePaths(ctx context.Context, buildID int64) ([]*indexpb.IndexFilePathInfo, error) {
	indexFilePathRequest := &indexpb.GetIndexFilePathsRequest{
		IndexBuildIDs: []UniqueID{buildID},
	}
	ctx3, cancel3 := context.WithTimeout(ctx, brokerRpcTimeout)
	defer cancel3()
	pathResponse, err := broker.indexCoord.GetIndexFilePaths(ctx3, indexFilePathRequest)
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

func (broker *CoordinatorBroker) parseIndexInfo(ctx context.Context, segmentID UniqueID, indexInfo *querypb.FieldIndexInfo) error {
	if !indexInfo.EnableIndex {
		log.Debug(fmt.Sprintf("fieldID %d of segment %d don't has index", indexInfo.FieldID, segmentID))
		return nil
	}
	buildID := indexInfo.BuildID
	indexFilePathInfos, err := broker.getIndexFilePaths(ctx, buildID)
	if err != nil {
		return err
	}

	if len(indexFilePathInfos) != 1 {
		err = fmt.Errorf("illegal index file paths, there should be only one vector column,  segmentID = %d, fieldID = %d, buildID = %d", segmentID, indexInfo.FieldID, buildID)
		log.Error(err.Error())
		return err
	}

	fieldPathInfo := indexFilePathInfos[0]
	if len(fieldPathInfo.IndexFilePaths) == 0 {
		err = fmt.Errorf("empty index paths, segmentID = %d, fieldID = %d, buildID = %d", segmentID, indexInfo.FieldID, buildID)
		log.Error(err.Error())
		return err
	}

	indexInfo.IndexFilePaths = fieldPathInfo.IndexFilePaths
	indexInfo.IndexSize = int64(fieldPathInfo.SerializedSize)

	log.Debug("get indexFilePath info from indexCoord success", zap.Int64("segmentID", segmentID), zap.Int64("fieldID", indexInfo.FieldID), zap.Int64("buildID", buildID), zap.Strings("indexPaths", fieldPathInfo.IndexFilePaths))

	indexCodec := storage.NewIndexFileBinlogCodec()
	for _, indexFilePath := range fieldPathInfo.IndexFilePaths {
		// get index params when detecting indexParamPrefix
		if path.Base(indexFilePath) == storage.IndexParamsKey {
			indexPiece, err := broker.cm.Read(indexFilePath)
			if err != nil {
				log.Error("load index params file failed",
					zap.Int64("segmentID", segmentID),
					zap.Int64("fieldID", indexInfo.FieldID),
					zap.Int64("indexBuildID", buildID),
					zap.String("index params filePath", indexFilePath),
					zap.Error(err))
				return err
			}
			_, indexParams, indexName, indexID, err := indexCodec.Deserialize([]*storage.Blob{{Key: storage.IndexParamsKey, Value: indexPiece}})
			if err != nil {
				log.Error("deserialize index params file failed",
					zap.Int64("segmentID", segmentID),
					zap.Int64("fieldID", indexInfo.FieldID),
					zap.Int64("indexBuildID", buildID),
					zap.String("index params filePath", indexFilePath),
					zap.Error(err))
				return err
			}
			if len(indexParams) <= 0 {
				err = fmt.Errorf("cannot find index param, segmentID = %d, fieldID = %d, buildID = %d, indexFilePath = %s", segmentID, indexInfo.FieldID, buildID, indexFilePath)
				log.Error(err.Error())
				return err
			}
			indexInfo.IndexName = indexName
			indexInfo.IndexID = indexID
			indexInfo.IndexParams = funcutil.Map2KeyValuePair(indexParams)
			break
		}
	}

	if len(indexInfo.IndexParams) == 0 {
		err = fmt.Errorf("no index params in Index file, segmentID = %d, fieldID = %d, buildID = %d, indexPaths = %v", segmentID, indexInfo.FieldID, buildID, fieldPathInfo.IndexFilePaths)
		log.Error(err.Error())
		return err
	}

	log.Info("set index info  success", zap.Int64("segmentID", segmentID), zap.Int64("fieldID", indexInfo.FieldID), zap.Int64("buildID", buildID))

	return nil
}

// Better to let index params key appear in the file paths first.
func (broker *CoordinatorBroker) loadIndexExtraInfo(ctx context.Context, fieldPathInfo *indexpb.IndexFilePathInfo) (*extraIndexInfo, error) {
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
