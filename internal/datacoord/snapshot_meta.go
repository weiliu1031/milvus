package datacoord

import (
	"context"
	"fmt"
	"slices"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type SnapshotDataInfo struct {
	snapshotInfo *datapb.SnapshotInfo
	SegmentIDs   typeutil.UniqueSet
	IndexIDs     typeutil.UniqueSet
}

type snapshotMeta struct {
	catalog metastore.DataCoordCatalog

	// snapshot id -> snapshot reference info
	// this is used to track the snapshot reference info.
	snapshotID2DataInfo *typeutil.ConcurrentMap[UniqueID, *SnapshotDataInfo]

	// read and write
	reader *SnapshotReader
	writer *SnapshotWriter
}

func newSnapshotMeta(ctx context.Context, catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager) (*snapshotMeta, error) {
	sm := &snapshotMeta{
		catalog:             catalog,
		snapshotID2DataInfo: typeutil.NewConcurrentMap[UniqueID, *SnapshotDataInfo](),
		reader:              NewSnapshotReader(chunkManager),
		writer:              NewSnapshotWriter(chunkManager),
	}

	if err := sm.reload(ctx); err != nil {
		log.Error("failed to reload snapshot meta from kv", zap.Error(err))
		return nil, err
	}

	return sm, nil
}

func (sm *snapshotMeta) reload(ctx context.Context) error {
	snapshots, err := sm.catalog.ListSnapshots(ctx)
	if err != nil {
		log.Info("failed to list snapshots from kv", zap.Error(err))
		return err
	}

	for _, snapshot := range snapshots {
		// Note: we don't need to read segment info from s3, anyone need to read segment info, they can read snapshot from s3.
		log.Info("reload snapshot from catalog",
			zap.String("name", snapshot.GetName()),
			zap.Int64("id", snapshot.GetId()),
			zap.String("s3_location", snapshot.GetS3Location()))

		snapshotData, err := sm.reader.ReadSnapshot(ctx, snapshot.GetCollectionId(), snapshot.GetId(), true)
		if err != nil {
			log.Error("failed to read snapshot from s3", zap.Error(err))
			return err
		}

		segmentIDs := make([]int64, 0)
		indexIDs := make([]int64, 0)
		for _, segment := range snapshotData.Segments {
			segmentIDs = append(segmentIDs, segment.GetSegmentId())
		}
		for _, index := range snapshotData.Indexes {
			indexIDs = append(indexIDs, index.GetIndexID())
		}

		sm.snapshotID2DataInfo.Insert(snapshot.GetId(), &SnapshotDataInfo{
			snapshotInfo: snapshot,
			SegmentIDs:   typeutil.NewUniqueSet(segmentIDs...),
			IndexIDs:     typeutil.NewUniqueSet(indexIDs...),
		})
	}

	return nil
}

func (sm *snapshotMeta) SaveSnapshot(ctx context.Context, snapshot *SnapshotData) error {
	// save snapshot info to s3
	metadataFilePath, err := sm.writer.Save(ctx, snapshot)
	if err != nil {
		log.Error("failed to save snapshot to s3", zap.Error(err))
		return err
	}
	snapshot.SnapshotInfo.S3Location = metadataFilePath

	segmentIDs := make([]int64, 0)
	indexIDs := make([]int64, 0)
	for _, segment := range snapshot.Segments {
		segmentIDs = append(segmentIDs, segment.GetSegmentId())
	}
	for _, index := range snapshot.Indexes {
		indexIDs = append(indexIDs, index.GetIndexID())
	}

	sm.snapshotID2DataInfo.Insert(snapshot.SnapshotInfo.GetId(), &SnapshotDataInfo{
		snapshotInfo: snapshot.SnapshotInfo,
		SegmentIDs:   typeutil.NewUniqueSet(segmentIDs...),
		IndexIDs:     typeutil.NewUniqueSet(indexIDs...),
	})

	return sm.catalog.SaveSnapshot(ctx, snapshot.SnapshotInfo)
}

func (sm *snapshotMeta) DropSnapshot(ctx context.Context, snapshotName string) error {
	log := log.Ctx(ctx).With(zap.String("snapshotName", snapshotName))
	snapshot, err := sm.getSnapshotByName(ctx, snapshotName)
	if err != nil {
		log.Error("failed to get snapshot by name", zap.Error(err))
		return err
	}

	sm.snapshotID2DataInfo.Remove(snapshot.GetId())
	err = sm.catalog.DropSnapshot(ctx, snapshot.GetCollectionId(), snapshot.GetId())
	if err != nil {
		log.Error("failed to drop snapshot from catalog", zap.Error(err))
		return err
	}

	err = sm.writer.Drop(ctx, snapshot.GetCollectionId(), snapshot.GetId())
	if err != nil {
		log.Error("failed to drop snapshot from s3", zap.Error(err))
		return err
	}

	return nil
}

func (sm *snapshotMeta) ListSnapshots(ctx context.Context, collectionID int64, partitionID int64) ([]string, error) {
	ret := make([]string, 0)
	sm.snapshotID2DataInfo.Range(func(key UniqueID, value *SnapshotDataInfo) bool {
		snapshotInfo := value.snapshotInfo
		collectionMatch := snapshotInfo.GetCollectionId() == collectionID || collectionID <= 0
		partitionMatch := partitionID <= 0 || slices.Contains(snapshotInfo.GetPartitionIds(), partitionID)
		if collectionMatch && partitionMatch {
			ret = append(ret, snapshotInfo.GetName())
		}
		return true
	})

	return ret, nil
}

func (sm *snapshotMeta) GetSnapshot(ctx context.Context, snapshotName string) (*datapb.SnapshotInfo, error) {
	snapshot, err := sm.getSnapshotByName(ctx, snapshotName)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// ReadSnapshotData reads the complete snapshot data including segments from S3
func (sm *snapshotMeta) ReadSnapshotData(ctx context.Context, snapshotName string) (*SnapshotData, error) {
	log := log.Ctx(ctx).With(zap.String("snapshotName", snapshotName))

	// Get snapshot info from memory first
	snapshotInfo, err := sm.getSnapshotByName(ctx, snapshotName)
	if err != nil {
		log.Error("failed to get snapshot by name", zap.Error(err))
		return nil, err
	}

	log.Info("got snapshot from memory before ReadSnapshot",
		zap.String("name", snapshotInfo.GetName()),
		zap.Int64("id", snapshotInfo.GetId()),
		zap.String("s3_location_from_memory", snapshotInfo.GetS3Location()))

	// Read complete snapshot data from S3
	snapshotData, err := sm.reader.ReadSnapshot(ctx, snapshotInfo.GetCollectionId(), snapshotInfo.GetId(), true)
	if err != nil {
		log.Error("failed to read snapshot data from S3", zap.Error(err))
		return nil, err
	}
	snapshotData.SnapshotInfo.S3Location = snapshotInfo.S3Location

	return snapshotData, nil
}

func (sm *snapshotMeta) getSnapshotByName(ctx context.Context, snapshotName string) (*datapb.SnapshotInfo, error) {
	var ret *datapb.SnapshotInfo
	sm.snapshotID2DataInfo.Range(func(key UniqueID, value *SnapshotDataInfo) bool {
		if value.snapshotInfo.GetName() == snapshotName {
			ret = value.snapshotInfo
			return false
		}
		return true
	})

	if ret == nil {
		return nil, fmt.Errorf("snapshot %s not found", snapshotName)
	}
	return ret, nil
}

func (sm *snapshotMeta) GetSnapshotBySegment(ctx context.Context, collectionID, segmentID UniqueID) []UniqueID {
	snapshotIDs := make([]UniqueID, 0)
	sm.snapshotID2DataInfo.Range(func(key UniqueID, value *SnapshotDataInfo) bool {
		if value.snapshotInfo.GetCollectionId() == collectionID && value.SegmentIDs.Contain(segmentID) {
			snapshotIDs = append(snapshotIDs, key)
		}
		return true
	})
	return snapshotIDs
}

func (sm *snapshotMeta) GetSnapshotByIndex(ctx context.Context, collectionID, indexID UniqueID) []UniqueID {
	snapshotIDs := make([]UniqueID, 0)
	sm.snapshotID2DataInfo.Range(func(key UniqueID, value *SnapshotDataInfo) bool {
		if value.snapshotInfo.GetCollectionId() == collectionID && value.IndexIDs.Contain(indexID) {
			snapshotIDs = append(snapshotIDs, key)
		}
		return true
	})
	return snapshotIDs
}
