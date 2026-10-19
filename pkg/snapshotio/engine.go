// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshotio

import (
	"context"
	"math"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	objectcopy "github.com/milvus-io/milvus/pkg/v3/objectstorage/copy"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotformat "github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type Reader interface {
	Read(context.Context, objectcopy.Object) ([]byte, error)
}
type Store interface {
	Reader
	Write(context.Context, objectcopy.Object, []byte) error
}
type Dependencies struct {
	Source Reader
	Target Store
	Copier objectcopy.Copier
	// Required for output URIs; must match Target and Copier.
	TargetStorageConfig *indexpb.StorageConfig
}
type Options struct{ Concurrency int }
type SegmentRange struct{ Start, End int64 }
type CopyRequest struct {
	SourceMetadataURI string
	TargetRoot        string
}

// Snapshot is immutable after Open and may be shared by concurrent ranges.
type Snapshot struct{ data *SnapshotData }

func (s *Snapshot) SegmentCount() int64 {
	if s == nil || s.data == nil {
		return 0
	}
	return int64(len(s.data.ManifestPaths))
}

type RangeResult struct {
	SourceMetadataURI                    string
	SnapshotID, CollectionID             int64
	TargetRoot                           string
	Range                                SegmentRange
	CopiedFiles, CopiedBytes, TotalFiles int64
}

// Publication can be persisted without source clients or credentials. Publish
// needs only target access, including after the source deadline has expired.
type Publication struct {
	StagingMetadataURI     string
	MetadataURI            string
	TotalFiles, TotalBytes int64
}
type CopyResult struct {
	MetadataURI            string
	TotalFiles, TotalBytes int64
}
type Engine struct {
	deps    Dependencies
	permits chan struct{}
}

func New(deps Dependencies, options Options) (*Engine, error) {
	if options.Concurrency <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("copy concurrency must be positive")
	}
	if deps.TargetStorageConfig == nil {
		return nil, merr.WrapErrParameterMissingMsg("target storage config is required")
	}
	// Source dependencies are checked by copying, so publication recovery needs
	// only the target store and its configuration.
	return &Engine{deps: deps, permits: make(chan struct{}, options.Concurrency)}, nil
}
func (e *Engine) Open(ctx context.Context, metadataURI string) (*Snapshot, error) {
	if e.deps.Source == nil {
		return nil, merr.WrapErrParameterMissingMsg("source reader is required")
	}
	bucket, _, _, err := ParseForeignURI(metadataURI)
	if err != nil {
		return nil, err
	}
	root, ok := DeriveSnapshotRootPath(metadataURI)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("snapshot metadata URI requires a snapshot root")
	}
	cm := &objectStore{reader: e.deps.Source, bucket: bucket, root: root}
	data, err := NewSnapshotReader(cm).ReadSnapshot(ctx, metadataURI, false)
	if err != nil {
		return nil, err
	}
	if typeutil.IsExternalCollection(data.Collection.GetSchema()) {
		return nil, merr.WrapErrParameterInvalidMsg("exporting external collections is not supported")
	}
	return &Snapshot{data: data}, nil
}
func (e *Engine) target(targetRoot string) (*ResolvedForeignStorage, error) {
	if e.deps.Target == nil {
		return nil, merr.WrapErrParameterMissingMsg("target store is required")
	}
	bucket, key, _, err := ParseForeignRootURI(targetRoot)
	if err != nil {
		return nil, err
	}
	cfg := e.deps.TargetStorageConfig
	if cfg.GetBucketName() != bucket {
		return nil, merr.WrapErrParameterInvalidMsg("target bucket differs from storage configuration")
	}
	if _, err := BuildStorageConfigSnapshotURI(cfg, GetSnapshotStagingMetadataPath(key)); err != nil {
		return nil, merr.Wrap(err, "invalid target storage configuration")
	}
	cm := &objectStore{reader: e.deps.Target, writer: e.deps.Target, bucket: bucket, root: key}
	return &ResolvedForeignStorage{ForeignBucket: bucket, ForeignCM: cm, ForeignStorageConfig: cfg, Copier: copierAdapter{e.deps.Copier}}, nil
}
func (e *Engine) CopyRange(ctx context.Context, snapshot *Snapshot, targetRoot string, r SegmentRange) (*RangeResult, error) {
	if snapshot == nil || snapshot.data == nil || e.deps.Source == nil || e.deps.Copier == nil {
		return nil, merr.WrapErrParameterMissingMsg("snapshot, source reader and copier are required")
	}
	resolved, err := e.target(targetRoot)
	if err != nil {
		return nil, err
	}
	bucket, _, _, err := ParseForeignURI(snapshot.data.MetadataPath)
	if err != nil {
		return nil, err
	}
	root, _ := DeriveSnapshotRootPath(snapshot.data.MetadataPath)
	cm := &objectStore{reader: e.deps.Source, bucket: bucket, root: root}
	return copyRange(ctx, snapshot.data, cm, nil, resolved, targetRoot, r, e.permits, func(ctx context.Context, descriptor string) (*snapshotformat.NativeManifestFiles, error) {
		return readManifestBytes(ctx, cm, bucket, descriptor)
	}, nil)
}
func (e *Engine) Prepare(ctx context.Context, snapshot *Snapshot, targetRoot string, results []RangeResult) (*Publication, error) {
	if snapshot == nil || snapshot.data == nil {
		return nil, merr.WrapErrParameterMissingMsg("snapshot is required")
	}
	resolved, err := e.target(targetRoot)
	if err != nil {
		return nil, err
	}
	return prepareRanges(ctx, snapshot.data, targetRoot, resolved, results)
}
func (e *Engine) Publish(ctx context.Context, p Publication) (*CopyResult, error) {
	if p.TotalFiles < 0 || p.TotalBytes < 0 {
		return nil, merr.WrapErrDataIntegrityMsg("negative publication statistics")
	}
	resolved, err := e.target(p.MetadataURI)
	if err != nil {
		return nil, err
	}
	if err := ValidateSnapshotObjectPathForBucket(resolved.ForeignCM, "staging metadata", p.StagingMetadataURI, resolved.ForeignBucket); err != nil {
		return nil, err
	}
	_, err = NewSnapshotWriter(resolved.ForeignCM).CommitStagedMetadata(ctx, p.StagingMetadataURI, p.MetadataURI, p.MetadataURI)
	if err != nil {
		return nil, err
	}
	return &CopyResult{MetadataURI: p.MetadataURI, TotalFiles: p.TotalFiles, TotalBytes: p.TotalBytes}, nil
}
func (e *Engine) CopySnapshot(ctx context.Context, req CopyRequest) (*CopyResult, error) {
	snapshot, err := e.Open(ctx, req.SourceMetadataURI)
	if err != nil {
		return nil, err
	}
	var ranges []RangeResult
	if snapshot.SegmentCount() > 0 {
		result, err := e.CopyRange(ctx, snapshot, req.TargetRoot, SegmentRange{End: snapshot.SegmentCount()})
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, *result)
	}
	publication, err := e.Prepare(ctx, snapshot, req.TargetRoot, ranges)
	if err != nil {
		return nil, err
	}
	return e.Publish(ctx, *publication)
}

type objectStore struct {
	reader       Reader
	writer       Store
	bucket, root string
}

func (s *objectStore) RootPath() string   { return s.root }
func (s *objectStore) BucketName() string { return s.bucket }
func (s *objectStore) Read(ctx context.Context, key string) ([]byte, error) {
	data, err := s.reader.Read(ctx, objectcopy.Object{Bucket: s.bucket, Key: key})
	return data, objectstorage.ToMilvusIoError(key, err)
}
func (s *objectStore) Write(ctx context.Context, key string, data []byte) error {
	if s.writer == nil {
		return merr.Wrap(merr.ErrServiceUnimplemented, "source store is read-only")
	}
	return objectstorage.ToMilvusIoError(key, s.writer.Write(ctx, objectcopy.Object{Bucket: s.bucket, Key: key}, data))
}
func (s *objectStore) Remove(context.Context, string) error {
	return merr.Wrap(merr.ErrServiceUnimplemented, "caller owns snapshot cleanup")
}
func (s *objectStore) MultiRemove(context.Context, []string) error {
	return merr.Wrap(merr.ErrServiceUnimplemented, "caller owns snapshot cleanup")
}

type copierAdapter struct{ copier objectcopy.Copier }

func (c copierAdapter) CopyCrossBucket(ctx context.Context, sb, sk, db, dk string) error {
	_, err := c.copier.Copy(ctx, objectcopy.Request{Source: objectcopy.Object{Bucket: sb, Key: sk}, Destination: objectcopy.Object{Bucket: db, Key: dk}})
	return err
}
func (c copierAdapter) CopyCrossBucketAndGetSize(ctx context.Context, sb, sk, db, dk string) (int64, error) {
	result, err := c.copier.Copy(ctx, objectcopy.Request{Source: objectcopy.Object{Bucket: sb, Key: sk}, Destination: objectcopy.Object{Bucket: db, Key: dk}})
	if err != nil {
		return 0, err
	}
	if !result.BytesKnown {
		return 0, merr.WrapErrServiceInternalMsg("snapshot copier must report confirmed data size")
	}
	return result.Bytes, nil
}

func copyRange(ctx context.Context, snapshot *SnapshotData, sourceCM ChunkManager, sourceConfig *indexpb.StorageConfig, resolved *ResolvedForeignStorage, targetRoot string, r SegmentRange, permits chan struct{}, readManifest ManifestReader, progress func(int64, int64, int64)) (*RangeResult, error) {
	if r.Start < 0 || r.End <= r.Start || r.End > int64(len(snapshot.ManifestPaths)) {
		return nil, merr.WrapErrDataIntegrityMsg("invalid snapshot manifest range")
	}
	if typeutil.IsExternalCollection(snapshot.Collection.GetSchema()) {
		return nil, merr.WrapErrParameterInvalidMsg("exporting external collections is not supported")
	}
	if readManifest == nil {
		readManifest = nativeManifestReader(sourceConfig)
	}
	data := *snapshot
	data.ManifestPaths = snapshot.ManifestPaths[r.Start:r.End]
	result := &RangeResult{SourceMetadataURI: snapshot.MetadataPath, SnapshotID: snapshot.SnapshotInfo.GetId(), CollectionID: snapshot.SnapshotInfo.GetCollectionId(), TargetRoot: targetRoot, Range: r}
	var mu sync.Mutex
	var statsErr error
	update := func(dc, db, dt int64) {
		mu.Lock()
		defer mu.Unlock()
		if db < 0 || dc < 0 || dt < 0 || db > math.MaxInt64-result.CopiedBytes || dc > math.MaxInt64-result.CopiedFiles || dt > math.MaxInt64-result.TotalFiles {
			statsErr = merr.WrapErrDataIntegrityMsg("export statistics overflow")
			return
		}
		result.CopiedFiles += dc
		result.CopiedBytes += db
		result.TotalFiles += dt
		if progress != nil {
			progress(result.CopiedFiles, result.CopiedBytes, result.TotalFiles)
		}
	}
	err := NewSnapshotReader(sourceCM, WithManifestReadPermits(permits)).visitSnapshotSegments(ctx, &data, data.oldRoot, data.newRoot, data.shouldRebase, func(ctx context.Context, one *SnapshotData) error {
		segment := one.Segments[0]
		// The visitor already binds the Avro identity; require its native entry point.
		if segment.GetStorageVersion() == StorageV3 && segment.GetNumOfRows() > 0 && segment.GetManifestPath() == "" {
			return merr.WrapErrDataIntegrityMsg("snapshot metadata is missing storage manifest for segment %d", segment.GetSegmentId())
		}
		bucket := sourceBucket(sourceCM, sourceConfig)
		items, err := buildSnapshotExportFiles(ctx, sourceCM, resolved.ForeignCM, bucket, resolved.ForeignBucket, one, targetRoot, resolved.ForeignStorageConfig, permits, readManifest)
		if err != nil {
			return err
		}
		sourceRoot, err := snapshotExportSourceRoot(one, sourceCM.RootPath())
		if err != nil {
			return err
		}
		if err := copySnapshotExportSegment(ctx, bucket, items, one, sourceRoot, NormalizeSnapshotObjectPath(targetRoot), resolved, permits, update); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if statsErr != nil {
		return nil, statsErr
	}
	return result, nil
}

func prepareRanges(ctx context.Context, snapshot *SnapshotData, targetRoot string, resolved *ResolvedForeignStorage, results []RangeResult) (*Publication, error) {
	sourceRoot, _ := DeriveSnapshotRootPath(snapshot.MetadataPath)
	plan, err := buildSnapshotExportMetadataPlan(snapshot, sourceRoot, targetRoot, resolved.ForeignStorageConfig)
	if err != nil {
		return nil, err
	}
	// Empty snapshots also need overlap protection; they have no CopyRange call.
	sourceBucket, _, _, err := ParseForeignURI(snapshot.MetadataPath)
	if err != nil {
		return nil, err
	}
	if sourceBucket == resolved.ForeignBucket && NormalizeSnapshotObjectPath(plan.MetadataURI) == NormalizeSnapshotObjectPath(snapshot.MetadataPath) {
		return nil, merr.WrapErrParameterInvalidMsg("export metadata would overwrite its source")
	}
	ranges := append([]RangeResult(nil), results...)
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Range.Start < ranges[j].Range.Start })
	var next, totalFiles, totalBytes int64
	for _, r := range ranges {
		if r.SourceMetadataURI != snapshot.MetadataPath || r.SnapshotID != snapshot.SnapshotInfo.GetId() || r.CollectionID != snapshot.SnapshotInfo.GetCollectionId() || r.TargetRoot != targetRoot || r.Range.Start != next || r.Range.End <= next || r.Range.End > int64(len(snapshot.ManifestPaths)) || r.TotalFiles < r.Range.End-r.Range.Start || r.CopiedFiles != r.TotalFiles || r.CopiedBytes < 0 {
			return nil, merr.WrapErrDataIntegrityMsg("incomplete or conflicting snapshot range result")
		}
		if r.CopiedFiles > math.MaxInt64-totalFiles || r.CopiedBytes > math.MaxInt64-totalBytes {
			return nil, merr.WrapErrDataIntegrityMsg("export statistics overflow")
		}
		next = r.Range.End
		totalFiles += r.CopiedFiles
		totalBytes += r.CopiedBytes
	}
	if next != int64(len(snapshot.ManifestPaths)) {
		return nil, merr.WrapErrDataIntegrityMsg("incomplete export range coverage")
	}
	if err := prepareSnapshotExportMetadata(ctx, resolved.ForeignCM, snapshot, plan); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	staging, err := BuildStorageConfigSnapshotURI(resolved.ForeignStorageConfig, GetSnapshotStagingMetadataPath(plan.TargetRoot))
	if err != nil {
		return nil, err
	}
	return &Publication{StagingMetadataURI: staging, MetadataURI: plan.MetadataURI, TotalFiles: totalFiles, TotalBytes: totalBytes}, nil
}

func sourceBucket(cm ChunkManager, config *indexpb.StorageConfig) string {
	if config != nil {
		return config.GetBucketName()
	}
	if source, ok := cm.(bucketChunkManager); ok {
		return source.BucketName()
	}
	return ""
}
