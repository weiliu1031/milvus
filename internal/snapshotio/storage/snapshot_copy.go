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

package storage

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Version 4 requires rewritten V5 Avro before a shard can complete. A new
// generation fences version 3 results whose Avro still contains source paths.
const SnapshotExportRequestVersion int32 = 4
const SnapshotExportMaxRequestBytes = 1024 * 1024

// SnapshotExportAttemptURI fences objects written by lost workers.
func SnapshotExportAttemptURI(req *datapb.SnapshotExportCopyRequest) string {
	id := req.GetGenerationId()
	return strings.TrimRight(req.GetTargetUri(), "/") + "/attempts/" + strconv.FormatInt(id, 10)
}

// CopySnapshotExportPlan copies files and reports sizes only after successful
// completion. Metadata copies report zero bytes but still report file completion.
// Callbacks are serialized. It shares the bounded executor with snapshot Restore.
func CopySnapshotExportPlan(ctx context.Context, items []FileCopy, sourceBucket, targetBucket string, copier milvusstorage.CrossBucketCopier, permits chan struct{}, completed func(int64)) error {
	sized, ok := copier.(milvusstorage.SizedCrossBucketCopier)
	if !ok || copier == nil {
		return merr.WrapErrServiceInternalMsg("invalid snapshot export copier")
	}
	var mu sync.Mutex
	var total int64
	return CopyFiles(ctx, items, permits, func(copyCtx context.Context, file FileCopy) error {
		var size int64
		var err error
		if file.IsMetadata {
			// Metadata is excluded from byte statistics. Do not add a destination
			// size query after a successful provider copy just to discard its result.
			err = copier.CopyCrossBucket(copyCtx, sourceBucket, file.SourcePath, targetBucket, file.DestinationPath)
		} else {
			size, err = sized.CopyCrossBucketAndGetSize(copyCtx, sourceBucket, file.SourcePath, targetBucket, file.DestinationPath)
		}
		if err != nil {
			return err
		}
		if size < 0 {
			return merr.WrapErrDataIntegrityMsg("invalid export file size")
		}
		mu.Lock()
		defer mu.Unlock()
		if size > math.MaxInt64-total {
			return merr.WrapErrDataIntegrityMsg("export size overflow")
		}
		total += size
		if completed != nil {
			completed(size)
		}
		return nil
	})
}

// SanitizeSnapshotExportReason excludes credentials from worker failure reports.
func SanitizeSnapshotExportReason(err error, req *datapb.SnapshotExportCopyRequest) string {
	if err == nil {
		return ""
	}
	secrets := []string{req.GetExternalSpec(), req.GetSourceStorageConfig().GetAccessKeyID(), req.GetSourceStorageConfig().GetSecretAccessKey(), req.GetSourceStorageConfig().GetGcpCredentialJSON(), req.GetSourceStorageConfig().GetSslCACert()}
	var spec struct {
		Extfs map[string]json.RawMessage `json:"extfs"`
	}
	if json.Unmarshal([]byte(req.GetExternalSpec()), &spec) == nil {
		for _, key := range []string{"access_key_id", "access_key_value", "ssl_ca_cert", "external_id", "credential_json", "source_sas_token"} {
			var v string
			if json.Unmarshal(spec.Extfs[key], &v) == nil {
				secrets = append(secrets, v)
			}
		}
	}
	reason := strings.TrimSpace(err.Error())
	for _, secret := range secrets {
		if secret != "" {
			reason = strings.ReplaceAll(reason, secret, "<redacted>")
			reason = strings.ReplaceAll(reason, url.QueryEscape(secret), "<redacted>")
		}
	}
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	return reason
}

// ExecuteSnapshotExport copies one segment range and returns scalar statistics.
// Only DataCoord finalization prepares staging metadata after all writers exit.
// The caller owns clients and supplies a task-local copy concurrency budget.
func ExecuteSnapshotExport(ctx context.Context, req *datapb.SnapshotExportCopyRequest, sourceCM milvusstorage.ChunkManager, resolved *ResolvedForeignStorage, permits chan struct{}, progress func(int64, int64, int64)) (*datapb.SnapshotExportCopyResponse, error) {
	options := []SnapshotReaderOption{WithManifestReadPermits(permits), WithSnapshotManifestRange(req.GetSegmentStart(), req.GetSegmentEnd())}
	result := &datapb.SnapshotExportCopyResponse{TaskId: req.GetTaskId()}
	var mu sync.Mutex
	var statsErr error
	// Statistics count successful segment copies, including repeated shared LOBs.
	// TotalFiles grows as segments finish planning; it is final only on completion.
	_, err := NewSnapshotReader(sourceCM, options...).readSnapshot(ctx, req.GetSnapshotMetadataPath(), true,
		func(ctx context.Context, snapshot *SnapshotData) error {
			if snapshot.SnapshotInfo.GetId() != req.GetSnapshotId() || snapshot.SnapshotInfo.GetCollectionId() != req.GetCollectionId() {
				return merr.WrapErrDataIntegrityMsg("snapshot export source identity changed")
			}
			if err := validateSnapshotExportShardMetadata(snapshot); err != nil {
				return err
			}
			items, err := buildSnapshotExportFiles(ctx, sourceCM, resolved.ForeignCM, req.GetSourceStorageConfig().GetBucketName(), resolved.ForeignBucket, snapshot, SnapshotExportAttemptURI(req), resolved.ForeignStorageConfig, req.GetSourceStorageConfig(), permits)
			if err != nil {
				return err
			}
			sourceRoot, err := snapshotExportSourceRoot(snapshot, sourceCM.RootPath())
			if err != nil {
				return err
			}
			err = copySnapshotExportSegment(ctx, req, items, snapshot, sourceRoot, NormalizeSnapshotObjectPath(SnapshotExportAttemptURI(req)), resolved, permits,
				func(dc, db, dt int64) {
					mu.Lock()
					defer mu.Unlock()
					if db > math.MaxInt64-result.CopiedBytes || dc > math.MaxInt64-result.CopiedFiles || dt > math.MaxInt64-result.TotalFiles {
						statsErr = merr.WrapErrDataIntegrityMsg("export statistics overflow")
						return
					}
					result.CopiedFiles += dc
					result.CopiedBytes += db
					result.TotalFiles += dt
					if progress != nil {
						progress(result.CopiedFiles, result.CopiedBytes, result.TotalFiles)
					}
				})
			return err
		})
	if err != nil {
		return nil, err
	}
	if statsErr != nil {
		return nil, statsErr
	}
	result.State = datapb.ImportTaskStateV2_Completed
	result.Status = merr.Success()
	return result, nil
}
