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

package datanode

import (
	"context"
	"io"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// snapshotExportExecutor is independent of Import scheduling and registry locks.
// Registration and shutdown share a gate; remote I/O never runs under that gate.
type snapshotExportExecutor struct {
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	stopped    bool
	tasks      map[int64]*snapshotExportExecution
	tombstones map[int64]time.Time
	wg         sync.WaitGroup
}

type snapshotExportExecution struct {
	req      *datapb.SnapshotExportCopyRequest
	cancel   context.CancelFunc
	done     chan struct{}
	mu       sync.Mutex
	response *datapb.SnapshotExportCopyResponse
}

func newSnapshotExportExecutor(parent context.Context) *snapshotExportExecutor {
	ctx, cancel := context.WithCancel(parent)
	e := &snapshotExportExecutor{ctx: ctx, cancel: cancel, tasks: make(map[int64]*snapshotExportExecution), tombstones: make(map[int64]time.Time)}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				e.expire(now)
			}
		}
	}()
	return e
}

func validateSnapshotExportRequest(req *datapb.SnapshotExportCopyRequest) error {
	if req.GetVersion() != snapshotstorage.SnapshotExportRequestVersion || req.GetTaskId() <= 0 || req.GetJobId() <= 0 || req.GetSnapshotId() <= 0 || req.GetCollectionId() <= 0 || req.GetSnapshotMetadataPath() == "" || req.GetTargetUri() == "" || req.GetSourceStorageConfig() == nil || proto.Size(req) > snapshotstorage.SnapshotExportMaxRequestBytes {
		return merr.WrapErrServiceInternalMsg("invalid snapshot export task request")
	}
	if req.GetGenerationId() <= 0 || req.GetSegmentStart() < 0 || req.GetSegmentEnd() <= req.GetSegmentStart() {
		return merr.WrapErrDataIntegrityMsg("invalid snapshot export shard request")
	}
	if req.GetDeadlineTime() > uint64(^uint64(0)>>1) || int64(req.GetDeadlineTime()) <= time.Now().UnixMilli() {
		return merr.WrapErrServiceInternalMsg("snapshot export deadline expired")
	}
	return nil
}

func (e *snapshotExportExecutor) create(req *datapb.SnapshotExportCopyRequest) error {
	if err := validateSnapshotExportRequest(req); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return merr.WrapErrServiceNotReadyMsg("snapshot export executor stopped")
	}
	if deadline, ok := e.tombstones[req.GetTaskId()]; ok && time.Now().Before(deadline) {
		return merr.WrapErrDataIntegrityMsg("snapshot export task has been dropped")
	}
	if existing := e.tasks[req.GetTaskId()]; existing != nil {
		if !proto.Equal(existing.req, req) {
			return merr.WrapErrDataIntegrityMsg("snapshot export task identity conflict")
		}
		return nil
	}
	ctx, cancel := context.WithDeadline(e.ctx, time.UnixMilli(int64(req.GetDeadlineTime())))
	t := &snapshotExportExecution{req: proto.Clone(req).(*datapb.SnapshotExportCopyRequest), cancel: cancel, done: make(chan struct{}), response: &datapb.SnapshotExportCopyResponse{Status: merr.Success(), TaskId: req.GetTaskId(), State: datapb.ImportTaskStateV2_InProgress}}
	e.tasks[req.GetTaskId()] = t
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer close(t.done)
		defer cancel()
		err := e.execute(ctx, t)
		t.mu.Lock()
		defer t.mu.Unlock()
		if err != nil {
			t.response.State = datapb.ImportTaskStateV2_Failed
			t.response.Reason = snapshotstorage.SanitizeSnapshotExportReason(err, t.req)
		} else {
			t.response.State = datapb.ImportTaskStateV2_Completed
		}
	}()
	return nil
}

func (e *snapshotExportExecutor) execute(ctx context.Context, t *snapshotExportExecution) error {
	req := t.req
	sourceConfig := snapshotstorage.ObjectStorageConfigFromIndexConfig(req.GetSourceStorageConfig())
	sourceConfig.SkipBucketCheck = true
	sourceCM, err := storage.NewRemoteChunkManager(ctx, sourceConfig)
	if err != nil {
		return err
	}
	defer closeSnapshotExportClient(sourceCM)
	resolved, err := snapshotstorage.ResolveForeignStorage(ctx, sourceConfig, snapshotstorage.DirectionExport, snapshotstorage.SnapshotExportAttemptURI(req), req.GetExternalSpec())
	if err != nil {
		return err
	}
	defer closeSnapshotExportClient(resolved.ForeignCM)
	result, err := snapshotstorage.ExecuteSnapshotExport(ctx, req, sourceCM, resolved, snapshotstorage.NewTaskCopyPermits(req.GetCopyConcurrency()), func(copied, copiedBytes, total int64) {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.response.CopiedFiles, t.response.CopiedBytes = copied, copiedBytes
		t.response.TotalFiles = total
	})
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.response = result
	t.mu.Unlock()
	return nil
}

func closeSnapshotExportClient(cm storage.ChunkManager) {
	if remote, ok := cm.(*storage.RemoteChunkManager); ok {
		if closer, ok := remote.UnderlyingObjectStorage().(io.Closer); ok {
			_ = closer.Close()
		}
	}
}

func (e *snapshotExportExecutor) query(taskID int64) *datapb.SnapshotExportCopyResponse {
	e.mu.Lock()
	t := e.tasks[taskID]
	e.mu.Unlock()
	if t == nil {
		return &datapb.SnapshotExportCopyResponse{Status: merr.Status(merr.WrapErrImportSysFailed("snapshot export task not found"))}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return proto.Clone(t.response).(*datapb.SnapshotExportCopyResponse)
}

func (e *snapshotExportExecutor) drop(ctx context.Context, taskID int64, deadline time.Time) error {
	e.mu.Lock()
	t := e.tasks[taskID]
	if t != nil {
		if original := time.UnixMilli(int64(t.req.GetDeadlineTime())); original.After(deadline) {
			deadline = original
		}
		t.cancel()
	}
	if deadline.After(e.tombstones[taskID]) {
		e.tombstones[taskID] = deadline
	}
	e.mu.Unlock()
	if t != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
		}
		e.mu.Lock()
		if e.tasks[taskID] == t {
			delete(e.tasks, taskID)
		}
		e.mu.Unlock()
	}
	return nil
}

func (e *snapshotExportExecutor) slots() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var n int64
	for _, t := range e.tasks {
		select {
		case <-t.done:
		default:
			// Match DataCoord accounting for both new and older requests.
			n += max(t.req.GetTaskSlot(), 1)
		}
	}
	return n
}

func (e *snapshotExportExecutor) expire(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id, deadline := range e.tombstones {
		if !now.Before(deadline) {
			delete(e.tombstones, id)
		}
	}
	for id, t := range e.tasks {
		if now.UnixMilli() >= int64(t.req.GetDeadlineTime()) {
			t.cancel()
			select {
			case <-t.done:
				delete(e.tasks, id)
			default:
			}
		}
	}
}

func (e *snapshotExportExecutor) close() {
	e.mu.Lock()
	e.stopped = true
	e.cancel()
	e.mu.Unlock()
	e.wg.Wait()
	e.mu.Lock()
	clear(e.tasks)
	clear(e.tombstones)
	e.mu.Unlock()
}

func (node *DataNode) createSnapshotExportCopy(ctx context.Context, req *datapb.SnapshotExportCopyRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if node.snapshotExportExecutor == nil {
		return merr.Status(merr.WrapErrServiceNotReadyMsg("snapshot export executor unavailable")), nil
	}
	return merr.Status(node.snapshotExportExecutor.create(req)), nil
}
func (node *DataNode) querySnapshotExportCopy(taskID int64) *datapb.SnapshotExportCopyResponse {
	if node.snapshotExportExecutor == nil {
		return &datapb.SnapshotExportCopyResponse{Status: merr.Status(merr.WrapErrServiceNotReadyMsg("snapshot export executor unavailable"))}
	}
	return node.snapshotExportExecutor.query(taskID)
}
func (node *DataNode) dropSnapshotExportCopy(ctx context.Context, taskID int64, rawDeadline string) (*commonpb.Status, error) {
	deadline, err := strconv.ParseInt(rawDeadline, 10, 64)
	if err != nil || deadline <= 0 {
		return merr.Status(merr.WrapErrServiceInternalMsg("snapshot export drop requires original deadline")), nil
	}
	if node.snapshotExportExecutor == nil {
		return merr.Status(merr.WrapErrServiceNotReadyMsg("snapshot export executor unavailable")), nil
	}
	return merr.Status(node.snapshotExportExecutor.drop(ctx, taskID, time.UnixMilli(deadline))), nil
}
