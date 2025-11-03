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

package importv2

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
)

type CopySegmentTask struct {
	ctx            context.Context
	cancel         context.CancelFunc
	jobID          int64
	taskID         int64
	collectionID   int64
	partitionIDs   []int64
	state          datapb.ImportTaskStateV2
	reason         string
	slots          int64
	segmentResults map[int64]*datapb.CopySegmentResult
	req            *datapb.CopySegmentRequest
	manager        TaskManager
	cm             storage.ChunkManager
}

func NewCopySegmentTask(
	req *datapb.CopySegmentRequest,
	manager TaskManager,
	cm storage.ChunkManager,
) Task {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize segmentResults map for each target segment
	segmentResults := make(map[int64]*datapb.CopySegmentResult)
	for _, target := range req.GetTargets() {
		segmentResults[target.GetSegmentId()] = &datapb.CopySegmentResult{
			SegmentId:         target.GetSegmentId(),
			ImportedRows:      0,
			Binlogs:           []*datapb.FieldBinlog{},
			Statslogs:         []*datapb.FieldBinlog{},
			Deltalogs:         []*datapb.FieldBinlog{},
			Bm25Logs:          []*datapb.FieldBinlog{},
			IndexInfos:        make(map[int64]*datapb.VectorScalarIndexInfo),
			TextIndexInfos:    make(map[int64]*datapb.TextIndexStats),
			JsonKeyIndexInfos: make(map[int64]*datapb.JsonKeyStats),
		}
	}

	// Extract collection and partition IDs from first target (all targets should have same collection)
	var collectionID int64
	var partitionIDs []int64
	if len(req.GetTargets()) > 0 {
		collectionID = req.GetTargets()[0].GetCollectionId()
		partitionIDSet := make(map[int64]struct{})
		for _, target := range req.GetTargets() {
			partitionIDSet[target.GetPartitionId()] = struct{}{}
		}
		for pid := range partitionIDSet {
			partitionIDs = append(partitionIDs, pid)
		}
	}

	task := &CopySegmentTask{
		ctx:            ctx,
		cancel:         cancel,
		jobID:          req.GetJobID(),
		taskID:         req.GetTaskID(),
		collectionID:   collectionID,
		partitionIDs:   partitionIDs,
		state:          datapb.ImportTaskStateV2_Pending,
		reason:         "",
		slots:          req.GetTaskSlot(),
		segmentResults: segmentResults,
		req:            req,
		manager:        manager,
		cm:             cm,
	}
	return task
}

func (t *CopySegmentTask) GetType() TaskType {
	return CopySegmentTaskType
}

func (t *CopySegmentTask) GetPartitionIDs() []int64 {
	return t.partitionIDs
}

func (t *CopySegmentTask) GetVchannels() []string {
	return nil // CopySegmentTask doesn't need vchannels
}

func (t *CopySegmentTask) GetJobID() int64 {
	return t.jobID
}

func (t *CopySegmentTask) GetTaskID() int64 {
	return t.taskID
}

func (t *CopySegmentTask) GetCollectionID() int64 {
	return t.collectionID
}

func (t *CopySegmentTask) GetState() datapb.ImportTaskStateV2 {
	return t.state
}

func (t *CopySegmentTask) GetReason() string {
	return t.reason
}

func (t *CopySegmentTask) GetSchema() *schemapb.CollectionSchema {
	return nil
}

func (t *CopySegmentTask) GetSlots() int64 {
	return t.slots
}

func (t *CopySegmentTask) GetBufferSize() int64 {
	return 0 // Copy task doesn't use buffer
}

func (t *CopySegmentTask) Cancel() {
	t.cancel()
}

func (t *CopySegmentTask) Clone() Task {
	return &CopySegmentTask{
		ctx:            t.ctx,
		cancel:         t.cancel,
		jobID:          t.jobID,
		taskID:         t.taskID,
		collectionID:   t.collectionID,
		partitionIDs:   t.partitionIDs,
		state:          t.state,
		reason:         t.reason,
		slots:          t.slots,
		segmentResults: t.segmentResults,
		req:            t.req,
		manager:        t.manager,
		cm:             t.cm,
	}
}

func (t *CopySegmentTask) GetSegmentResults() map[int64]*datapb.CopySegmentResult {
	return t.segmentResults
}

func (t *CopySegmentTask) Execute() []*conc.Future[any] {
	log.Info("start copy segment task", WrapLogFields(t)...)

	t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))

	sources := t.req.GetSources()
	targets := t.req.GetTargets()

	// Validate input
	if len(sources) == 0 {
		reason := "no source segments to copy"
		t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
		return nil
	}
	if len(sources) != len(targets) {
		reason := fmt.Sprintf("source segments count (%d) does not match target segments count (%d)",
			len(sources), len(targets))
		t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
		return nil
	}

	futures := make([]*conc.Future[any], 0, len(sources))
	for i := range sources {
		source := sources[i]
		target := targets[i]
		future := GetExecPool().Submit(func() (any, error) {
			return t.copySingleSegment(source, target)
		})
		futures = append(futures, future)
	}

	return futures
}

func (t *CopySegmentTask) copySingleSegment(source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget) (any, error) {
	logFields := WrapLogFields(t,
		zap.Int64("sourceCollectionID", source.GetCollectionId()),
		zap.Int64("sourcePartitionID", source.GetPartitionId()),
		zap.Int64("sourceSegmentID", source.GetSegmentId()),
		zap.Int64("targetCollectionID", target.GetCollectionId()),
		zap.Int64("targetPartitionID", target.GetPartitionId()),
		zap.Int64("targetSegmentID", target.GetSegmentId()),
		zap.Int("insertBinlogFields", len(source.GetInsertBinlogs())),
		zap.Int("statsBinlogFields", len(source.GetStatsBinlogs())),
		zap.Int("deltaBinlogFields", len(source.GetDeltaBinlogs())),
		zap.Int("bm25BinlogFields", len(source.GetBm25Binlogs())),
		zap.Int("vectorScalarIndexInfoCount", len(source.GetIndexFiles())),
		zap.Int("textIndexFieldCount", len(source.GetTextIndexFiles())),
		zap.Int("jsonKeyIndexFieldCount", len(source.GetJsonKeyIndexFiles())),
	)

	log.Info("start copying single segment", logFields...)
	if len(source.GetInsertBinlogs()) == 0 && len(source.GetDeltaBinlogs()) == 0 {
		reason := "no insert/delete binlogs for segment"
		log.Error(reason, logFields...)
		t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
		return nil, errors.New(reason)
	}

	// Copy segment files and index files together
	segmentResult, err := CopySegmentAndIndexFiles(
		t.ctx,
		t.cm,
		source,
		target,
		logFields,
	)
	if err != nil {
		reason := fmt.Sprintf("failed to copy segment files: %v", err)
		log.Error(reason, logFields...)
		t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
		return nil, err
	}

	// Update segment result in task with complete metadata (binlogs + indexes)
	t.manager.Update(t.GetTaskID(), UpdateSegmentResult(segmentResult))

	log.Info("successfully copied single segment", logFields...)
	return nil, nil
}
