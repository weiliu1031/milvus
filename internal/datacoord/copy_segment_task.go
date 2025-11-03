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

package datacoord

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

type CopySegmentTaskFilter func(task CopySegmentTask) bool

func WithCopyTaskJob(jobID int64) CopySegmentTaskFilter {
	return func(task CopySegmentTask) bool {
		return task.GetJobId() == jobID
	}
}

func WithCopyTaskStates(states ...datapb.ImportTaskStateV2) CopySegmentTaskFilter {
	return func(task CopySegmentTask) bool {
		for _, state := range states {
			if task.GetState() == state {
				return true
			}
		}
		return false
	}
}

type UpdateCopySegmentTaskAction func(task CopySegmentTask)

func UpdateCopyTaskState(state datapb.ImportTaskStateV2) UpdateCopySegmentTaskAction {
	return func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().State = state
	}
}

func UpdateCopyTaskReason(reason string) UpdateCopySegmentTaskAction {
	return func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().Reason = reason
	}
}

func UpdateCopyTaskNodeID(nodeID int64) UpdateCopySegmentTaskAction {
	return func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().NodeId = nodeID
	}
}

func UpdateCopyTaskCompleteTs(completeTs uint64) UpdateCopySegmentTaskAction {
	return func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().CompleteTs = completeTs
	}
}

type CopySegmentTask interface {
	task.Task
	GetTaskId() int64
	GetJobId() int64
	GetCollectionId() int64
	GetNodeId() int64
	GetState() datapb.ImportTaskStateV2
	GetReason() string
	GetIdMappings() []*datapb.CopySegmentIDMapping // Lightweight ID mappings
	GetTR() *timerecord.TimeRecorder
	Clone() CopySegmentTask
}

type copySegmentTask struct {
	task atomic.Pointer[datapb.CopySegmentTask]

	copyMeta     CopySegmentMeta
	meta         *meta         // For accessing segment metadata and collection schema
	snapshotMeta *snapshotMeta // For accessing snapshot data
	tr           *timerecord.TimeRecorder
	times        *taskcommon.Times
}

func (t *copySegmentTask) GetTaskId() int64 {
	return t.task.Load().GetTaskId()
}

func (t *copySegmentTask) GetJobId() int64 {
	return t.task.Load().GetJobId()
}

func (t *copySegmentTask) GetCollectionId() int64 {
	return t.task.Load().GetCollectionId()
}

func (t *copySegmentTask) GetNodeId() int64 {
	return t.task.Load().GetNodeId()
}

func (t *copySegmentTask) GetState() datapb.ImportTaskStateV2 {
	return t.task.Load().GetState()
}

func (t *copySegmentTask) GetReason() string {
	return t.task.Load().GetReason()
}

func (t *copySegmentTask) GetIdMappings() []*datapb.CopySegmentIDMapping {
	return t.task.Load().GetIdMappings()
}

func (t *copySegmentTask) GetTR() *timerecord.TimeRecorder {
	return t.tr
}

func (t *copySegmentTask) Clone() CopySegmentTask {
	cloned := &copySegmentTask{
		copyMeta:     t.copyMeta,
		meta:         t.meta,
		snapshotMeta: t.snapshotMeta,
		tr:           t.tr,
		times:        t.times,
	}
	cloned.task.Store(t.task.Load())
	return cloned
}

// Implement task.Task interface

func (t *copySegmentTask) GetTaskID() int64 {
	return t.GetTaskId()
}

func (t *copySegmentTask) GetTaskType() taskcommon.Type {
	return taskcommon.CopySegment
}

func (t *copySegmentTask) GetTaskState() taskcommon.State {
	return taskcommon.FromImportState(t.GetState())
}

func (t *copySegmentTask) GetTaskSlot() int64 {
	return t.task.Load().GetTaskSlot()
}

func (t *copySegmentTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *copySegmentTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *copySegmentTask) GetTaskVersion() int64 {
	return t.task.Load().GetTaskVersion()
}

func (t *copySegmentTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log.Info("processing pending copy segment task...", WrapCopySegmentTaskLog(t)...)
	job := t.copyMeta.GetJob(context.TODO(), t.GetJobId())
	req, err := AssembleCopySegmentRequest(t, job)
	if err != nil {
		log.Warn("failed to assemble copy segment request",
			WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID), zap.Error(err))...)
		return
	}
	err = cluster.CreateCopySegment(nodeID, req)
	if err != nil {
		log.Warn("failed to create copy segment task on datanode",
			WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID), zap.Error(err))...)
		return
	}
	log.Info("create copy segment task on datanode done",
		WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID))...)
	err = t.copyMeta.UpdateTask(context.TODO(), t.GetTaskId(),
		UpdateCopyTaskNodeID(nodeID),
		UpdateCopyTaskState(datapb.ImportTaskStateV2_InProgress))
	if err != nil {
		log.Warn("failed to update copy segment task state",
			WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID), zap.Error(err))...)
		return
	}
	// Record pending duration
	pendingDuration := t.GetTR().RecordSpan()
	metrics.CopySegmentJobLatency.WithLabelValues(metrics.CopyStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info("copy segment task start to execute",
		WrapCopySegmentTaskLog(t, zap.Int64("scheduledNodeID", nodeID),
			zap.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (t *copySegmentTask) QueryTaskOnWorker(cluster session.Cluster) {
	nodeID := t.GetNodeId()
	req := &datapb.QueryCopySegmentRequest{
		TaskID: t.GetTaskId(),
	}
	resp, err := cluster.QueryCopySegment(nodeID, req)
	if err != nil || resp.GetState() != datapb.ImportTaskStateV2_Completed {
		err = t.copyMeta.UpdateTask(context.TODO(), t.GetTaskId(),
			UpdateCopyTaskState(datapb.ImportTaskStateV2_Failed),
			UpdateCopyTaskReason(resp.GetReason()))
		if err != nil {
			log.Warn("failed to update copy segment task state to failed",
				WrapCopySegmentTaskLog(t, zap.Error(err))...)
			return
		}

		// Sync job state immediately
		job := t.copyMeta.GetJob(context.TODO(), t.GetJobId())
		if job != nil && job.GetState() != datapb.CopySegmentJobState_CopySegmentJobFailed {
			err = t.copyMeta.UpdateJob(context.TODO(), t.GetJobId(),
				UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
				UpdateCopyJobReason(resp.GetReason()))
			if err != nil {
				log.Warn("failed to update job state to Failed",
					zap.Int64("jobID", t.GetJobId()), zap.Error(err))
			}
		}
		log.Warn("copy segment task failed",
			WrapCopySegmentTaskLog(t, zap.String("reason", resp.GetReason()))...)
		return
	}

	// Sync task state and binlog info
	err = SyncCopySegmentTask(t, resp, t.copyMeta, t.meta)
	if err != nil {
		log.Warn("failed to sync copy segment task",
			WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID), zap.Error(err))...)
	}

	log.Info("query copy segment task",
		WrapCopySegmentTaskLog(t, zap.String("respState", resp.GetState().String()),
			zap.String("reason", resp.GetReason()))...)
}

func (t *copySegmentTask) DropTaskOnWorker(cluster session.Cluster) {
	nodeID := t.GetNodeId()
	err := cluster.DropCopySegment(nodeID, t.GetTaskId())
	if err != nil {
		log.Warn("failed to drop copy segment task on datanode",
			WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID), zap.Error(err))...)
		return
	}
	log.Info("drop copy segment task on datanode done",
		WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID))...)
}

// Helper functions

func WrapCopySegmentTaskLog(task CopySegmentTask, fields ...zap.Field) []zap.Field {
	res := []zap.Field{
		zap.Int64("taskID", task.GetTaskId()),
		zap.Int64("jobID", task.GetJobId()),
		zap.Int64("collectionID", task.GetCollectionId()),
		zap.String("state", task.GetState().String()),
	}
	res = append(res, fields...)
	return res
}

func AssembleCopySegmentRequest(task CopySegmentTask, job CopySegmentJob) (*datapb.CopySegmentRequest, error) {
	t := task.(*copySegmentTask)
	ctx := context.Background()

	// Read complete snapshot data from S3 to retrieve source segment binlogs
	snapshotData, err := t.snapshotMeta.ReadSnapshotData(ctx, job.GetSnapshotName())
	if err != nil {
		log.Error("failed to read snapshot data for copy segment task",
			append(WrapCopySegmentTaskLog(task), zap.Error(err))...)
		return nil, err
	}

	// Build source segment map for quick lookup
	sourceSegmentMap := make(map[int64]*datapb.SegmentDescription)
	for _, segDesc := range snapshotData.Segments {
		sourceSegmentMap[segDesc.GetSegmentId()] = segDesc
	}

	// Dynamically build sources and targets from id_mappings
	idMappings := task.GetIdMappings()
	sources := make([]*datapb.CopySegmentSource, 0, len(idMappings))
	targets := make([]*datapb.CopySegmentTarget, 0, len(idMappings))

	for _, mapping := range idMappings {
		sourceSegID := mapping.GetSourceSegmentId()
		targetSegID := mapping.GetTargetSegmentId()
		partitionID := mapping.GetPartitionId()

		// Get source segment description from snapshot
		sourceSegDesc, ok := sourceSegmentMap[sourceSegID]
		if !ok {
			log.Warn("source segment not found in snapshot",
				zap.Int64("sourceSegmentID", sourceSegID),
				zap.String("snapshotName", job.GetSnapshotName()))
			continue
		}

		// Build source with full binlog information
		source := &datapb.CopySegmentSource{
			CollectionId:      snapshotData.SnapshotInfo.GetCollectionId(),
			PartitionId:       sourceSegDesc.GetPartitionId(),
			SegmentId:         sourceSegDesc.GetSegmentId(),
			InsertBinlogs:     sourceSegDesc.GetBinlogs(),
			StatsBinlogs:      sourceSegDesc.GetStatslogs(),
			DeltaBinlogs:      sourceSegDesc.GetDeltalogs(),
			IndexFiles:        sourceSegDesc.GetIndexFiles(),        // vector/scalar index file info
			Bm25Binlogs:       sourceSegDesc.GetBm25Statslogs(),     // BM25 stats logs
			TextIndexFiles:    sourceSegDesc.GetTextIndexFiles(),    // Text index files
			JsonKeyIndexFiles: sourceSegDesc.GetJsonKeyIndexFiles(), // JSON key index files
		}
		sources = append(sources, source)

		// Build target with only IDs (binlog paths will be generated during copy)
		target := &datapb.CopySegmentTarget{
			CollectionId: job.GetCollectionId(),
			PartitionId:  partitionID,
			SegmentId:    targetSegID,
		}
		log.Info("prepare copy segment source and target", zap.Any("source", sourceSegDesc), zap.Any("target", target))
		targets = append(targets, target)
	}

	return &datapb.CopySegmentRequest{
		ClusterID:     Params.CommonCfg.ClusterPrefix.GetValue(),
		JobID:         task.GetJobId(),
		TaskID:        task.GetTaskId(),
		Sources:       sources,
		Targets:       targets,
		StorageConfig: createStorageConfig(),
		TaskSlot:      task.GetTaskSlot(),
	}, nil
}

func SyncCopySegmentTask(task CopySegmentTask, resp *datapb.QueryCopySegmentResponse, copyMeta CopySegmentMeta, meta *meta) error {
	ctx := context.TODO()

	// Update task state based on response
	switch resp.GetState() {
	case datapb.ImportTaskStateV2_Completed:
		// Update binlog information for all segments
		for _, result := range resp.GetSegmentResults() {
			// Compress binlog paths and fill logID
			err := binlog.CompressBinLogs(result.GetBinlogs(), result.GetDeltalogs(),
				result.GetStatslogs(), result.GetBm25Logs())
			if err != nil {
				log.Warn("fail to CompressBinLogs for copy segment binlogs",
					WrapCopySegmentTaskLog(task, zap.Int64("segmentID", result.GetSegmentId()),
						zap.Error(err))...)
				return err
			}

			// Update binlog info and segment state to Flushed
			op1 := UpdateBinlogsOperator(result.GetSegmentId(), result.GetBinlogs(),
				result.GetStatslogs(), result.GetDeltalogs(), result.GetBm25Logs())
			op2 := UpdateStatusOperator(result.GetSegmentId(), commonpb.SegmentState_Flushed)
			err = meta.UpdateSegmentsInfo(ctx, op1, op2)
			if err != nil {
				// On error, mark task and job as failed
				updateErr := copyMeta.UpdateTask(ctx, task.GetTaskId(),
					UpdateCopyTaskState(datapb.ImportTaskStateV2_Failed),
					UpdateCopyTaskReason(err.Error()))
				if updateErr != nil {
					log.Warn("failed to update task state to Failed",
						zap.Int64("taskID", task.GetTaskId()), zap.Error(updateErr))
				}

				updateErr = copyMeta.UpdateJob(ctx, task.GetJobId(),
					UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
					UpdateCopyJobReason(err.Error()))
				if updateErr != nil {
					log.Warn("failed to update job state to Failed",
						zap.Int64("jobID", task.GetJobId()), zap.Error(updateErr))
				}

				log.Warn("update copy segment binlogs failed",
					WrapCopySegmentTaskLog(task, zap.String("err", err.Error()))...)
				return err
			}

			// Sync vector/scalar indexes
			if err = syncVectorScalarIndexes(ctx, result, task, meta, copyMeta); err != nil {
				return err
			}

			// Sync text indexes
			if err = syncTextIndexes(ctx, result, task, meta, copyMeta); err != nil {
				return err
			}

			// Sync JSON key indexes
			if err = syncJsonKeyIndexes(ctx, result, task, meta, copyMeta); err != nil {
				return err
			}

			log.Info("update copy segment info done",
				WrapCopySegmentTaskLog(task, zap.Int64("segmentID", result.GetSegmentId()),
					zap.Any("segmentResult", result))...)
		}

		// Mark task as completed and record copying duration
		completeTs := uint64(time.Now().UnixNano())
		copyingDuration := task.GetTR().RecordSpan()
		metrics.CopySegmentJobLatency.WithLabelValues(metrics.CopyStageCopying).Observe(float64(copyingDuration.Milliseconds()))
		log.Info("copy segment task completed",
			WrapCopySegmentTaskLog(task, zap.Duration("taskTimeCost/copying", copyingDuration))...)

		return copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.ImportTaskStateV2_Completed),
			UpdateCopyTaskCompleteTs(completeTs))

	case datapb.ImportTaskStateV2_Failed:
		return copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.ImportTaskStateV2_Failed),
			UpdateCopyTaskReason(resp.GetReason()))
	}
	return nil
}

// syncVectorScalarIndexes synchronizes vector and scalar index metadata to indexMeta
func syncVectorScalarIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, copyMeta CopySegmentMeta,
) error {
	if len(result.GetIndexInfos()) == 0 {
		return nil
	}

	// Find partition ID from task's ID mappings
	var partitionID int64
	for _, mapping := range task.GetIdMappings() {
		if mapping.GetTargetSegmentId() == result.GetSegmentId() {
			partitionID = mapping.GetPartitionId()
			break
		}
	}

	// Sync each vector/scalar index
	for fieldID, indexInfo := range result.GetIndexInfos() {
		segIndex := &model.SegmentIndex{
			SegmentID:                 result.GetSegmentId(),
			CollectionID:              task.GetCollectionId(),
			PartitionID:               partitionID,
			IndexID:                   indexInfo.GetIndexId(),
			BuildID:                   indexInfo.GetBuildId(),
			IndexState:                commonpb.IndexState_Finished,
			IndexFileKeys:             indexInfo.GetIndexFilePaths(),
			IndexSerializedSize:       uint64(indexInfo.GetIndexSize()),
			IndexMemSize:              uint64(indexInfo.GetIndexSize()),
			IndexVersion:              indexInfo.GetVersion(),
			CurrentIndexVersion:       indexInfo.GetCurrentIndexVersion(),
			CurrentScalarIndexVersion: indexInfo.GetCurrentScalarIndexVersion(),
			CreatedUTCTime:            uint64(time.Now().Unix()),
			FinishedUTCTime:           uint64(time.Now().Unix()),
			NumRows:                   result.GetImportedRows(),
		}

		err := meta.indexMeta.AddSegmentIndex(ctx, segIndex)
		if err != nil {
			log.Warn("failed to add segment index",
				WrapCopySegmentTaskLog(task,
					zap.Int64("segmentID", result.GetSegmentId()),
					zap.Int64("fieldID", fieldID),
					zap.Int64("indexID", indexInfo.GetIndexId()),
					zap.Error(err))...)

			// Mark task and job as failed
			updateErr := copyMeta.UpdateTask(ctx, task.GetTaskId(),
				UpdateCopyTaskState(datapb.ImportTaskStateV2_Failed),
				UpdateCopyTaskReason(err.Error()))
			if updateErr != nil {
				log.Warn("failed to update task state to Failed",
					zap.Int64("taskID", task.GetTaskId()), zap.Error(updateErr))
			}

			updateErr = copyMeta.UpdateJob(ctx, task.GetJobId(),
				UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
				UpdateCopyJobReason(err.Error()))
			if updateErr != nil {
				log.Warn("failed to update job state to Failed",
					zap.Int64("jobID", task.GetJobId()), zap.Error(updateErr))
			}
			return err
		}

		log.Info("synced vector/scalar index",
			WrapCopySegmentTaskLog(task,
				zap.Int64("segmentID", result.GetSegmentId()),
				zap.Int64("fieldID", fieldID),
				zap.Int64("indexID", indexInfo.GetIndexId()),
				zap.Int64("buildID", indexInfo.GetBuildId()))...)
	}
	return nil
}

// syncTextIndexes synchronizes text index metadata to segment
func syncTextIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, copyMeta CopySegmentMeta,
) error {
	if len(result.GetTextIndexInfos()) == 0 {
		return nil
	}

	err := meta.UpdateSegment(result.GetSegmentId(),
		SetTextIndexLogs(result.GetTextIndexInfos()))
	if err != nil {
		log.Warn("failed to update text index",
			WrapCopySegmentTaskLog(task,
				zap.Int64("segmentID", result.GetSegmentId()),
				zap.Error(err))...)

		// Mark task and job as failed
		updateErr := copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.ImportTaskStateV2_Failed),
			UpdateCopyTaskReason(err.Error()))
		if updateErr != nil {
			log.Warn("failed to update task state to Failed",
				zap.Int64("taskID", task.GetTaskId()), zap.Error(updateErr))
		}

		updateErr = copyMeta.UpdateJob(ctx, task.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(err.Error()))
		if updateErr != nil {
			log.Warn("failed to update job state to Failed",
				zap.Int64("jobID", task.GetJobId()), zap.Error(updateErr))
		}
		return err
	}

	log.Info("synced text indexes",
		WrapCopySegmentTaskLog(task,
			zap.Int64("segmentID", result.GetSegmentId()),
			zap.Int("count", len(result.GetTextIndexInfos())))...)
	return nil
}

// syncJsonKeyIndexes synchronizes JSON key index metadata to segment
func syncJsonKeyIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, copyMeta CopySegmentMeta,
) error {
	if len(result.GetJsonKeyIndexInfos()) == 0 {
		return nil
	}

	err := meta.UpdateSegment(result.GetSegmentId(),
		SetJsonKeyIndexLogs(result.GetJsonKeyIndexInfos()))
	if err != nil {
		log.Warn("failed to update json key index",
			WrapCopySegmentTaskLog(task,
				zap.Int64("segmentID", result.GetSegmentId()),
				zap.Error(err))...)

		// Mark task and job as failed
		updateErr := copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.ImportTaskStateV2_Failed),
			UpdateCopyTaskReason(err.Error()))
		if updateErr != nil {
			log.Warn("failed to update task state to Failed",
				zap.Int64("taskID", task.GetTaskId()), zap.Error(updateErr))
		}

		updateErr = copyMeta.UpdateJob(ctx, task.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(err.Error()))
		if updateErr != nil {
			log.Warn("failed to update job state to Failed",
				zap.Int64("jobID", task.GetJobId()), zap.Error(updateErr))
		}
		return err
	}

	log.Info("synced json key indexes",
		WrapCopySegmentTaskLog(task,
			zap.Int64("segmentID", result.GetSegmentId()),
			zap.Int("count", len(result.GetJsonKeyIndexInfos())))...)
	return nil
}
