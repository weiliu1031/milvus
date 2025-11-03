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
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type CopySegmentChecker interface {
	Start()
	Close()
}

type copySegmentChecker struct {
	ctx      context.Context
	meta     *meta
	broker   broker.Broker
	alloc    allocator.Allocator
	copyMeta CopySegmentMeta

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewCopySegmentChecker(
	ctx context.Context,
	meta *meta,
	broker broker.Broker,
	alloc allocator.Allocator,
	copyMeta CopySegmentMeta,
) CopySegmentChecker {
	return &copySegmentChecker{
		ctx:       ctx,
		meta:      meta,
		broker:    broker,
		alloc:     alloc,
		copyMeta:  copyMeta,
		closeChan: make(chan struct{}),
	}
}

func (c *copySegmentChecker) Start() {
	log.Info("start copy segment checker")
	ticker := time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalHigh.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			log.Info("copy segment checker exited")
			return
		case <-ticker.C:
			jobs := c.copyMeta.GetJobBy(c.ctx)
			for _, job := range jobs {
				switch job.GetState() {
				case datapb.CopySegmentJobState_CopySegmentJobPending:
					c.checkPendingJob(job)
				case datapb.CopySegmentJobState_CopySegmentJobExecuting:
					c.checkCopyingJob(job)
				case datapb.CopySegmentJobState_CopySegmentJobFailed:
					c.checkFailedJob(job)
				}
				c.tryTimeoutJob(job)
				c.checkGC(job)
			}
			c.LogJobStats(jobs)
			c.LogTaskStats()
		}
	}
}

func (c *copySegmentChecker) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

func (c *copySegmentChecker) LogJobStats(jobs []CopySegmentJob) {
	byState := lo.GroupBy(jobs, func(job CopySegmentJob) string {
		return job.GetState().String()
	})
	stateNum := make(map[string]int)
	for state := range datapb.CopySegmentJobState_value {
		if state == datapb.CopySegmentJobState_CopySegmentJobNone.String() {
			continue
		}
		num := len(byState[state])
		stateNum[state] = num
		metrics.CopySegmentJobs.WithLabelValues(state).Set(float64(num))
	}
	log.Info("copy segment job stats", zap.Any("stateNum", stateNum))
}

func (c *copySegmentChecker) LogTaskStats() {
	tasks := c.copyMeta.GetTaskBy(c.ctx)
	byState := lo.GroupBy(tasks, func(t CopySegmentTask) datapb.ImportTaskStateV2 {
		return t.GetState()
	})
	pending := len(byState[datapb.ImportTaskStateV2_Pending])
	inProgress := len(byState[datapb.ImportTaskStateV2_InProgress])
	completed := len(byState[datapb.ImportTaskStateV2_Completed])
	failed := len(byState[datapb.ImportTaskStateV2_Failed])
	log.Info("copy segment task stats",
		zap.Int("pending", pending), zap.Int("inProgress", inProgress),
		zap.Int("completed", completed), zap.Int("failed", failed))
	metrics.CopySegmentTasks.WithLabelValues(datapb.ImportTaskStateV2_Pending.String()).Set(float64(pending))
	metrics.CopySegmentTasks.WithLabelValues(datapb.ImportTaskStateV2_InProgress.String()).Set(float64(inProgress))
	metrics.CopySegmentTasks.WithLabelValues(datapb.ImportTaskStateV2_Completed.String()).Set(float64(completed))
	metrics.CopySegmentTasks.WithLabelValues(datapb.ImportTaskStateV2_Failed.String()).Set(float64(failed))
}

// checkPendingJob: Pending -> Copying
// Creates CopySegmentTask, grouping segments to avoid tasks that are too large
func (c *copySegmentChecker) checkPendingJob(job CopySegmentJob) {
	log := log.With(zap.Int64("jobID", job.GetJobId()))

	// Check if tasks already created
	tasks := c.copyMeta.GetTaskBy(c.ctx, WithCopyTaskJob(job.GetJobId()))
	if len(tasks) > 0 {
		return
	}

	// Group id mappings into tasks
	idMappings := job.GetIdMappings()
	if len(idMappings) == 0 {
		log.Warn("no id mappings to copy, mark job as completed")
		c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted),
			UpdateCopyJobReason("no segments to copy"))
		return
	}

	// Split into groups (max segments per task configured by maxSegmentsPerCopyTask)
	maxSegmentsPerTask := Params.DataCoordCfg.MaxSegmentsPerCopyTask.GetAsInt()
	groups := lo.Chunk(idMappings, maxSegmentsPerTask)

	// Create CopySegmentTask for each group
	for i, group := range groups {
		taskID, err := c.alloc.AllocID(c.ctx)
		if err != nil {
			log.Warn("failed to alloc task ID", zap.Error(err))
			return
		}

		// Extract partition IDs for this group
		task := &copySegmentTask{
			copyMeta: c.copyMeta,
			tr:       timerecord.NewTimeRecorder("copy segment task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       taskID,
			JobId:        job.GetJobId(),
			CollectionId: job.GetCollectionId(),
			NodeId:       NullNodeID,
			TaskVersion:  0,
			TaskSlot:     1, // Each copy task uses 1 slot
			State:        datapb.ImportTaskStateV2_Pending,
			Reason:      "",
			IdMappings:  group, // Lightweight ID mappings only
			CreatedTs:   uint64(time.Now().UnixNano()),
			CompleteTs:  0,
		})

		err = c.copyMeta.AddTask(c.ctx, task)
		if err != nil {
			log.Warn("failed to add copy segment task",
				zap.Int("groupIndex", i),
				zap.Int("segmentCount", len(group)),
				zap.Error(err))
			return
		}
		log.Info("created copy segment task",
			zap.Int64("taskID", taskID),
			zap.Int("groupIndex", i),
			zap.Int("segmentCount", len(group)))
	}

	// Update job state to Copying
	err := c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting),
		UpdateCopyJobProgress(0, int64(len(idMappings))))
	if err != nil {
		log.Warn("failed to update job state to Copying", zap.Error(err))
		return
	}
	log.Info("copy segment job started",
		zap.Int("taskCount", len(groups)),
		zap.Int("totalSegments", len(idMappings)))
}

// checkCopyingJob: Copying -> Completed
// Waits for all CopySegmentTask to complete, then finishes the job
func (c *copySegmentChecker) checkCopyingJob(job CopySegmentJob) {
	log := log.With(zap.Int64("jobID", job.GetJobId()))

	tasks := c.copyMeta.GetTaskBy(c.ctx, WithCopyTaskJob(job.GetJobId()))
	totalTasks := len(tasks)
	completedTasks := 0
	failedTasks := 0
	copiedSegments := int64(0)
	totalSegments := int64(len(job.GetIdMappings()))

	for _, task := range tasks {
		switch task.GetState() {
		case datapb.ImportTaskStateV2_Completed:
			completedTasks++
			copiedSegments += int64(len(task.GetIdMappings()))
		case datapb.ImportTaskStateV2_Failed:
			failedTasks++
		}
	}

	// Update job progress
	if copiedSegments != job.GetCopiedSegments() {
		err := c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
			UpdateCopyJobProgress(copiedSegments, totalSegments))
		if err != nil {
			log.Warn("failed to update job progress", zap.Error(err))
		} else {
			log.Debug("updated job progress",
				zap.Int64("copiedSegments", copiedSegments),
				zap.Int64("totalSegments", totalSegments),
				zap.Int("completedTasks", completedTasks),
				zap.Int("totalTasks", totalTasks))
		}
	}

	// If any task failed, mark job as failed
	if failedTasks > 0 {
		log.Warn("copy segment job has failed tasks",
			zap.Int("failedTasks", failedTasks),
			zap.Int("totalTasks", totalTasks))
		c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(fmt.Sprintf("%d/%d tasks failed", failedTasks, totalTasks)))
		return
	}

	// Wait for all tasks to complete
	if completedTasks < totalTasks {
		log.Debug("waiting for copy segment tasks to complete",
			zap.Int("completed", completedTasks),
			zap.Int("total", totalTasks))
		return
	}

	// All tasks completed, collect total rows and finish job
	var totalRows int64
	for _, task := range tasks {
		for _, mapping := range task.GetIdMappings() {
			targetSegID := mapping.GetTargetSegmentId()
			segment := c.meta.GetSegment(c.ctx, targetSegID)
			if segment != nil {
				totalRows += segment.GetNumOfRows()
			}
		}
	}

	c.finishJob(job, totalRows)
	log.Info("all copy segment tasks completed, job finished")
}

func (c *copySegmentChecker) finishJob(job CopySegmentJob, totalRows int64) {
	log := log.With(zap.Int64("jobID", job.GetJobId()))

	// Collect all target segment IDs from id_mappings
	tasks := c.copyMeta.GetTaskBy(c.ctx, WithCopyTaskJob(job.GetJobId()))
	targetSegmentIDs := make([]int64, 0)
	for _, task := range tasks {
		for _, mapping := range task.GetIdMappings() {
			targetSegmentIDs = append(targetSegmentIDs, mapping.GetTargetSegmentId())
		}
	}

	// Update segment states to Flushed (make them visible for query)
	if len(targetSegmentIDs) > 0 {
		for _, segID := range targetSegmentIDs {
			segment := c.meta.GetSegment(c.ctx, segID)
			if segment != nil && segment.GetState() != commonpb.SegmentState_Flushed {
				op := UpdateStatusOperator(segID, commonpb.SegmentState_Flushed)
				if err := c.meta.UpdateSegmentsInfo(c.ctx, op); err != nil {
					log.Warn("failed to update segment state to Flushed",
						zap.Int64("segmentID", segID),
						zap.Error(err))
				} else {
					log.Info("updated segment state to Flushed",
						zap.Int64("segmentID", segID))
				}
			}
		}
	}

	completeTs := uint64(time.Now().UnixNano())
	err := c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted),
		UpdateCopyJobCompleteTs(completeTs),
		UpdateCopyJobTotalRows(totalRows))
	if err != nil {
		log.Warn("failed to update job state to Completed", zap.Error(err))
		return
	}
	totalDuration := job.GetTR().ElapseSpan()
	metrics.CopySegmentJobLatency.WithLabelValues(metrics.TotalLabel).Observe(float64(totalDuration.Milliseconds()))
	log.Info("copy segment job completed",
		zap.Int64("totalRows", totalRows),
		zap.Int("targetSegments", len(targetSegmentIDs)),
		zap.Duration("totalDuration", totalDuration))
}

// checkFailedJob: marks all associated tasks as failed
func (c *copySegmentChecker) checkFailedJob(job CopySegmentJob) {
	log := log.With(zap.Int64("jobID", job.GetJobId()))

	tasks := c.copyMeta.GetTaskBy(c.ctx, WithCopyTaskJob(job.GetJobId()),
		WithCopyTaskStates(datapb.ImportTaskStateV2_Pending, datapb.ImportTaskStateV2_InProgress))

	if len(tasks) == 0 {
		return
	}

	log.Warn("copy segment job has failed, marking all tasks as failed",
		zap.String("reason", job.GetReason()),
		zap.Int("taskCount", len(tasks)))

	for _, task := range tasks {
		err := c.copyMeta.UpdateTask(c.ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.ImportTaskStateV2_Failed),
			UpdateCopyTaskReason(job.GetReason()))
		if err != nil {
			log.Warn("failed to update task state to failed",
				WrapCopySegmentTaskLog(task, zap.Error(err))...)
		}
	}
}

// tryTimeoutJob: checks job timeout
func (c *copySegmentChecker) tryTimeoutJob(job CopySegmentJob) {
	timeoutTime := tsoutil.PhysicalTime(job.GetTimeoutTs())
	if time.Now().After(timeoutTime) {
		log.Warn("copy segment job timeout",
			zap.Int64("jobID", job.GetJobId()),
			zap.Time("timeoutTime", timeoutTime))
		c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason("timeout"))
	}
}

// checkGC: garbage collection for completed/failed jobs
func (c *copySegmentChecker) checkGC(job CopySegmentJob) {
	if job.GetState() != datapb.CopySegmentJobState_CopySegmentJobCompleted &&
		job.GetState() != datapb.CopySegmentJobState_CopySegmentJobFailed {
		return
	}

	cleanupTime := tsoutil.PhysicalTime(job.GetCleanupTs())
	if time.Now().After(cleanupTime) {
		log := log.With(zap.Int64("jobID", job.GetJobId()))
		GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
		log.Info("copy segment job has reached GC retention",
			zap.Time("cleanupTime", cleanupTime), zap.Duration("GCRetention", GCRetention))

		tasks := c.copyMeta.GetTaskBy(c.ctx, WithCopyTaskJob(job.GetJobId()))
		shouldRemoveJob := true

		for _, task := range tasks {
			// If job failed and task has target segments in meta, don't remove yet
			if job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed {
				hasSegments := false
				for _, mapping := range task.GetIdMappings() {
					segment := c.meta.GetSegment(c.ctx, mapping.GetTargetSegmentId())
					if segment != nil {
						hasSegments = true
						break
					}
				}
				if hasSegments {
					shouldRemoveJob = false
					continue
				}
			}

			// If task is still on a node, don't remove yet
			if task.GetNodeId() != NullNodeID {
				shouldRemoveJob = false
				continue
			}

			err := c.copyMeta.RemoveTask(c.ctx, task.GetTaskId())
			if err != nil {
				log.Warn("failed to remove copy segment task during GC",
					WrapCopySegmentTaskLog(task, zap.Error(err))...)
				shouldRemoveJob = false
				continue
			}
			log.Info("copy segment task removed", WrapCopySegmentTaskLog(task)...)
		}

		if !shouldRemoveJob {
			return
		}

		err := c.copyMeta.RemoveJob(c.ctx, job.GetJobId())
		if err != nil {
			log.Warn("failed to remove copy segment job", zap.Error(err))
			return
		}
		log.Info("copy segment job removed")
	}
}
