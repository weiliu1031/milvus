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
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type CopySegmentInspector interface {
	Start()
	Close()
}

type copySegmentInspector struct {
	ctx       context.Context
	meta      *meta
	copyMeta  CopySegmentMeta
	scheduler task.GlobalScheduler

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewCopySegmentInspector(
	ctx context.Context,
	meta *meta,
	copyMeta CopySegmentMeta,
	scheduler task.GlobalScheduler,
) CopySegmentInspector {
	return &copySegmentInspector{
		ctx:       ctx,
		meta:      meta,
		copyMeta:  copyMeta,
		scheduler: scheduler,
		closeChan: make(chan struct{}),
	}
}

func (s *copySegmentInspector) Start() {
	s.reloadFromMeta()
	log.Ctx(s.ctx).Info("start copy segment inspector")
	ticker := time.NewTicker(Params.DataCoordCfg.ImportScheduleInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-s.closeChan:
			log.Ctx(s.ctx).Info("copy segment inspector exited")
			return
		case <-ticker.C:
			s.inspect()
		}
	}
}

func (s *copySegmentInspector) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}

func (s *copySegmentInspector) reloadFromMeta() {
	// Reload InProgress tasks to scheduler on restart
	jobs := s.copyMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobId() < jobs[j].GetJobId()
	})

	for _, job := range jobs {
		tasks := s.copyMeta.GetTaskBy(s.ctx, WithCopyTaskJob(job.GetJobId()))
		for _, task := range tasks {
			if task.GetState() == datapb.ImportTaskStateV2_InProgress {
				s.scheduler.Enqueue(task)
			}
		}
	}
	log.Info("copy segment inspector reloaded tasks from meta",
		zap.Int("jobCount", len(jobs)))
}

func (s *copySegmentInspector) inspect() {
	jobs := s.copyMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobId() < jobs[j].GetJobId()
	})

	for _, job := range jobs {
		tasks := s.copyMeta.GetTaskBy(s.ctx, WithCopyTaskJob(job.GetJobId()))
		for _, task := range tasks {
			switch task.GetState() {
			case datapb.ImportTaskStateV2_Pending:
				s.processPending(task)
			case datapb.ImportTaskStateV2_Failed:
				s.processFailed(task)
			}
		}
	}
}

func (s *copySegmentInspector) processPending(task CopySegmentTask) {
	s.scheduler.Enqueue(task)
}

func (s *copySegmentInspector) processFailed(task CopySegmentTask) {
	// Drop target segments if copy failed
	for _, mapping := range task.GetIdMappings() {
		targetSegID := mapping.GetTargetSegmentId()
		segment := s.meta.GetSegment(s.ctx, targetSegID)
		if segment != nil {
			op := UpdateStatusOperator(targetSegID, commonpb.SegmentState_Dropped)
			err := s.meta.UpdateSegmentsInfo(s.ctx, op)
			if err != nil {
				log.Warn("failed to drop target segment after copy task failed",
					WrapCopySegmentTaskLog(task, zap.Int64("segmentID", targetSegID), zap.Error(err))...)
			} else {
				log.Info("dropped target segment after copy task failed",
					WrapCopySegmentTaskLog(task, zap.Int64("segmentID", targetSegID))...)
			}
		}
	}
}
