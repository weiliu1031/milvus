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

	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

type CopySegmentMeta interface {
	// Job operations
	AddJob(ctx context.Context, job CopySegmentJob) error
	UpdateJob(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error
	GetJob(ctx context.Context, jobID int64) CopySegmentJob
	GetJobBy(ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob
	CountJobBy(ctx context.Context, filters ...CopySegmentJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error

	// Task operations
	AddTask(ctx context.Context, task CopySegmentTask) error
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateCopySegmentTaskAction) error
	GetTask(ctx context.Context, taskID int64) CopySegmentTask
	GetTaskBy(ctx context.Context, filters ...CopySegmentTaskFilter) []CopySegmentTask
	RemoveTask(ctx context.Context, taskID int64) error
}

type copySegmentTasks struct {
	tasks map[int64]CopySegmentTask
}

func newCopySegmentTasks() *copySegmentTasks {
	return &copySegmentTasks{
		tasks: make(map[int64]CopySegmentTask),
	}
}

func (t *copySegmentTasks) get(taskID int64) CopySegmentTask {
	ret, ok := t.tasks[taskID]
	if !ok {
		return nil
	}
	return ret
}

func (t *copySegmentTasks) add(task CopySegmentTask) {
	t.tasks[task.GetTaskId()] = task
}

func (t *copySegmentTasks) remove(taskID int64) {
	delete(t.tasks, taskID)
}

func (t *copySegmentTasks) listTasks() []CopySegmentTask {
	return maps.Values(t.tasks)
}

type copySegmentMeta struct {
	mu           lock.RWMutex // guards jobs and tasks
	jobs         map[int64]CopySegmentJob
	tasks        *copySegmentTasks
	catalog      metastore.DataCoordCatalog
	meta         *meta
	snapshotMeta *snapshotMeta
}

// NewCopySegmentMeta creates a new CopySegmentMeta instance with provided dependencies.
func NewCopySegmentMeta(ctx context.Context, catalog metastore.DataCoordCatalog, meta *meta, snapshotMeta *snapshotMeta) (CopySegmentMeta, error) {
	restoredJobs, err := catalog.ListCopySegmentJobs(ctx)
	if err != nil {
		return nil, err
	}
	restoredTasks, err := catalog.ListCopySegmentTasks(ctx)
	if err != nil {
		return nil, err
	}

	tasks := newCopySegmentTasks()
	copySegmentMeta := &copySegmentMeta{
		catalog:      catalog,
		meta:         meta,
		snapshotMeta: snapshotMeta,
	}

	for _, task := range restoredTasks {
		t := &copySegmentTask{
			copyMeta:     copySegmentMeta,
			meta:         meta,
			snapshotMeta: snapshotMeta,
			tr:           timerecord.NewTimeRecorder("copy segment task"),
			times:        taskcommon.NewTimes(),
		}
		t.task.Store(task)
		tasks.add(t)
	}

	jobs := make(map[int64]CopySegmentJob)
	for _, job := range restoredJobs {
		jobs[job.GetJobId()] = &copySegmentJob{
			CopySegmentJob: job,
			tr:             timerecord.NewTimeRecorder("copy segment job"),
		}
	}

	copySegmentMeta.jobs = jobs
	copySegmentMeta.tasks = tasks
	return copySegmentMeta, nil
}

func (m *copySegmentMeta) AddJob(ctx context.Context, job CopySegmentJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.catalog.SaveCopySegmentJob(ctx, job.(*copySegmentJob).CopySegmentJob)
	if err != nil {
		return err
	}
	m.jobs[job.GetJobId()] = job
	return nil
}

func (m *copySegmentMeta) UpdateJob(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job, ok := m.jobs[jobID]; ok {
		updatedJob := job.Clone()
		for _, action := range actions {
			action(updatedJob)
		}
		err := m.catalog.SaveCopySegmentJob(ctx, updatedJob.(*copySegmentJob).CopySegmentJob)
		if err != nil {
			return err
		}
		m.jobs[updatedJob.GetJobId()] = updatedJob
	}
	return nil
}

func (m *copySegmentMeta) GetJob(ctx context.Context, jobID int64) CopySegmentJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[jobID]
}

func (m *copySegmentMeta) GetJobBy(ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getJobBy(filters...)
}

func (m *copySegmentMeta) getJobBy(filters ...CopySegmentJobFilter) []CopySegmentJob {
	ret := make([]CopySegmentJob, 0)
OUTER:
	for _, job := range m.jobs {
		for _, f := range filters {
			if !f(job) {
				continue OUTER
			}
		}
		ret = append(ret, job)
	}
	return ret
}

func (m *copySegmentMeta) CountJobBy(ctx context.Context, filters ...CopySegmentJobFilter) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.getJobBy(filters...))
}

func (m *copySegmentMeta) RemoveJob(ctx context.Context, jobID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[jobID]; ok {
		err := m.catalog.DropCopySegmentJob(ctx, jobID)
		if err != nil {
			return err
		}
		delete(m.jobs, jobID)
	}
	return nil
}

func (m *copySegmentMeta) AddTask(ctx context.Context, task CopySegmentTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure the task has meta references
	t := task.(*copySegmentTask)
	t.meta = m.meta
	t.snapshotMeta = m.snapshotMeta

	err := m.catalog.SaveCopySegmentTask(ctx, t.task.Load())
	if err != nil {
		return err
	}
	m.tasks.add(task)
	return nil
}

func (m *copySegmentMeta) UpdateTask(ctx context.Context, taskID int64, actions ...UpdateCopySegmentTaskAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		updatedTask := task.Clone()
		for _, action := range actions {
			action(updatedTask)
		}
		err := m.catalog.SaveCopySegmentTask(ctx, updatedTask.(*copySegmentTask).task.Load())
		if err != nil {
			return err
		}
		// update memory task
		task.(*copySegmentTask).task.Store(updatedTask.(*copySegmentTask).task.Load())
	}
	return nil
}

func (m *copySegmentMeta) GetTask(ctx context.Context, taskID int64) CopySegmentTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks.get(taskID)
}

func (m *copySegmentMeta) GetTaskBy(ctx context.Context, filters ...CopySegmentTaskFilter) []CopySegmentTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]CopySegmentTask, 0)
OUTER:
	for _, task := range m.tasks.listTasks() {
		for _, f := range filters {
			if !f(task) {
				continue OUTER
			}
		}
		ret = append(ret, task)
	}
	return ret
}

func (m *copySegmentMeta) RemoveTask(ctx context.Context, taskID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		err := m.catalog.DropCopySegmentTask(ctx, taskID)
		if err != nil {
			return err
		}
		m.tasks.remove(taskID)
	}
	return nil
}
