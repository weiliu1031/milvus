package job

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// JobScheduler schedules jobs,
// all jobs within the same collection will run sequentially
const (
	collectionQueueCap = 64
	waitQueueCap       = 512
)

type jobQueue chan Job

type JobScheduler struct {
	stopCh chan struct{}
	wg     sync.WaitGroup

	processors *typeutil.ConcurrentSet[int64] // Collections of having processor
	queues     map[int64]jobQueue             // CollectionID -> Queue
	waitQueue  jobQueue
}

func NewJobScheduler() *JobScheduler {
	return &JobScheduler{
		stopCh:     make(chan struct{}),
		processors: typeutil.NewConcurrentSet[int64](),
		queues:     make(map[int64]jobQueue),
		waitQueue:  make(jobQueue, waitQueueCap),
	}
}

func (scheduler *JobScheduler) Start(ctx context.Context) {
	scheduler.wg.Add(1)
	go scheduler.schedule(ctx)
}

func (scheduler *JobScheduler) Stop() {
	close(scheduler.stopCh)
	scheduler.wg.Wait()
}

func (scheduler *JobScheduler) schedule(ctx context.Context) {
	defer scheduler.wg.Done()

	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info("JobManager stopped due to context canceled")
				return

			case <-scheduler.stopCh:
				log.Info("JobManager stopped")
				return

			case job := <-scheduler.waitQueue:
				queue, ok := scheduler.queues[job.CollectionID()]
				if !ok {
					queue = make(jobQueue, collectionQueueCap)
					scheduler.queues[job.CollectionID()] = queue
				}
				queue <- job
				scheduler.startProcessor(job.CollectionID(), queue)

			case <-ticker.C:
				for collection, queue := range scheduler.queues {
					if len(queue) > 0 {
						scheduler.startProcessor(collection, queue)
					} else {
						// Release resource if no job for the collection
						delete(scheduler.queues, collection)
					}
				}
			}
		}
	}()
}

func (scheduler *JobScheduler) isStopped() bool {
	select {
	case <-scheduler.stopCh:
		return true
	default:
		return false
	}
}

func (scheduler *JobScheduler) Add(job Job) {
	scheduler.waitQueue <- job
}

func (scheduler *JobScheduler) startProcessor(collection int64, queue jobQueue) {
	if scheduler.isStopped() {
		return
	}
	if !scheduler.processors.Insert(collection) {
		return
	}

	scheduler.wg.Add(1)
	go scheduler.processQueue(collection, queue)
}

// processQueue processes jobs in the given queue,
// it only processes jobs with the number of the length of queue at the time,
// to avoid leaking goroutines
func (scheduler *JobScheduler) processQueue(collection int64, queue jobQueue) {
	defer scheduler.wg.Done()
	defer scheduler.processors.Remove(collection)

	len := len(queue)
	for i := 0; i < len; i++ {
		scheduler.process(<-queue)
	}
}

func (scheduler *JobScheduler) process(job Job) {
	log := log.With(
		zap.Int64("msg-id", job.MsgID()),
		zap.Int64("collection-id", job.CollectionID()))

	defer func() {
		log.Info("start to post-execute job")
		job.PostExecute()
		log.Info("job finished")
		job.Done()
	}()

	log.Info("start to pre-execute job")
	err := job.PreExecute()
	if err != nil {
		log.Warn("failed to pre-execute job", zap.Error(err))
		job.SetError(err)
		return
	}

	log.Info("start to execute job")
	err = job.Execute()
	if err != nil {
		log.Warn("failed to execute job", zap.Error(err))
		job.SetError(err)
	}
}
