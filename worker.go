package gue

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

const (
	defaultWakeInterval = 5 * time.Second
	defaultQueueName    = ""
)

// WorkFunc is a function that performs a Job. If an error is returned, the job
// is re-enqueued with exponential backoff.
type WorkFunc func(j *Job) error

// WorkMap is a map of Job names to WorkFuncs that are used to perform Jobs of a
// given type.
type WorkMap map[string]WorkFunc

// Worker is a single worker that pulls jobs off the specified queue. If no Job
// is found, the Worker will sleep for interval seconds.
type Worker struct {
	wm       WorkMap
	interval time.Duration
	queue    string
	c        *Client
	id       string
	mu       sync.Mutex
	running  bool
}

// NewWorker returns a Worker that fetches Jobs from the Client and executes
// them using WorkMap. If the type of Job is not registered in the WorkMap, it's
// considered an error and the job is re-enqueued with a backoff.
//
// Workers default to an interval of 5 seconds, which can be overridden by
// WakeInterval option.
// The default queue is the nameless queue "", which can be overridden by
// WorkerQueue option.
func NewWorker(c *Client, wm WorkMap, options ...WorkerOption) *Worker {
	instance := Worker{
		interval: defaultWakeInterval,
		queue:    defaultQueueName,
		c:        c,
		wm:       wm,
	}

	for _, option := range options {
		option(&instance)
	}

	if instance.id == "" {
		instance.id = newID()
	}

	return &instance
}

// Start pulls jobs off the Worker's queue at its interval. This function runs
// in its own goroutine, use cancel context to shut it down.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running {
		return fmt.Errorf("worker[id=%s] already running", w.id)
	}

	w.running = true
	go func() {
		defer func() {
			w.running = false
			log.Printf("worker[id=%s] done", w.id)
		}()

		for {
			// Try to work a job
			if w.WorkOne(ctx) {
				// Since we just did work, non-blocking check whether we should exit
				select {
				case <-ctx.Done():
					return
				default:
					// continue in loop
				}
			} else {
				// No work found, block until exit or timer expires
				select {
				case <-ctx.Done():
					return
				case <-time.After(w.interval):
					// continue in loop
				}
			}
		}
	}()

	return nil
}

// WorkOne tries to consume single message from the queue.
func (w *Worker) WorkOne(ctx context.Context) (didWork bool) {
	j, err := w.c.LockJob(ctx, w.queue)
	if err != nil {
		log.Printf("worker[id=%s] attempting to lock job: %v", w.id, err)
		return
	}
	if j == nil {
		return // no job was available
	}
	defer j.Done(ctx)
	defer recoverPanic(ctx, j)

	didWork = true

	wf, ok := w.wm[j.Type]
	if !ok {
		msg := fmt.Sprintf("worker[id=%s] unknown job type: %q", w.id, j.Type)
		log.Println(msg)
		if err = j.Error(ctx, msg); err != nil {
			log.Printf("attempting to save error on job %d: %v", j.ID, err)
		}
		return
	}

	if err = wf(j); err != nil {
		if jErr := j.Error(ctx, err.Error()); jErr != nil {
			log.Printf("worker[id=%s] got an error (%v) when tried to mark job as errored (%v)", w.id, jErr, err)
		}
		return
	}

	if err = j.Delete(ctx); err != nil {
		log.Printf("worker[id=%s] attempting to delete job %d: %v", w.id, j.ID, err)
	}
	log.Printf("worker[id=%s] event=job_worked job_id=%d job_type=%s", w.id, j.ID, j.Type)
	return
}

// recoverPanic tries to handle panics in job execution.
// A stacktrace is stored into Job last_error.
func recoverPanic(ctx context.Context, j *Job) {
	if r := recover(); r != nil {
		// record an error on the job with panic message and stacktrace
		stackBuf := make([]byte, 1024)
		n := runtime.Stack(stackBuf, false)

		buf := &bytes.Buffer{}
		fmt.Fprintf(buf, "%v\n", r)
		fmt.Fprintln(buf, string(stackBuf[:n]))
		fmt.Fprintln(buf, "[...]")
		stacktrace := buf.String()
		log.Printf("event=panic job_id=%d job_type=%s\n%s", j.ID, j.Type, stacktrace)
		if err := j.Error(ctx, stacktrace); err != nil {
			log.Printf("attempting to save error on job %d: %v", j.ID, err)
		}
	}
}

func newID() string {
	hasher := md5.New()
	// nolint:errcheck
	hasher.Write([]byte(time.Now().Format(time.RFC3339Nano)))
	return hex.EncodeToString(hasher.Sum(nil))[:6]
}

// WorkerPool is a pool of Workers, each working jobs from the queue queue
// at the specified interval using the WorkMap.
type WorkerPool struct {
	wm       WorkMap
	interval time.Duration
	queue    string
	c        *Client
	workers  []*Worker
	id       string
	mu       sync.Mutex
	running  bool
}

// NewWorkerPool creates a new WorkerPool with count workers using the Client c.
//
// Each Worker in the pool default to an interval of 5 seconds, which can be
// overridden by PoolWakeInterval option. The default queue is the
// nameless queue "", which can be overridden by PoolWorkerQueue option.
func NewWorkerPool(c *Client, wm WorkMap, poolSize int, options ...WorkerPoolOption) *WorkerPool {
	instance := WorkerPool{
		wm:       wm,
		interval: defaultWakeInterval,
		queue:    defaultQueueName,
		c:        c,
		workers:  make([]*Worker, poolSize),
	}

	for _, option := range options {
		option(&instance)
	}

	if instance.id == "" {
		instance.id = newID()
	}

	return &instance
}

// Start starts all of the Workers in the WorkerPool in own goroutines.
// Use cancel context to shut them down
func (w *WorkerPool) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running {
		return fmt.Errorf("worker pool[id=%s] already running", w.id)
	}

	workerCtx := make([]context.Context, len(w.workers))
	cancelFunc := make([]context.CancelFunc, len(w.workers))
	w.running = true
	for i := range w.workers {
		w.workers[i] = NewWorker(w.c, w.wm, WorkerID(fmt.Sprintf("%s/worker-%d", w.id, i)))
		w.workers[i].interval = w.interval
		w.workers[i].queue = w.queue

		workerCtx[i], cancelFunc[i] = context.WithCancel(ctx)
		if err := w.workers[i].Start(workerCtx[i]); err != nil {
			return err
		}
	}

	go func(cancelFunc []context.CancelFunc) {
		defer func() {
			w.running = false
			log.Printf("worker pool[id=%s] done", w.id)
		}()

		<-ctx.Done()
	}(cancelFunc)

	return nil
}
