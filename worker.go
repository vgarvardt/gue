package gue

import (
	"bytes"
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
	mu       sync.Mutex
	done     bool
	ch       chan struct{}
	id       string
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
		ch:       make(chan struct{}),
	}

	for _, option := range options {
		option(&instance)
	}

	if instance.id == "" {
		instance.id = newID()
	}

	return &instance
}

// Work pulls jobs off the Worker's queue at its interval. This function only
// returns after Shutdown() is called, so it should be run in its own goroutine.
func (w *Worker) Work() {
	defer log.Printf("worker[id=%s] done", w.id)
	for {
		// Try to work a job
		if w.WorkOne() {
			// Since we just did work, non-blocking check whether we should exit
			select {
			case <-w.ch:
				return
			default:
				// continue in loop
			}
		} else {
			// No work found, block until exit or timer expires
			select {
			case <-w.ch:
				return
			case <-time.After(w.interval):
				// continue in loop
			}
		}
	}
}

// WorkOne tries to consume single message from the queue.
func (w *Worker) WorkOne() (didWork bool) {
	j, err := w.c.LockJob(w.queue)
	if err != nil {
		log.Printf("worker[id=%s] attempting to lock job: %v", w.id, err)
		return
	}
	if j == nil {
		return // no job was available
	}
	defer j.Done()
	defer recoverPanic(j)

	didWork = true

	wf, ok := w.wm[j.Type]
	if !ok {
		msg := fmt.Sprintf("worker[id=%s] unknown job type: %q", w.id, j.Type)
		log.Println(msg)
		if err = j.Error(msg); err != nil {
			log.Printf("attempting to save error on job %d: %v", j.ID, err)
		}
		return
	}

	if err = wf(j); err != nil {
		if jErr := j.Error(err.Error()); jErr != nil {
			log.Printf("worker[id=%s] got an error (%v) when tried to mark job as errored (%v)", w.id, jErr, err)
		}
		return
	}

	if err = j.Delete(); err != nil {
		log.Printf("worker[id=%s] attempting to delete job %d: %v", w.id, j.ID, err)
	}
	log.Printf("worker[id=%s] event=job_worked job_id=%d job_type=%s", w.id, j.ID, j.Type)
	return
}

// Shutdown tells the worker to finish processing its current job and then stop.
// There is currently no timeout for in-progress jobs. This function blocks
// until the Worker has stopped working. It should only be called on an active
// Worker.
func (w *Worker) Shutdown() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.done {
		return
	}

	log.Printf("worker[id=%s]  shutting down gracefully...", w.id)
	w.ch <- struct{}{}
	w.done = true
	close(w.ch)
}

// recoverPanic tries to handle panics in job execution.
// A stacktrace is stored into Job last_error.
func recoverPanic(j *Job) {
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
		if err := j.Error(stacktrace); err != nil {
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
	mu       sync.Mutex
	done     bool
	id       string
}

// NewWorkerPool creates a new WorkerPool with count workers using the Client c.
//
// Each Worker in the pool default to an interval of 5 seconds, which can be
// overridden by PoolWakeInterval option. The default queue is the
// nameless queue "", which can be overridden by PoolWorkerQueue option.
func NewWorkerPool(c *Client, wm WorkMap, count int, options ...WorkerPoolOption) *WorkerPool {
	instance := WorkerPool{
		wm:       wm,
		interval: defaultWakeInterval,
		queue:    defaultQueueName,
		c:        c,
		workers:  make([]*Worker, count),
	}

	for _, option := range options {
		option(&instance)
	}

	if instance.id == "" {
		instance.id = newID()
	}

	return &instance
}

// Start starts all of the Workers in the WorkerPool.
func (w *WorkerPool) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.workers {
		w.workers[i] = NewWorker(w.c, w.wm, WorkerID(fmt.Sprintf("%s/worker-%d", w.id, i)))
		w.workers[i].interval = w.interval
		w.workers[i].queue = w.queue
		go w.workers[i].Work()
	}
}

// Shutdown sends a Shutdown signal to each of the Workers in the WorkerPool and
// waits for them all to finish shutting down.
func (w *WorkerPool) Shutdown() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.done {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(w.workers))

	for _, worker := range w.workers {
		go func(worker *Worker) {
			// If Shutdown is called before Start has been called,
			// then these are nil, so don't try to close them
			if worker != nil {
				worker.Shutdown()
			}
			wg.Done()
		}(worker)
	}
	wg.Wait()
	w.done = true
}
