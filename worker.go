package gue

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/vgarvardt/gue/v3/adapter"
)

type PollStrategy string

const (
	defaultPollInterval = 5 * time.Second
	defaultQueueName    = ""
	// PriorityPollStrategy cares about the priority first to lock top priority jobs first even if there are available
	//ones that should be executed earlier but with lower priority.
	PriorityPollStrategy PollStrategy = "OrderByPriority"
	// RunAtPollStrategy cares about the scheduled time first to lock earliest to execute jobs first even if there
	// are ones with a higher priority scheduled to a later time but already eligible for execution
	RunAtPollStrategy PollStrategy = "OrderByRunAtPriority"
)

// WorkFunc is a function that performs a Job. If an error is returned, the job
// is re-enqueued with exponential backoff.
type WorkFunc func(ctx context.Context, j *Job) error

// WorkMap is a map of Job names to WorkFuncs that are used to perform Jobs of a
// given type.
type WorkMap map[string]WorkFunc

// pollFunc is a function that polls the db for a job to be worked on
type pollFunc func(context.Context, string) (*Job, error)

// Worker is a single worker that pulls jobs off the specified queue. If no Job
// is found, the Worker will sleep for interval seconds.
type Worker struct {
	wm           WorkMap
	interval     time.Duration
	queue        string
	c            *Client
	id           string
	logger       adapter.Logger
	mu           sync.Mutex
	running      bool
	pollStrategy PollStrategy
	pollFunc     pollFunc
}

// NewWorker returns a Worker that fetches Jobs from the Client and executes
// them using WorkMap. If the type of Job is not registered in the WorkMap, it's
// considered an error and the job is re-enqueued with a backoff.
//
// Worker defaults to a poll interval of 5 seconds, which can be overridden by
// WithWorkerPollInterval option.
// The default queue is the nameless queue "", which can be overridden by
// WithWorkerQueue option.
func NewWorker(c *Client, wm WorkMap, options ...WorkerOption) *Worker {
	w := Worker{
		interval:     defaultPollInterval,
		queue:        defaultQueueName,
		c:            c,
		wm:           wm,
		logger:       adapter.NoOpLogger{},
		pollStrategy: PriorityPollStrategy,
	}

	for _, option := range options {
		option(&w)
	}

	if w.id == "" {
		w.id = newID()
	}

	switch w.pollStrategy {
	case RunAtPollStrategy:
		w.pollFunc = w.c.LockNextScheduledJob
	default:
		w.pollFunc = w.c.LockJob
	}

	w.logger = w.logger.With(adapter.F("worker-id", w.id))

	return &w
}

// Start pulls jobs off the Worker's queue at its interval. This function runs
// in its own goroutine, use cancel context to shut it down.
//
// Deprecated: use Run instead of Start. Start leaks resources and does not wait
// for shutdown to complete.
func (w *Worker) Start(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() {
		errc <- w.runLock(ctx, func(ctx context.Context) error {
			errc <- nil
			return w.runLoop(ctx)
		})
	}()
	return <-errc
}

// Run pulls jobs off the Worker's queue at its interval. This function does
// not run in its own goroutine so it’s possible to wait for completion. Use
// context cancellation to shut it down.
func (w *Worker) Run(ctx context.Context) error {
	return w.runLock(ctx, w.runLoop)
}

// runLock runs function f under a run lock. Any attempt to call runLock concurrently
// will return an error.
func (w *Worker) runLock(ctx context.Context, f func(ctx context.Context) error) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("worker[id=%s] is already running", w.id)
	}
	w.running = true
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.running = false
		w.mu.Unlock()
	}()

	return f(ctx)
}

// runLoop pulls jobs off the Worker's queue at its interval.
func (w *Worker) runLoop(ctx context.Context) error {
	defer w.logger.Info("Worker finished")

	var timer *time.Timer
	for {
		// Try to work a job
		if w.WorkOne(ctx) {
			// Since we just did work, non-blocking check whether we should exit
			select {
			case <-ctx.Done():
				return nil
			default:
				continue
			}
		}

		// Reset or create the timer; time.After is leaky
		// on context cancellation since we can’t stop it.
		if timer != nil {
			timer.Reset(w.interval)
		} else {
			timer = time.NewTimer(w.interval)
			defer timer.Stop()
		}

		// No work found, block until exit or timer expires
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			continue
		}
	}
}

// WorkOne tries to consume single message from the queue.
func (w *Worker) WorkOne(ctx context.Context) (didWork bool) {
	j, err := w.pollFunc(ctx, w.queue)

	if err != nil {
		w.logger.Error("Worker failed to lock a job", adapter.Err(err))
		return
	}
	if j == nil {
		return // no job was available
	}

	ll := w.logger.With(adapter.F("job-id", j.ID), adapter.F("job-type", j.Type))

	defer func() {
		if err := j.Done(ctx); err != nil {
			ll.Error("Failed to mark job as done", adapter.Err(err))
		}
	}()
	defer recoverPanic(ctx, ll, j)

	didWork = true

	wf, ok := w.wm[j.Type]
	if !ok {
		ll.Error("Got a job with unknown type")
		if err = j.Error(ctx, fmt.Sprintf("worker[id=%s] unknown job type: %q", w.id, j.Type)); err != nil {
			ll.Error("Got an error on setting an error to unknown job", adapter.Err(err))
		}
		return
	}

	if err = wf(ctx, j); err != nil {
		if jErr := j.Error(ctx, err.Error()); jErr != nil {
			ll.Error("Got an error on setting an error to an errored job", adapter.Err(jErr), adapter.F("job-error", err))
		}
		return
	}

	if err = j.Delete(ctx); err != nil {
		ll.Error("Got an error on deleting a job", adapter.Err(err))
	}
	ll.Debug("Job finished")
	return
}

// recoverPanic tries to handle panics in job execution.
// A stacktrace is stored into Job last_error.
func recoverPanic(ctx context.Context, logger adapter.Logger, j *Job) {
	if r := recover(); r != nil {
		// record an error on the job with panic message and stacktrace
		stackBuf := make([]byte, 1024)
		n := runtime.Stack(stackBuf, false)

		buf := &bytes.Buffer{}
		fmt.Fprintf(buf, "%v\n", r)
		fmt.Fprintln(buf, string(stackBuf[:n]))
		fmt.Fprintln(buf, "[...]")
		stacktrace := buf.String()

		logger.Error("Job panicked", adapter.F("stacktrace", stacktrace))
		if err := j.Error(ctx, stacktrace); err != nil {
			logger.Error("Got an error on setting an error to a panicked job", adapter.Err(err))
		}
	}
}

// WorkerPool is a pool of Workers, each working jobs from the queue queue
// at the specified interval using the WorkMap.
type WorkerPool struct {
	wm           WorkMap
	interval     time.Duration
	queue        string
	c            *Client
	workers      []*Worker
	id           string
	logger       adapter.Logger
	mu           sync.Mutex
	running      bool
	pollStrategy PollStrategy
}

// NewWorkerPool creates a new WorkerPool with count workers using the Client c.
//
// Each Worker in the pool default to a poll interval of 5 seconds, which can be
// overridden by WithPoolPollInterval option. The default queue is the
// nameless queue "", which can be overridden by WithPoolQueue option.
func NewWorkerPool(c *Client, wm WorkMap, poolSize int, options ...WorkerPoolOption) *WorkerPool {
	w := WorkerPool{
		wm:           wm,
		interval:     defaultPollInterval,
		queue:        defaultQueueName,
		c:            c,
		workers:      make([]*Worker, poolSize),
		logger:       adapter.NoOpLogger{},
		pollStrategy: PriorityPollStrategy,
	}

	for _, option := range options {
		option(&w)
	}

	if w.id == "" {
		w.id = newID()
	}

	w.logger = w.logger.With(adapter.F("worker-pool-id", w.id))

	for i := range w.workers {
		w.workers[i] = NewWorker(
			w.c,
			w.wm,
			WithWorkerPollInterval(w.interval),
			WithWorkerQueue(w.queue),
			WithWorkerID(fmt.Sprintf("%s/worker-%d", w.id, i)),
			WithWorkerLogger(w.logger),
			WithWorkerPollStrategy(w.pollStrategy),
		)
	}
	return &w
}

// Start starts all of the Workers in the WorkerPool in own goroutines.
// Use cancel context to shut them down.
//
// Deprecated: use Run instead of Start. Start leaks resources and does not wait
// for shutdown to complete.
func (w *WorkerPool) Start(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() {
		// Note that the previous behavior was to start workers sequentially
		// and return on first error without shutting down all previously
		// started workers. The current behavior is correct, i.e. workers
		// are shut down on any error, but we don’t return an error on
		// startup. That said, the Start method actually never returned
		// non-nil error from worker in previous implementation so it’s
		// OK to just use runGroup here.
		//
		errc <- w.runLock(ctx, func(ctx context.Context) error {
			errc <- nil
			return w.runGroup(ctx)
		})
	}()
	return <-errc
}

// Run runs all of the Workers in the WorkerPool in own goroutines.
// Run blocks until all workers exit. Use context cancellation for
// shutdown.
func (w *WorkerPool) Run(ctx context.Context) error {
	return w.runLock(ctx, w.runGroup)
}

// runLock runs function f under a run lock. Any attempt to call runLock concurrently
// will return an error.
func (w *WorkerPool) runLock(ctx context.Context, f func(ctx context.Context) error) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("worker pool[id=%s] already running", w.id)
	}
	w.running = true
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.running = false
		w.mu.Unlock()
	}()

	return f(ctx)
}

// runGroup starts all of the Workers in the WorkerPool in own goroutines
// managed by errgroup.Group.
func (w *WorkerPool) runGroup(ctx context.Context) error {
	defer w.logger.Info("Worker pool finished")

	grp, ctx := errgroup.WithContext(ctx)
	for i := range w.workers {
		worker := w.workers[i]
		grp.Go(func() error {
			return worker.Run(ctx)
		})
	}
	return grp.Wait()
}
