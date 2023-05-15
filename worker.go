package gue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.com/vgarvardt/gue/v5/adapter"
)

// PollStrategy determines how the DB is queried for the next job to work on
type PollStrategy string

const (
	defaultPollInterval = 5 * time.Second
	defaultQueueName    = ""

	defaultPanicStackBufSize = 1024

	// PriorityPollStrategy cares about the priority first to lock top priority jobs first even if there are available
	// ones that should be executed earlier but with lower priority.
	PriorityPollStrategy PollStrategy = "OrderByPriority"
	// RunAtPollStrategy cares about the scheduled time first to lock earliest to execute jobs first even if there
	// are ones with a higher priority scheduled to a later time but already eligible for execution
	RunAtPollStrategy PollStrategy = "OrderByRunAtPriority"
)

// WorkFunc is the handler function that performs the Job. If an error is returned, the Job
// is either re-enqueued with the given backoff or is discarded based on the worker backoff strategy
// and returned error.
//
// Modifying Job fields and calling any methods that are modifying its state within the handler may lead to undefined
// behaviour. Please never do this.
type WorkFunc func(ctx context.Context, j *Job) error

// HookFunc is a function that may react to a Job lifecycle events. All the callbacks are being executed synchronously,
// so be careful with the long-running locking operations. Hooks do not return an error, therefore they can not and
// must not be used to affect the Job execution flow, e.g. cancel it - this is the WorkFunc responsibility.
// Modifying Job fields and calling any methods that are modifying its state within hooks may lead to undefined
// behaviour. Please never do this.
//
// Depending on the event err parameter may be empty or not - check the event description for its meaning.
type HookFunc func(ctx context.Context, j *Job, err error)

// WorkMap is a map of Job names to WorkFuncs that are used to perform Jobs of a
// given type.
type WorkMap map[string]WorkFunc

// pollFunc is a function that queries the DB for the next job to work on
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

	graceful    bool
	gracefulCtx func() context.Context

	tracer trace.Tracer
	meter  metric.Meter

	hooksJobLocked      []HookFunc
	hooksUnknownJobType []HookFunc
	hooksJobDone        []HookFunc

	mWorked   metric.Int64Counter
	mDuration metric.Int64Histogram

	panicStackBufSize int
	spanWorkOneNoJob  bool
}

// NewWorker returns a Worker that fetches Jobs from the Client and executes
// them using WorkMap. If the type of Job is not registered in the WorkMap, it's
// considered an error and the job is re-enqueued with a backoff.
//
// Worker defaults to a poll interval of 5 seconds, which can be overridden by
// WithWorkerPollInterval option.
// The default queue is the nameless queue "", which can be overridden by
// WithWorkerQueue option.
func NewWorker(c *Client, wm WorkMap, options ...WorkerOption) (*Worker, error) {
	w := Worker{
		interval:     defaultPollInterval,
		queue:        defaultQueueName,
		c:            c,
		id:           RandomStringID(),
		wm:           wm,
		logger:       adapter.NoOpLogger{},
		pollStrategy: PriorityPollStrategy,
		tracer:       trace.NewNoopTracerProvider().Tracer("noop"),
		meter:        noop.NewMeterProvider().Meter("noop"),

		panicStackBufSize: defaultPanicStackBufSize,
	}

	for _, option := range options {
		option(&w)
	}

	switch w.pollStrategy {
	case RunAtPollStrategy:
		w.pollFunc = w.c.LockNextScheduledJob
	default:
		w.pollFunc = w.c.LockJob
	}

	w.logger = w.logger.With(adapter.F("worker-id", w.id))

	return &w, w.initMetrics()
}

// Run pulls jobs off the Worker's queue at its interval. This function does
// not run in its own goroutine, so it’s possible to wait for completion. Use
// context cancellation to shut it down.
func (w *Worker) Run(ctx context.Context) error {
	return RunLock(ctx, w.runLoop, &w.mu, &w.running, w.id)
}

// runLoop pulls jobs off the Worker's queue at its interval.
func (w *Worker) runLoop(ctx context.Context) error {
	defer w.logger.Info("Worker finished")

	timer := time.NewTimer(w.interval)
	defer timer.Stop()

	for {
		handlerCtx := ctx
		if w.graceful {
			if w.gracefulCtx == nil {
				handlerCtx = context.Background()
			} else {
				handlerCtx = w.gracefulCtx()
			}
		}

		// Try to work a job
		if w.WorkOne(handlerCtx) {
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
		timer.Reset(w.interval)

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
	ctx, span := w.tracer.Start(ctx, "Worker.WorkOne")
	// worker option is set to generate spans even when no job is found - let it be
	if w.spanWorkOneNoJob {
		defer span.End()
	}

	j, err := w.pollFunc(ctx, w.queue)
	if err != nil {
		span.RecordError(fmt.Errorf("woker failed to lock a job: %w", err))
		w.mWorked.Add(ctx, 1, metric.WithAttributes(attrJobType.String(""), attrSuccess.Bool(false)))
		w.logger.Error("Worker failed to lock a job", adapter.Err(err))

		for _, hook := range w.hooksJobLocked {
			hook(ctx, nil, err)
		}
		return
	}
	if j == nil {
		return // no job was available
	}

	// at this point we have a job, so we need to ensure that span will be generated
	if !w.spanWorkOneNoJob {
		defer span.End()
	}

	processingStartedAt := time.Now()
	span.SetAttributes(
		attribute.String("job-id", j.ID.String()),
		attribute.String("job-queue", j.Queue),
		attribute.String("job-type", j.Type),
	)

	ll := w.logger.With(adapter.F("job-id", j.ID.String()), adapter.F("job-type", j.Type))

	defer func() {
		if err := j.Done(ctx); err != nil {
			span.RecordError(fmt.Errorf("failed to mark job as done: %w", err))
			ll.Error("Failed to mark job as done", adapter.Err(err))
		}

		w.mDuration.Record(
			ctx,
			time.Since(processingStartedAt).Milliseconds(),
			metric.WithAttributes(attrJobType.String(j.Type)),
		)
	}()
	defer w.recoverPanic(ctx, ll, j)

	for _, hook := range w.hooksJobLocked {
		hook(ctx, j, nil)
	}

	didWork = true

	wf, ok := w.wm[j.Type]
	if !ok {
		w.mWorked.Add(ctx, 1, metric.WithAttributes(attrJobType.String(j.Type), attrSuccess.Bool(false)))

		span.RecordError(fmt.Errorf("job with unknown type: %q", j.Type))
		ll.Error("Got a job with unknown type")

		errUnknownType := fmt.Errorf("worker[id=%s] unknown job type: %q", w.id, j.Type)
		if err = j.Error(ctx, errUnknownType); err != nil {
			span.RecordError(fmt.Errorf("failed to mark job as error: %w", err))
			ll.Error("Got an error on setting an error to unknown job", adapter.Err(err))
		}

		for _, hook := range w.hooksUnknownJobType {
			hook(ctx, j, errUnknownType)
		}

		return
	}

	if err = wf(ctx, j); err != nil {
		w.mWorked.Add(ctx, 1, metric.WithAttributes(attrJobType.String(j.Type), attrSuccess.Bool(false)))

		for _, hook := range w.hooksJobDone {
			hook(ctx, j, err)
		}

		if jErr := j.Error(ctx, err); jErr != nil {
			span.RecordError(fmt.Errorf("failed to mark job as error: %w", err))
			ll.Error("Got an error on setting an error to an errored job", adapter.Err(jErr), adapter.F("job-error", err))
		}

		return
	}

	for _, hook := range w.hooksJobDone {
		hook(ctx, j, nil)
	}

	err = j.Delete(ctx)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to delete finished job: %w", err))
		ll.Error("Got an error on deleting a job", adapter.Err(err))
	}

	w.mWorked.Add(ctx, 1, metric.WithAttributes(attrJobType.String(j.Type), attrSuccess.Bool(err == nil)))
	ll.Debug("Job finished")
	return
}

func (w *Worker) initMetrics() (err error) {
	if w.mWorked, err = w.meter.Int64Counter(
		"gue_worker_jobs_worked",
		metric.WithDescription("Number of jobs processed"),
		metric.WithUnit("1"),
	); err != nil {
		return fmt.Errorf("could not register mWorked metric: %w", err)
	}

	if w.mDuration, err = w.meter.Int64Histogram(
		"gue_worker_jobs_duration",
		metric.WithDescription("Duration of the single locked job to be processed with all the hooks"),
		metric.WithUnit("ms"),
	); err != nil {
		return fmt.Errorf("could not register mDuration metric: %w", err)
	}

	return nil
}

// recoverPanic tries to handle panics in job execution.
// A stacktrace is stored into Job last_error.
func (w *Worker) recoverPanic(ctx context.Context, logger adapter.Logger, j *Job) {
	if r := recover(); r != nil {
		ctx, span := w.tracer.Start(ctx, "Worker.recoverPanic")
		defer span.End()

		// record an error on the job with panic message and stacktrace
		stackBuf := make([]byte, w.panicStackBufSize)
		n := runtime.Stack(stackBuf, false)

		buf := new(bytes.Buffer)
		_, printRErr := fmt.Fprintf(buf, "%v\n", r)
		_, printStackErr := fmt.Fprintln(buf, string(stackBuf[:n]))
		_, printEllipsisErr := fmt.Fprintln(buf, "[...]")
		stacktrace := buf.String()

		if err := multierr.Combine(printRErr, printStackErr, printEllipsisErr); err != nil {
			logger.Error("Could not build panicked job stacktrace", adapter.Err(err), adapter.F("runtime-stack", string(stackBuf[:n])))
		}

		w.mWorked.Add(ctx, 1, metric.WithAttributes(attrJobType.String(j.Type), attrSuccess.Bool(false)))
		span.RecordError(errors.New("job panicked"), trace.WithAttributes(attribute.String("stacktrace", stacktrace)))
		logger.Error("Job panicked", adapter.F("stacktrace", stacktrace))

		if err := j.Error(ctx, errors.New(stacktrace)); err != nil {
			span.RecordError(fmt.Errorf("failed to mark panicked job as error: %w", err))
			logger.Error("Got an error on setting an error to a panicked job", adapter.Err(err))
		}
	}
}

// WorkerPool is a pool of Workers, each working jobs from the queue
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

	graceful    bool
	gracefulCtx func() context.Context

	tracer trace.Tracer
	meter  metric.Meter

	hooksJobLocked      []HookFunc
	hooksUnknownJobType []HookFunc
	hooksJobDone        []HookFunc

	panicStackBufSize int
	spanWorkOneNoJob  bool
}

// NewWorkerPool creates a new WorkerPool with count workers using the Client c.
//
// Each Worker in the pool default to a poll interval of 5 seconds, which can be
// overridden by WithPoolPollInterval option. The default queue is the
// nameless queue "", which can be overridden by WithPoolQueue option.
func NewWorkerPool(c *Client, wm WorkMap, poolSize int, options ...WorkerPoolOption) (*WorkerPool, error) {
	w := WorkerPool{
		wm:           wm,
		interval:     defaultPollInterval,
		queue:        defaultQueueName,
		c:            c,
		id:           RandomStringID(),
		workers:      make([]*Worker, poolSize),
		logger:       adapter.NoOpLogger{},
		pollStrategy: PriorityPollStrategy,
		tracer:       trace.NewNoopTracerProvider().Tracer("noop"),
		meter:        noop.NewMeterProvider().Meter("noop"),

		panicStackBufSize: defaultPanicStackBufSize,
	}

	for _, option := range options {
		option(&w)
	}

	w.logger = w.logger.With(adapter.F("worker-pool-id", w.id))

	var err error
	for i := range w.workers {
		w.workers[i], err = NewWorker(
			w.c,
			w.wm,
			WithWorkerPollInterval(w.interval),
			WithWorkerQueue(w.queue),
			WithWorkerID(fmt.Sprintf("%s/worker-%d", w.id, i)),
			WithWorkerLogger(w.logger),
			WithWorkerPollStrategy(w.pollStrategy),
			WithWorkerTracer(w.tracer),
			WithWorkerMeter(w.meter),
			WithWorkerHooksJobLocked(w.hooksJobLocked...),
			WithWorkerHooksUnknownJobType(w.hooksUnknownJobType...),
			WithWorkerHooksJobDone(w.hooksJobDone...),
			WithWorkerPanicStackBufSize(w.panicStackBufSize),
			WithWorkerSpanWorkOneNoJob(w.spanWorkOneNoJob),
		)

		if err != nil {
			return nil, fmt.Errorf("could not init worker instance: %w", err)
		}

		w.workers[i].graceful = w.graceful
		w.workers[i].gracefulCtx = w.gracefulCtx
	}

	return &w, nil
}

// Run runs all the Workers in the WorkerPool in own goroutines.
// Run blocks until all workers exit. Use context cancellation for
// shutdown.
func (w *WorkerPool) Run(ctx context.Context) error {
	return RunLock(ctx, w.runGroup, &w.mu, &w.running, w.id)
}

// WorkOne tries to consume single message from the queue.
func (w *WorkerPool) WorkOne(ctx context.Context) (didWork bool) {
	return w.workers[0].WorkOne(ctx)
}

// runGroup starts all the Workers in the WorkerPool in own goroutines
// managed by errgroup.Group.
func (w *WorkerPool) runGroup(ctx context.Context) error {
	defer w.logger.Info("Worker pool finished")

	grp, ctx := errgroup.WithContext(ctx)
	for i := range w.workers {
		idx := i
		worker := w.workers[idx]
		grp.Go(func() error {
			return worker.Run(setWorkerIdx(ctx, idx))
		})
	}

	return grp.Wait()
}
