package gue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	Meter        = global.MeterProvider().Meter("guex")
	EnqueueMeter instrument.Int64Counter
)

func init() {
	var err error
	EnqueueMeter, err = Meter.Int64Counter("enqueue",
		instrument.WithUnit("1"),
		instrument.WithDescription("tasks insertion metric"),
	)
	if err != nil {
		panic("error create metric")
	}
}

// PollStrategy determines how the DB is queried for the next job to work on
type PollStrategy string

const (
	defaultPollInterval = 5 * time.Second
	defaultQueueName    = ""

	defaultPanicStackBufSize = 1024
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

// WorkerPool is a pool of Workers, each working jobs from the queue
// at the specified interval using the WorkMap.
type WorkerPool struct {
	wm       WorkMap
	interval time.Duration
	queue    []QueueLimit
	client   *Client
	id       string

	ctxCancel  context.CancelFunc
	ctxCancel1 context.CancelCauseFunc
	waitStop   sync.WaitGroup

	logger *zap.Logger

	once sync.Once
	lock sync.RWMutex

	queueRestoreAfter    time.Duration
	queueRestoreInterval time.Duration

	graceful    bool
	gracefulCtx func() context.Context

	hooksUnknownJobType []HookFunc
	hooksJobDone        []HookFunc

	panicStackBufSize int
}

// NewWorkerPool creates a new WorkerPool with count workers using the Client c.
//
// Each Worker in the pool default to a poll interval of 5 seconds, which can be
// overridden by WithPoolInterval option. The default queue is the
// nameless queue "", which can be overridden by WithPoolQueue option.
func NewWorkerPool(c *Client, options ...WorkerPoolOption) (*WorkerPool, error) {
	var w = WorkerPool{
		interval: defaultPollInterval,
		client:   c,
		id:       RandomStringID(),
		wm:       WorkMap{},

		panicStackBufSize: defaultPanicStackBufSize,
	}

	for _, option := range options {
		option(&w)
	}

	if len(w.wm) == 0 {
		return nil, fmt.Errorf("empty worker functions list")
	}

	return &w, nil
}

// Run runs all the Workers in the WorkerPool in own goroutines.
// Run blocks until all workers exit. Use context cancellation for
// shutdown.
func (w *WorkerPool) Run(ctx context.Context) (err error) {
	ctx, w.ctxCancel = context.WithCancel(ctx)
	w.once.Do(func() {
		var grp, ctx = errgroup.WithContext(ctx)
		w.waitStop.Add(1)
		grp.Go(func() error {
			defer w.waitStop.Done()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(w.interval):
					if err = w.WorkOne(ctx); err != nil {
						w.logger.Error("error run worker", zap.Error(err))
					}
				}
			}
		})

		if w.queueRestoreAfter > 0 {
			w.waitStop.Add(1)
			grp.Go(func() error {
				defer w.waitStop.Done()
				for {
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(w.queueRestoreInterval):
					}

					var childCtx, cancelFunc = context.WithTimeout(ctx, time.Second*30)
					if err := w.client.RestoreStuck(childCtx, w.queueRestoreAfter, w.queue...); err != nil {
						w.logger.Error("error restore stuck jobs", zap.Error(err))
					}

					cancelFunc()
				}
			})

		}
		err = grp.Wait()
		w.once = sync.Once{}
	})
	return err
}

func (w *WorkerPool) Stop() {
	w.ctxCancel()
	w.waitStop.Wait()
}

// WorkOne tries to consume single message from the queue.
func (w *WorkerPool) WorkOne(ctx context.Context) (err error) {
	var jobs []*Job
	w.lock.RLock()
	if jobs, err = w.client.LockNextScheduledJob(ctx, w.queue); err != nil {
		w.lock.RUnlock()
		return fmt.Errorf("error get scheduled job: %w", err)
	}
	w.lock.RUnlock()
	if jobs == nil {
		return nil
	}

	for _, j := range jobs {
		w.limitWorker(j.Queue, -1)

		go func(j *Job) {
			defer w.recoverPanic(ctx, j)
			defer w.limitWorker(j.Queue, 1)

			var (
				err error
				h   WorkFunc
				ok  bool
				l   = w.logger.With(zap.String("type", j.JobType), zap.Int64("id", j.ID))
			)
			if h, ok = w.wm[j.JobType]; !ok {
				var errUnknownType = fmt.Errorf("unknown job type: %s", j.JobType)
				if err = j.Error(ctx, errUnknownType); err != nil {
					l.Error("error set error status for job", zap.Error(err))
					return
				}

				for _, hook := range w.hooksUnknownJobType {
					hook(ctx, j, errUnknownType)
				}

				return
			}

			if err = h(ctx, j); err != nil {
				for _, hook := range w.hooksJobDone {
					hook(ctx, j, err)
				}

				if jErr := j.Error(ctx, err); jErr != nil {
					l.Error("error set error status for job", zap.Error(err))
				}

				return
			}

			for _, hook := range w.hooksJobDone {
				hook(ctx, j, nil)
			}

			if err = j.Done(ctx); err != nil {
				l.Error("error set done status for job", zap.Error(err))
			}

			return
		}(j)

	}

	return
}

func (w *WorkerPool) limitWorker(queue string, counter int32) {
	w.lock.Lock()
	defer w.lock.Unlock()
	for i := range w.queue {
		if w.queue[i].Queue != queue {
			continue
		}
		w.queue[i].Limit += counter
	}
}

// recoverPanic tries to handle panics in job execution.
// A stacktrace is stored into Job last_error.
func (w *WorkerPool) recoverPanic(ctx context.Context, j *Job) {
	if r := recover(); r != nil {

		// record an error on the job with panic message and stacktrace
		var (
			stackBuf            = make([]byte, w.panicStackBufSize)
			n                   = runtime.Stack(stackBuf, false)
			buf                 = new(bytes.Buffer)
			_, printRErr        = fmt.Fprintf(buf, "%v\n", r)
			_, printStackErr    = fmt.Fprintln(buf, string(stackBuf[:n]))
			_, printEllipsisErr = fmt.Fprintln(buf, "[...]")
			stacktrace          = buf.String()
		)
		if err := multierr.Combine(printRErr, printStackErr, printEllipsisErr); err != nil {
			w.logger.Error("could not build panicked job stacktrace", zap.Error(err))
		}

		if err := j.Error(ctx, errors.New(stacktrace)); err != nil {
			w.logger.Error("got an error on setting an error to a panicked job", zap.Error(err))
		}
	}
}
