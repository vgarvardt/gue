package gue

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/vgarvardt/gue/v5/adapter"
)

// WorkerOption defines a type that allows to set worker properties during the build-time.
type WorkerOption func(*Worker)

// WorkerPoolOption defines a type that allows to set worker pool properties during the build-time.
type WorkerPoolOption func(pool *WorkerPool)

// WithWorkerPollInterval overrides default poll interval with the given value.
// Poll interval is the "sleep" duration if there were no jobs found in the DB.
func WithWorkerPollInterval(d time.Duration) WorkerOption {
	return func(w *Worker) {
		w.interval = d
	}
}

// WithWorkerQueue overrides default worker queue name with the given value.
func WithWorkerQueue(queue string) WorkerOption {
	return func(w *Worker) {
		w.queue = queue
	}
}

// WithWorkerID sets worker ID for easier identification in logs
func WithWorkerID(id string) WorkerOption {
	return func(w *Worker) {
		w.id = id
	}
}

// WithWorkerLogger sets Logger implementation to worker
func WithWorkerLogger(logger adapter.Logger) WorkerOption {
	return func(w *Worker) {
		w.logger = logger
	}
}

// WithWorkerTracer sets trace.Tracer instance to the worker.
func WithWorkerTracer(tracer trace.Tracer) WorkerOption {
	return func(w *Worker) {
		w.tracer = tracer
	}
}

// WithWorkerMeter sets metric.Meter instance to the worker.
func WithWorkerMeter(meter metric.Meter) WorkerOption {
	return func(w *Worker) {
		w.meter = meter
	}
}

// WithWorkerPanicStackBufSize sets max size for the stacktrace buffer for panicking jobs.
// Default value is 1024 that is enough for most of the cases. Be careful setting buffer suze to the big values
// as this may affect overall performance.
func WithWorkerPanicStackBufSize(size int) WorkerOption {
	return func(w *Worker) {
		w.panicStackBufSize = size
	}
}

// WithWorkerHooksJobLocked sets hooks that are called right after the job was polled from the DB.
// Depending on the polling results hook will have either error or job set, but not both.
// If the error field is set - no other lifecycle hooks will be called for the job.
func WithWorkerHooksJobLocked(hooks ...HookFunc) WorkerOption {
	return func(w *Worker) {
		w.hooksJobLocked = hooks
	}
}

// WithWorkerHooksUnknownJobType sets hooks that are called when worker finds a job with unknown type.
// Error field for this event type is always set since this is an error situation.
// If this hook is called - no other lifecycle hooks will be called for the job.
func WithWorkerHooksUnknownJobType(hooks ...HookFunc) WorkerOption {
	return func(w *Worker) {
		w.hooksUnknownJobType = hooks
	}
}

// WithWorkerHooksJobDone sets hooks that are called when worker finished working the job,
// right before the successfully executed job will be removed or errored job handler will be called to decide
// if the Job will be re-queued or discarded.
// Error field is set for the cases when the job was worked with an error.
func WithWorkerHooksJobDone(hooks ...HookFunc) WorkerOption {
	return func(w *Worker) {
		w.hooksJobDone = hooks
	}
}

// WithWorkerPollStrategy overrides default poll strategy with given value
func WithWorkerPollStrategy(s PollStrategy) WorkerOption {
	return func(w *Worker) {
		w.pollStrategy = s
	}
}

// WithWorkerGracefulShutdown enables graceful shutdown mode in the worker.
// When graceful shutdown is enabled - worker does not propagate cancel context to Job,
// as a result worker is waiting for the Job being currently executed and only then shuts down.
// Use this mode carefully, as Job handler is not aware anymore of the worker context state and
// dependencies may already be cancelled/closed, so it is up to the job to ensure everything is
// still working. Values of the original context are not propagated to the handler context as well
// when the graceful mode is enabled.
//
// Use "handlerCtx" to set up custom handler context. When set to nil - defaults to context.Background().
func WithWorkerGracefulShutdown(handlerCtx func() context.Context) WorkerOption {
	return func(w *Worker) {
		w.graceful = true
		w.gracefulCtx = handlerCtx
	}
}

// WithWorkerSpanWorkOneNoJob enables tracing span generation for every try to get one.
// When set to true - generates a span for every DB poll, even when no job was acquired. This may
// generate a lot of empty spans, but may help with some debugging, so use carefully.
func WithWorkerSpanWorkOneNoJob(spanWorkOneNoJob bool) WorkerOption {
	return func(w *Worker) {
		w.spanWorkOneNoJob = spanWorkOneNoJob
	}
}

// WithPoolPollInterval overrides default poll interval with the given value.
// Poll interval is the "sleep" duration if there were no jobs found in the DB.
func WithPoolPollInterval(d time.Duration) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.interval = d
	}
}

// WithPoolQueue overrides default worker queue name with the given value.
func WithPoolQueue(queue string) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.queue = queue
	}
}

// WithPoolID sets worker pool ID for easier identification in logs
func WithPoolID(id string) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.id = id
	}
}

// WithPoolLogger sets Logger implementation to worker pool
func WithPoolLogger(logger adapter.Logger) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.logger = logger
	}
}

// WithPoolPollStrategy overrides default poll strategy with given value
func WithPoolPollStrategy(s PollStrategy) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.pollStrategy = s
	}
}

// WithPoolTracer sets trace.Tracer instance to every worker in the pool.
func WithPoolTracer(tracer trace.Tracer) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.tracer = tracer
	}
}

// WithPoolMeter sets metric.Meter instance to every worker in the pool.
func WithPoolMeter(meter metric.Meter) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.meter = meter
	}
}

// WithPoolHooksJobLocked calls WithWorkerHooksJobLocked for every worker in the pool.
func WithPoolHooksJobLocked(hooks ...HookFunc) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.hooksJobLocked = hooks
	}
}

// WithPoolHooksUnknownJobType calls WithWorkerHooksUnknownJobType for every worker in the pool.
func WithPoolHooksUnknownJobType(hooks ...HookFunc) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.hooksUnknownJobType = hooks
	}
}

// WithPoolHooksJobDone calls WithWorkerHooksJobDone for every worker in the pool.
func WithPoolHooksJobDone(hooks ...HookFunc) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.hooksJobDone = hooks
	}
}

// WithPoolGracefulShutdown enables graceful shutdown mode for all workers in the pool.
// See WithWorkerGracefulShutdown for details.
func WithPoolGracefulShutdown(handlerCtx func() context.Context) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.graceful = true
		w.gracefulCtx = handlerCtx
	}
}

// WithPoolPanicStackBufSize sets max size for the stacktrace buffer for panicking jobs.
// Default value is 1024 that is enough for most of the cases. Be careful setting buffer suze to the big values
// as this may affect overall performance.
func WithPoolPanicStackBufSize(size int) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.panicStackBufSize = size
	}
}

// WithPoolSpanWorkOneNoJob enables tracing span generation for every try to get one.
// When set to true - generates a span for every DB poll, even when no job was acquired. This may
// generate a lot of empty spans, but may help with some debugging, so use carefully.
func WithPoolSpanWorkOneNoJob(spanWorkOneNoJob bool) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.spanWorkOneNoJob = spanWorkOneNoJob
	}
}
