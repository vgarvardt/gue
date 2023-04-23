package gue

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// WorkerPoolOption defines a type that allows to set worker pool properties during the build-time.
type WorkerPoolOption func(pool *WorkerPool)

// WithWorkerPoolQueue overrides default worker queue name with the given value.
func WithWorkerPoolQueue(queue ...QueueLimit) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.queue = append(w.queue, queue...)
	}
}

// WithWorkerPanicStackBufSize sets max size for the stacktrace buffer for panicking jobs.
// Default value is 1024 that is enough for most of the cases. Be careful setting buffer suze to the big values
// as this may affect overall performance.
func WithWorkerPanicStackBufSize(size int) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.panicStackBufSize = size
	}
}

func WithWorkerPanicWorkerHandler(jobType string, h WorkFunc) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.wm[jobType] = h
	}
}

func WithLogger(l *zap.Logger) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.logger = l
	}
}

func WithWorkerPanicWorkerMap(workMap WorkMap) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.wm = workMap
	}
}

// WithPoolInterval overrides default poll interval with the given value.
// Poll interval is the "sleep" duration if there were no jobs found in the DB.
func WithPoolInterval(d time.Duration) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.interval = d
	}
}

// WithPoolID sets worker pool ID for easier identification in logs
func WithPoolID(id string) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.id = id
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

func WithPoolQueueRestore(restoreAfter, interval time.Duration) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.queueRestoreAfter = restoreAfter
		w.queueRestoreInterval = interval
	}
}
