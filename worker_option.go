package gue

import (
	"time"

	"github.com/vgarvardt/gue/v3/adapter"
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

// WithWorkerNextScheduledPollStrategy sets pollStrategy as nextScheduledPollStrategy
func WithWorkerNextScheduledPollStrategy() WorkerOption {
	return func(w *Worker) {
		w.pollStrategy = nextScheduledPollStrategy
	}
}

// setWorkerPollStrategy setter method for worker pollStrategy
func setWorkerPollStrategy(strategy string) WorkerOption {
	switch strategy {
	case nextScheduledPollStrategy:
		return WithWorkerNextScheduledPollStrategy()
	default:
		return func(w *Worker) {
			w.pollStrategy = defaultPollStrategy
		}
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

// WithPoolNextScheduledPollStrategy sets pollStrategy as nextScheduledPollStrategy
func WithPoolNextScheduledPollStrategy() WorkerPoolOption {
	return func(w *WorkerPool) {
		w.pollStrategy = nextScheduledPollStrategy
	}
}
