package gue

import "context"

type ctxKey struct{}

var (
	workerIdxKey = ctxKey{}
)

const (
	// WorkerIdxUnknown is returned when worker index in the pool is not set for some reasons.
	WorkerIdxUnknown = -1
)

// SetWorkerIdx sets the index of the worker in the pool to the worker context.
func SetWorkerIdx(ctx context.Context, idx int) context.Context {
	return context.WithValue(ctx, workerIdxKey, idx)
}

// GetWorkerIdx gets the index of the worker in the pool from the worker context.
// Returns WorkerIdxUnknown if the context is not set or the value is not found there.
func GetWorkerIdx(ctx context.Context) int {
	if ctx == nil {
		return WorkerIdxUnknown
	}

	if idx, ok := ctx.Value(workerIdxKey).(int); ok {
		return idx
	}

	return WorkerIdxUnknown
}
