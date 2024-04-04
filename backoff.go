package gue

import (
	"time"

	exp "github.com/vgarvardt/backoff"
)

// Backoff is the interface for backoff implementation that will be used to reschedule errored jobs to a later time.
// If the Backoff implementation returns negative duration - the job will be discarded.
type Backoff func(retries int) time.Duration

var (
	// DefaultExponentialBackoff is the exponential Backoff implementation with default config applied
	DefaultExponentialBackoff = NewExponentialBackoff(exp.Config{
		BaseDelay:  1.0 * time.Second,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   1.0 * time.Hour,
	})

	// BackoffNever is the Backoff implementation that never returns errored job to the queue for retry,
	// but discards it in case of the error.
	BackoffNever = func(retries int) time.Duration {
		return -1
	}
)

// NewExponentialBackoff instantiates new exponential Backoff implementation with config
func NewExponentialBackoff(cfg exp.Config) Backoff {
	return exp.Exponential{Config: cfg}.Backoff
}

// NewConstantBackoff instantiates new backoff implementation with the constant retry duration that does not depend
// on the retry.
func NewConstantBackoff(d time.Duration) Backoff {
	return func(int) time.Duration {
		return d
	}
}
