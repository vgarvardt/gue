package exponential

import (
	"time"

	"github.com/vgarvardt/backoff"
)

// Default is the exponential backoff implementation with default config applied
var Default = New(backoff.Config{
	BaseDelay:  1.0 * time.Second,
	Multiplier: 1.6,
	Jitter:     0.2,
	MaxDelay:   1.0 * time.Hour,
})

// New instantiates new exponential backoff implementation with config
func New(cfg backoff.Config) func(retries int) time.Duration {
	return backoff.Exponential{Config: cfg}.Backoff
}
