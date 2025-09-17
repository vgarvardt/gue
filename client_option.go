package gue

import (
	"log/slog"

	"go.opentelemetry.io/otel/metric"
)

// ClientOption defines a type that allows to set client properties during the build-time.
type ClientOption func(*Client)

// WithClientLogger sets Logger implementation to client.
func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

// WithClientID sets client ID for easier identification in logs.
func WithClientID(id string) ClientOption {
	return func(c *Client) {
		c.id = id
	}
}

// WithClientBackoff sets backoff implementation that will be applied to errored jobs
// within current client session.
func WithClientBackoff(backoff Backoff) ClientOption {
	return func(c *Client) {
		c.backoff = backoff
	}
}

// WithClientMeter sets metric.Meter instance to the client.
func WithClientMeter(meter metric.Meter) ClientOption {
	return func(c *Client) {
		c.meter = meter
	}
}
