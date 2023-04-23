package gue

// ClientOption defines a type that allows to set client properties during the build-time.
type ClientOption func(*Client)

// WithClientBackoff sets backoff implementation that will be applied to errored jobs
// within current client session.
func WithClientBackoff(backoff Backoff) ClientOption {
	return func(c *Client) {
		c.backoff = backoff
	}
}
