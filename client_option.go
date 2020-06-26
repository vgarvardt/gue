package gue

import "github.com/vgarvardt/gue/v2/adapter"

// ClientOption defines a type that allows to set client properties during the build-time.
type ClientOption func(*Client)

// WithClientLogger sets Logger implementation to client
func WithClientLogger(logger adapter.Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

// WithClientID sets client ID for easier identification in logs
func WithClientID(id string) ClientOption {
	return func(c *Client) {
		c.id = id
	}
}
