package gue

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/vgarvardt/gue/v4/adapter"
)

func TestWithClientID(t *testing.T) {
	clientWithDefaultID := NewClient(nil)
	assert.NotEmpty(t, clientWithDefaultID.id)

	customID := "some-meaningful-id"
	clientWithCustomID := NewClient(nil, WithClientID(customID))
	assert.Equal(t, customID, clientWithCustomID.id)
}

func TestWithClientLogger(t *testing.T) {
	clientWithDefaultLogger := NewClient(nil)
	assert.IsType(t, adapter.NoOpLogger{}, clientWithDefaultLogger.logger)

	logMessage := "hello"

	l := new(mockLogger)
	l.On("Info", logMessage, mock.Anything)
	// worker sets id as default logger field
	l.On("With", mock.Anything).Return(l)

	clientWithCustomLogger := NewClient(nil, WithClientLogger(l))
	clientWithCustomLogger.logger.Info(logMessage)

	l.AssertExpectations(t)
}

func TestWithClientBackoff(t *testing.T) {
	customBackoff := func(retries int) time.Duration {
		return time.Duration(retries) * time.Second
	}

	defaultPtr := reflect.ValueOf(DefaultExponentialBackoff).Pointer()
	customPtr := reflect.ValueOf(customBackoff).Pointer()

	clientWithDefaultBackoff := NewClient(nil)
	clientWithDefaultBackoffPtr := reflect.ValueOf(clientWithDefaultBackoff.backoff).Pointer()

	assert.Equal(t, defaultPtr, clientWithDefaultBackoffPtr)

	clientWithCustomBackoff := NewClient(nil, WithClientBackoff(customBackoff))
	assert.Equal(t, customBackoff(123), clientWithCustomBackoff.backoff(123))
	assert.NotEqual(t, defaultPtr, customPtr)
}
