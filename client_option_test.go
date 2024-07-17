package gue

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/vgarvardt/gue/v4/adapter"
)

func TestWithClientID(t *testing.T) {
	clientWithDefaultID, err := NewClient(nil)
	require.NoError(t, err)
	assert.NotEmpty(t, clientWithDefaultID.id)

	customID := "some-meaningful-id"
	clientWithCustomID, err := NewClient(nil, WithClientID(customID))
	require.NoError(t, err)
	assert.Equal(t, customID, clientWithCustomID.id)
}

func TestWithClientLogger(t *testing.T) {
	clientWithDefaultLogger, err := NewClient(nil)
	require.NoError(t, err)
	assert.IsType(t, adapter.NoOpLogger{}, clientWithDefaultLogger.logger)

	logMessage := "hello"

	l := new(mockLogger)
	l.On("Info", logMessage, mock.Anything)
	// worker sets id as default logger field
	l.On("With", mock.Anything).Return(l)

	clientWithCustomLogger, err := NewClient(nil, WithClientLogger(l))
	require.NoError(t, err)
	clientWithCustomLogger.logger.Info(logMessage)

	l.AssertExpectations(t)
}

func TestWithClientBackoff(t *testing.T) {
	customBackoff := func(retries int) time.Duration {
		return time.Duration(retries) * time.Second
	}

	defaultPtr := reflect.ValueOf(DefaultExponentialBackoff).Pointer()
	customPtr := reflect.ValueOf(customBackoff).Pointer()

	clientWithDefaultBackoff, err := NewClient(nil)
	require.NoError(t, err)

	clientWithDefaultBackoffPtr := reflect.ValueOf(clientWithDefaultBackoff.backoff).Pointer()

	assert.Equal(t, defaultPtr, clientWithDefaultBackoffPtr)

	clientWithCustomBackoff, err := NewClient(nil, WithClientBackoff(customBackoff))
	require.NoError(t, err)

	assert.Equal(t, customBackoff(123), clientWithCustomBackoff.backoff(123))
	assert.NotEqual(t, defaultPtr, customPtr)
}

func TestWithClientMeter(t *testing.T) {
	customMeter := noop.NewMeterProvider().Meter("custom")

	_, err := NewClient(nil)
	require.NoError(t, err)

	clientWithCustomMeter, err := NewClient(nil, WithClientMeter(customMeter))
	require.NoError(t, err)

	assert.Equal(t, customMeter, clientWithCustomMeter.meter)
}
