package gue

import (
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/cappuccinotm/slogx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
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
	assert.IsType(t, slogx.NopHandler(), clientWithDefaultLogger.logger.Handler())

	const logMessage = "hello"

	observe, logs := observer.New(zap.InfoLevel)
	logger := zapcore.NewTee(zaptest.NewLogger(t).Core(), observe)

	clientWithCustomLogger, err := NewClient(nil, WithClientLogger(slog.New(zapslog.NewHandler(logger))))
	require.NoError(t, err)
	clientWithCustomLogger.logger.Info(logMessage)

	require.Len(t, logs.All(), 1)
	assert.Equal(t, logMessage, logs.All()[0].Message)
	assert.Equal(t, zapcore.InfoLevel, logs.All()[0].Level)
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
