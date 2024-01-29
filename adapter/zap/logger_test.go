package zap

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	uberZap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/sadpenguinn/gue/v6/adapter"
)

func TestNew(t *testing.T) {
	zapCore, logs := observer.New(zapcore.DebugLevel)
	l := uberZap.New(zapCore)
	ll := New(l)

	err := errors.New("something went wrong")

	ll.Debug("debug-1", adapter.F("debug-key", "debug-val"))
	ll.Info("info-1", adapter.F("info-key", "info-val"))
	ll.Error("error-1", adapter.F("error-key", "error-val"))
	ll.Error("error-2", adapter.Err(err))

	lll := ll.With(adapter.F("nested-key", "nested-val"))
	lll.Info("info-2", adapter.F("info-key-2", "info-val-2"))

	require.Equal(t, 5, logs.Len())

	var i any

	logEntries := logs.AllUntimed()
	assert.Equal(t, []observer.LoggedEntry{
		{
			Entry: zapcore.Entry{
				Level:   zapcore.DebugLevel,
				Message: "debug-1",
			},
			Context: []zapcore.Field{
				{
					Key:       "debug-key",
					String:    "debug-val",
					Type:      zapcore.StringType,
					Interface: i,
				},
			},
		}, {
			Entry: zapcore.Entry{
				Level:   zapcore.InfoLevel,
				Message: "info-1",
			},
			Context: []zapcore.Field{
				{
					Key:       "info-key",
					String:    "info-val",
					Type:      zapcore.StringType,
					Interface: i,
				},
			},
		}, {
			Entry: zapcore.Entry{
				Level:   zapcore.ErrorLevel,
				Message: "error-1",
			},
			Context: []zapcore.Field{
				{
					Key:       "error-key",
					String:    "error-val",
					Type:      zapcore.StringType,
					Interface: i,
				},
			},
		}, {
			Entry: zapcore.Entry{
				Level:   zapcore.ErrorLevel,
				Message: "error-2",
			},
			Context: []zapcore.Field{
				{
					Key:       adapter.KeyError,
					Type:      zapcore.ErrorType,
					Interface: err,
				},
			},
		}, {
			Entry: zapcore.Entry{
				Level:   zapcore.InfoLevel,
				Message: "info-2",
			},
			Context: []zapcore.Field{
				{
					Key:       "nested-key",
					String:    "nested-val",
					Type:      zapcore.StringType,
					Interface: i,
				}, {
					Key:       "info-key-2",
					String:    "info-val-2",
					Type:      zapcore.StringType,
					Interface: i,
				},
			},
		},
	}, logEntries)
}
