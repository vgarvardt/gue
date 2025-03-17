//go:build go1.21

package slog

import (
	libSLog "log/slog"

	"github.com/sadpenguinn/gue/v6/adapter"
)

var _ adapter.Logger = &slog{}

type slog struct {
	l *libSLog.Logger
}

// New instantiates new adapter.Logger using go.uber.org/slog
func New(l *libSLog.Logger) adapter.Logger {
	return &slog{l}
}

// Debug implements Logger.Debug for go.uber.org/slog logger
func (l *slog) Debug(msg string, fields ...adapter.Field) {
	l.l.Debug(msg, l.slogFields(fields...)...)
}

// Info implements Logger.Debug for go.uber.org/slog logger
func (l *slog) Info(msg string, fields ...adapter.Field) {
	l.l.Info(msg, l.slogFields(fields...)...)
}

// Error implements Logger.Debug for go.uber.org/slog logger
func (l *slog) Error(msg string, fields ...adapter.Field) {
	l.l.Error(msg, l.slogFields(fields...)...)
}

// With implements nested logger for go.uber.org/slog logger
func (l *slog) With(fields ...adapter.Field) adapter.Logger {
	return New(l.l.With(l.slogFields(fields...)...))
}

func (l *slog) slogFields(fields ...adapter.Field) []any {
	result := make([]any, 0, len(fields)*2)
	for _, f := range fields {
		result = append(result, f.Key, f.Value)
	}
	return result
}
