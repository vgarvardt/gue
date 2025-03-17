package zap

import (
	uberZap "go.uber.org/zap"

	"github.com/sadpenguinn/gue/v6/adapter"
)

var _ adapter.Logger = &zap{}

type zap struct {
	l *uberZap.Logger
}

// New instantiates new adapter.Logger using go.uber.org/zap
func New(l *uberZap.Logger) adapter.Logger {
	return &zap{l}
}

// Debug implements Logger.Debug for go.uber.org/zap logger
func (l *zap) Debug(msg string, fields ...adapter.Field) {
	l.l.Debug(msg, l.zapFields(fields...)...)
}

// Info implements Logger.Debug for go.uber.org/zap logger
func (l *zap) Info(msg string, fields ...adapter.Field) {
	l.l.Info(msg, l.zapFields(fields...)...)
}

// Error implements Logger.Debug for go.uber.org/zap logger
func (l *zap) Error(msg string, fields ...adapter.Field) {
	l.l.Error(msg, l.zapFields(fields...)...)
}

// With implements nested logger for go.uber.org/zap logger
func (l *zap) With(fields ...adapter.Field) adapter.Logger {
	return New(l.l.With(l.zapFields(fields...)...))
}

func (l *zap) zapFields(fields ...adapter.Field) []uberZap.Field {
	result := make([]uberZap.Field, len(fields))
	for i, f := range fields {
		result[i] = uberZap.Any(f.Key, f.Value)
	}
	return result
}
