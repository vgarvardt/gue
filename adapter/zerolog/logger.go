package zerolog

import (
	rsZerolog "github.com/rs/zerolog"

	"github.com/sadpenguinn/gue/v6/adapter"
)

var _ adapter.Logger = &zerolog{}

type zerolog struct {
	l rsZerolog.Logger
}

// New instantiates new adapter.Logger using github.com/rs/zerolog
func New(l rsZerolog.Logger) adapter.Logger {
	return &zerolog{l}
}

// Debug implements Logger.Debug for github.com/rs/zerolog logger
func (l *zerolog) Debug(msg string, fields ...adapter.Field) {
	l.l.Debug().Fields(l.zerologFields(fields...)).Msg(msg)
}

// Info implements Logger.Debug for github.com/rs/zerolog logger
func (l *zerolog) Info(msg string, fields ...adapter.Field) {
	l.l.Info().Fields(l.zerologFields(fields...)).Msg(msg)
}

// Error implements Logger.Debug for github.com/rs/zerolog logger
func (l *zerolog) Error(msg string, fields ...adapter.Field) {
	l.l.Error().Fields(l.zerologFields(fields...)).Msg(msg)
}

// With implements nested logger for github.com/rs/zerolog logger
func (l *zerolog) With(fields ...adapter.Field) adapter.Logger {
	return New(l.l.With().Fields(l.zerologFields(fields...)).Logger())
}

func (l *zerolog) zerologFields(fields ...adapter.Field) map[string]any {
	fieldsMap := make(map[string]any, len(fields))
	for _, f := range fields {
		fieldsMap[f.Key] = f.Value
	}
	return fieldsMap
}
