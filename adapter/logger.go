package adapter

import (
	"fmt"
	"log"
	"strings"
	"sync"
)

// KeyError is the default key for error field
const KeyError = "error"

type (
	// Field is the simple container for a single log field
	Field struct {
		Key   string
		Value any
	}

	// Logger declares base logging methods
	Logger interface {
		Debug(msg string, fields ...Field)
		Info(msg string, fields ...Field)
		Error(msg string, fields ...Field)

		With(fields ...Field) Logger
	}

	// NoOpLogger implements Logger that does nothing, all logs are going to /dev/null
	NoOpLogger struct{}

	// StdLogger implements Logger that uses stdlib "log" as output
	StdLogger struct {
		fields *sync.Map
		fLen   int
	}
)

// F returns value as field
func F(key string, value any) Field {
	return Field{Key: key, Value: value}
}

// Err returns error as field
func Err(err error) Field {
	return F(KeyError, err)
}

// Debug implements Logger.Debug for /dev/null logger
func (l NoOpLogger) Debug(string, ...Field) {}

// Info implements Logger.Debug for /dev/null logger
func (l NoOpLogger) Info(string, ...Field) {}

// Error implements Logger.Debug for /dev/null logger
func (l NoOpLogger) Error(string, ...Field) {}

// With implements nested logger for /dev/null logger
func (l NoOpLogger) With(...Field) Logger {
	return l
}

// NewStdLogger instantiates new Logger using stdlib "log".
// Builder allows to set default set of fields for all the logs being written.
func NewStdLogger(fields ...Field) *StdLogger {
	f := new(sync.Map)
	for _, ff := range fields {
		f.Store(ff.Key, ff.Value)
	}

	return &StdLogger{f, len(fields)}
}

// Debug implements Logger.Debug for stdlib "log" logger
func (l *StdLogger) Debug(msg string, fields ...Field) {
	log.Printf("%s %s", msg, l.buildContext("debug", fields...))
}

// Info implements Logger.Debug for stdlib "log" logger
func (l *StdLogger) Info(msg string, fields ...Field) {
	log.Printf("%s %s", msg, l.buildContext("info", fields...))
}

// Error implements Logger.Debug for stdlib "log" logger
func (l *StdLogger) Error(msg string, fields ...Field) {
	log.Printf("%s %s", msg, l.buildContext("error", fields...))
}

// With implements nested logger for stdlib "log" logger
func (l *StdLogger) With(fields ...Field) Logger {
	f := new(sync.Map)
	fLen := len(fields)
	l.fields.Range(func(key, value any) bool {
		f.Store(key, value)
		fLen++
		return true
	})
	for _, ff := range fields {
		f.Store(ff.Key, ff.Value)
	}

	return &StdLogger{f, fLen}
}

func (l *StdLogger) buildContext(level string, fields ...Field) string {
	ctx := make([]string, 0, len(fields)+l.fLen+1)
	ctx = append(ctx, "level="+level)
	l.fields.Range(func(key, value any) bool {
		ctx = append(ctx, fmt.Sprintf("%s=%v", key, value))
		return true
	})
	for _, f := range fields {
		ctx = append(ctx, fmt.Sprintf("%s=%v", f.Key, f.Value))
	}

	return strings.Join(ctx, " ")
}
