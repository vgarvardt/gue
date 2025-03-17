//go:build go1.21

package slog

import (
	"bytes"
	"errors"
	libSLog "log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sadpenguinn/gue/v6/adapter"
)

func TestNew(t *testing.T) {
	var buf bytes.Buffer
	l := libSLog.New(libSLog.NewJSONHandler(&buf, &libSLog.HandlerOptions{
		AddSource: true,
		Level:     libSLog.LevelDebug,
	}))
	ll := New(l)

	err := errors.New("something went wrong")

	ll.Debug("debug-1", adapter.F("debug-key", "debug-val"))
	ll.Info("info-1", adapter.F("info-key", "info-val"))
	ll.Error("error-1", adapter.F("error-key", "error-val"))
	ll.Error("error-2", adapter.Err(err))

	lll := ll.With(adapter.F("nested-key", "nested-val"))
	lll.Info("info-2", adapter.F("info-key-2", "info-val-2"))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 5)

	for line, contains := range [][]string{
		{`"level":"DEBUG"`, `"msg":"debug-1"`, `"debug-key":"debug-val"`},
		{`"level":"INFO"`, `"msg":"info-1"`, `"info-key":"info-val"`},
		{`"level":"ERROR"`, `"msg":"error-1"`, `"error-key":"error-val"`},
		{`"level":"ERROR"`, `"msg":"error-2"`, `"error":"something went wrong"`},
		{`"level":"INFO"`, `"msg":"info-2"`, `"info-key-2":"info-val-2"`, `"nested-key":"nested-val"`},
	} {
		for _, sub := range contains {
			assert.Contains(t, lines[line], sub)
		}
	}
}
