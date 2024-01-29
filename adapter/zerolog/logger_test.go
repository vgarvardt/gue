package zerolog

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	rsZerolog "github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sadpenguinn/gue/v6/adapter"
)

func TestNew(t *testing.T) {
	buf := new(bytes.Buffer)
	zLog := rsZerolog.New(buf)
	ll := New(zLog)

	err := errors.New("something went wrong")

	ll.Debug("debug-1", adapter.F("debug-key", "debug-val"))
	ll.Info("info-1", adapter.F("info-key", "info-val"))
	ll.Error("error-1", adapter.F("error-key", "error-val"))
	ll.Error("error-2", adapter.Err(err))

	lll := ll.With(adapter.F("nested-key", "nested-val"))
	lll.Info("info-2", adapter.F("info-key-2", "info-val-2"))

	logLines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, logLines, 5)

	assert.Contains(t, logLines[0], `"level":"debug"`)
	assert.Contains(t, logLines[0], `"message":"debug-1"`)
	assert.Contains(t, logLines[0], `"debug-key":"debug-val"`)

	assert.Contains(t, logLines[1], `"level":"info"`)
	assert.Contains(t, logLines[1], `"message":"info-1"`)
	assert.Contains(t, logLines[1], `"info-key":"info-val"`)

	assert.Contains(t, logLines[2], `"level":"error"`)
	assert.Contains(t, logLines[2], `"message":"error-1"`)
	assert.Contains(t, logLines[2], `"error-key":"error-val"`)

	assert.Contains(t, logLines[3], `"level":"error"`)
	assert.Contains(t, logLines[3], `"message":"error-2"`)
	assert.Contains(t, logLines[3], `"error":"something went wrong"`)

	assert.Contains(t, logLines[4], `"level":"info"`)
	assert.Contains(t, logLines[4], `"message":"info-2"`)
	assert.Contains(t, logLines[4], `"nested-key":"nested-val"`)
	assert.Contains(t, logLines[4], `"info-key-2":"info-val-2"`)
}
