package gue

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func TestWorkerWorkOne(t *testing.T) {
	c := openTestClientPGXv3(t)

	success := false
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			success = true
			return nil
		},
	}
	w := NewWorker(c, wm)

	didWork := w.WorkOne()
	assert.False(t, didWork)

	err := c.Enqueue(&Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork = w.WorkOne()
	assert.True(t, didWork)
	assert.True(t, success)
}

func TestWorker_Start(t *testing.T) {
	c := openTestClientPGXv3(t)

	w := NewWorker(c, WorkMap{})

	ctx, cancel := context.WithCancel(context.Background())
	err := w.Start(ctx)
	require.NoError(t, err)

	assert.True(t, w.running)

	// try to start one more time to get an error about already running worker
	err = w.Start(context.Background())
	require.Error(t, err)

	cancel()

	// give worker time to get a signal and stop
	time.Sleep(time.Second)
	assert.False(t, w.running)
}

func TestWorkerPool_Start(t *testing.T) {
	c := openTestClientPGXv3(t)

	poolSize := 2
	w := NewWorkerPool(c, WorkMap{}, poolSize)

	ctx, cancel := context.WithCancel(context.Background())
	err := w.Start(ctx)
	require.NoError(t, err)

	assert.True(t, w.running)
	for i := range w.workers {
		assert.True(t, w.workers[i].running)
	}

	// try to start one more time to get an error about already running worker pool
	err = w.Start(context.Background())
	require.Error(t, err)

	cancel()

	// give worker time to get a signal and stop
	time.Sleep(time.Second)
	assert.False(t, w.running)
	for i := range w.workers {
		assert.False(t, w.workers[i].running)
	}
}

func BenchmarkWorker(b *testing.B) {
	c := openTestClientPGXv3(b)
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stdout)
	}()

	w := NewWorker(c, WorkMap{"Nil": nilWorker})

	for i := 0; i < b.N; i++ {
		if err := c.Enqueue(&Job{Type: "Nil"}); err != nil {
			log.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WorkOne()
	}
}

func nilWorker(j *Job) error {
	return nil
}

func TestWorkerWorkReturnsError(t *testing.T) {
	c := openTestClientPGXv3(t)

	called := 0
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			called++
			return errors.New("the error msg")
		},
	}
	w := NewWorker(c, wm)

	didWork := w.WorkOne()
	assert.False(t, didWork)

	err := c.Enqueue(&Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork = w.WorkOne()
	assert.True(t, didWork)
	assert.Equal(t, 1, called)

	tx, err := c.pool.Begin()
	require.NoError(t, err)
	defer func() {
		err := tx.Rollback()
		assert.NoError(t, err)
	}()

	j := findOneJob(t, tx)
	require.NotNil(t, j)

	assert.Equal(t, int32(1), j.ErrorCount)
	assert.NotEqual(t, pgtype.Null, j.LastError.Status)
	assert.Equal(t, "the error msg", j.LastError.String)
}

func TestWorkerWorkRescuesPanic(t *testing.T) {
	c := openTestClientPGXv3(t)

	called := 0
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			called++
			panic("the panic msg")
		},
	}
	w := NewWorker(c, wm)

	err := c.Enqueue(&Job{Type: "MyJob"})
	require.NoError(t, err)

	w.WorkOne()
	assert.Equal(t, 1, called)

	tx, err := c.pool.Begin()
	require.NoError(t, err)
	defer func() {
		err := tx.Rollback()
		assert.NoError(t, err)
	}()

	j := findOneJob(t, tx)
	require.NotNil(t, j)

	assert.Equal(t, int32(1), j.ErrorCount)
	assert.NotEqual(t, pgtype.Null, j.LastError.Status)
	assert.Contains(t, j.LastError.String, "the panic msg\n")
	// basic check if a stacktrace is there - not the stacktrace format itself
	assert.Contains(t, j.LastError.String, "worker.go:")
	assert.Contains(t, j.LastError.String, "worker_test.go:")
}

func TestWorkerWorkOneTypeNotInMap(t *testing.T) {
	c := openTestClientPGXv3(t)

	currentConns := c.pool.Stat().CurrentConnections
	availConns := c.pool.Stat().AvailableConnections

	wm := WorkMap{}
	w := NewWorker(c, wm)

	didWork := w.WorkOne()
	assert.False(t, didWork)

	err := c.Enqueue(&Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork = w.WorkOne()
	assert.True(t, didWork)

	assert.Equal(t, currentConns, c.pool.Stat().CurrentConnections)
	assert.Equal(t, availConns, c.pool.Stat().AvailableConnections)

	tx, err := c.pool.Begin()
	require.NoError(t, err)
	defer func() {
		err := tx.Rollback()
		assert.NoError(t, err)
	}()

	j := findOneJob(t, tx)
	require.NotNil(t, j)

	assert.Equal(t, int32(1), j.ErrorCount)
	require.NotEqual(t, pgtype.Null, j.LastError.Status)
	assert.Contains(t, j.LastError.String, `unknown job type: "MyJob"`)
}
