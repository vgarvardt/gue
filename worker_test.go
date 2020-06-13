package gue

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func TestWorkerWorkOne(t *testing.T) {
	c := openTestClient(t)

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

func TestWorkerShutdown(t *testing.T) {
	c := openTestClient(t)

	w := NewWorker(c, WorkMap{})
	finished := false
	go func() {
		w.Work()
		finished = true
	}()
	w.Shutdown()

	assert.True(t, finished)
	assert.True(t, w.done)
}

func BenchmarkWorker(b *testing.B) {
	c := openTestClient(b)
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
	c := openTestClient(t)

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
	c := openTestClient(t)

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
	c := openTestClient(t)

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
