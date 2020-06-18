package gue

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/adapter"
	adapterTesting "github.com/vgarvardt/gue/adapter/testing"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func TestWorkerWorkOne(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testWorkerWorkOne(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testWorkerWorkOne(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testWorkerWorkOne(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testWorkerWorkOne(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	success := false
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			success = true
			return nil
		},
	}
	w := NewWorker(c, wm)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork = w.WorkOne(ctx)
	assert.True(t, didWork)
	assert.True(t, success)
}

func TestWorker_Start(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testWorkerStart(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testWorkerStart(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testWorkerStart(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testWorkerStart(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)

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
	t.Run("pgx/v3", func(t *testing.T) {
		testWorkerPoolStart(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testWorkerPoolStart(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testWorkerPoolStart(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testWorkerPoolStart(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)

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
	b.Run("pgx/v3", func(b *testing.B) {
		benchmarkWorker(b, adapterTesting.OpenTestPoolPGXv3(b))
	})
	b.Run("pgx/v4", func(b *testing.B) {
		benchmarkWorker(b, adapterTesting.OpenTestPoolPGXv4(b))
	})
	b.Run("lib/pq", func(b *testing.B) {
		benchmarkWorker(b, adapterTesting.OpenTestPoolLibPQ(b))
	})
}

func benchmarkWorker(b *testing.B, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	w := NewWorker(c, WorkMap{"Nil": nilWorker})

	for i := 0; i < b.N; i++ {
		if err := c.Enqueue(ctx, &Job{Type: "Nil"}); err != nil {
			log.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WorkOne(ctx)
	}
}

func nilWorker(j *Job) error {
	return nil
}

func TestWorkerWorkReturnsError(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testWorkerWorkReturnsError(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testWorkerWorkReturnsError(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testWorkerWorkReturnsError(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testWorkerWorkReturnsError(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	connService, err := connPool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		connService.Release()
	}()

	called := 0
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			called++
			return errors.New("the error msg")
		},
	}
	w := NewWorker(c, wm)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork = w.WorkOne(ctx)
	assert.True(t, didWork)
	assert.Equal(t, 1, called)

	tx, err := connService.Begin(ctx)
	require.NoError(t, err)
	defer func() {
		err := tx.Rollback(ctx)
		assert.NoError(t, err)
	}()

	j := findOneJob(t, tx)
	require.NotNil(t, j)

	assert.Equal(t, int32(1), j.ErrorCount)
	assert.NotEqual(t, pgtype.Null, j.LastError.Status)
	assert.Equal(t, "the error msg", j.LastError.String)
}

func TestWorkerWorkRescuesPanic(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testWorkerWorkRescuesPanic(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testWorkerWorkRescuesPanic(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testWorkerWorkRescuesPanic(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testWorkerWorkRescuesPanic(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	connService, err := connPool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		connService.Release()
	}()

	called := 0
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			called++
			panic("the panic msg")
		},
	}
	w := NewWorker(c, wm)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	w.WorkOne(ctx)
	assert.Equal(t, 1, called)

	tx, err := connService.Begin(ctx)
	require.NoError(t, err)
	defer func() {
		err := tx.Rollback(ctx)
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
	t.Run("pgx/v3", func(t *testing.T) {
		testWorkerWorkOneTypeNotInMap(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testWorkerWorkOneTypeNotInMap(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testWorkerWorkOneTypeNotInMap(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testWorkerWorkOneTypeNotInMap(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	currentConns := c.pool.Stat().CurrentConnections
	availConns := c.pool.Stat().AvailableConnections

	wm := WorkMap{}
	w := NewWorker(c, wm)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork = w.WorkOne(ctx)
	assert.True(t, didWork)

	assert.Equal(t, currentConns, c.pool.Stat().CurrentConnections)
	assert.Equal(t, availConns, c.pool.Stat().AvailableConnections)

	connService, err := connPool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		connService.Release()
	}()

	tx, err := connService.Begin(ctx)
	require.NoError(t, err)
	defer func() {
		err := tx.Rollback(ctx)
		assert.NoError(t, err)
	}()

	j := findOneJob(t, tx)
	require.NotNil(t, j)

	assert.Equal(t, int32(1), j.ErrorCount)
	require.NotEqual(t, pgtype.Null, j.LastError.Status)
	assert.Contains(t, j.LastError.String, `unknown job type: "MyJob"`)
}
