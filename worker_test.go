package gue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/vgarvardt/gue/v3/adapter"
	adapterTesting "github.com/vgarvardt/gue/v3/adapter/testing"
)

type mockHook struct {
	called int
	ctx    context.Context
	j      *Job
	err    error
}

func (h *mockHook) handler(ctx context.Context, j *Job, err error) {
	h.called++
	h.ctx, h.j, h.err = ctx, j, err
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
	t.Run("go-pg/v10", func(t *testing.T) {
		testWorkerWorkOne(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testWorkerWorkOne(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	var success bool
	wm := WorkMap{
		"MyJob": func(ctx context.Context, j *Job) error {
			success = true
			return nil
		},
	}

	jobLockedHook := new(mockHook)
	unknownJobTypeHook := new(mockHook)
	jobDoneHook := new(mockHook)

	w := NewWorker(
		c,
		wm,
		WithWorkerHooksJobLocked(jobLockedHook.handler),
		WithWorkerHooksUnknownJobType(unknownJobTypeHook.handler),
		WithWorkerHooksJobDone(jobDoneHook.handler),
	)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork = w.WorkOne(ctx)
	assert.True(t, didWork)
	assert.True(t, success)

	assert.Equal(t, 1, jobLockedHook.called)
	assert.NotNil(t, jobLockedHook.j)
	assert.NoError(t, jobLockedHook.err)

	assert.Equal(t, 0, unknownJobTypeHook.called)

	assert.Equal(t, 1, jobDoneHook.called)
	assert.NotNil(t, jobDoneHook.j)
	assert.NoError(t, jobDoneHook.err)
}

func TestWorker_Run(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testWorkerRun(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testWorkerRun(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testWorkerRun(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
	t.Run("go-pg/v10", func(t *testing.T) {
		testWorkerRun(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testWorkerRun(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)

	w := NewWorker(c, WorkMap{})

	ctx, cancel := context.WithCancel(context.Background())

	var grp errgroup.Group
	grp.Go(func() error {
		return w.Run(ctx)
	})

	// give worker time to start
	time.Sleep(time.Second)

	assert.True(t, w.running)

	// try to start one more time to get an error about already running worker
	assert.Error(t, w.Run(context.Background()))

	cancel()
	assert.NoError(t, grp.Wait())

	assert.False(t, w.running)
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
	t.Run("go-pg/v10", func(t *testing.T) {
		testWorkerStart(t, adapterTesting.OpenTestPoolGoPGv10(t))
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

func TestWorkerPool_Run(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testWorkerPoolRun(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testWorkerPoolRun(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testWorkerPoolRun(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
	t.Run("go-pg/v10", func(t *testing.T) {
		testWorkerPoolRun(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testWorkerPoolRun(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)

	poolSize := 2
	w := NewWorkerPool(c, WorkMap{}, poolSize)

	ctx, cancel := context.WithCancel(context.Background())

	var grp errgroup.Group
	grp.Go(func() error {
		return w.Run(ctx)
	})

	// give worker time to start
	time.Sleep(time.Second)

	assert.True(t, w.running)
	for i := range w.workers {
		assert.True(t, w.workers[i].running)
	}

	// try to start one more time to get an error about already running worker pool
	assert.Error(t, w.Run(context.Background()))

	cancel()

	require.NoError(t, grp.Wait())

	assert.False(t, w.running)
	for i := range w.workers {
		assert.False(t, w.workers[i].running)
	}
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
	t.Run("go-pg/v10", func(t *testing.T) {
		testWorkerPoolStart(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testWorkerPoolStart(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)

	poolSize := 2
	w := NewWorkerPool(c, WorkMap{}, poolSize)

	ctx, cancel := context.WithCancel(context.Background())
	err := w.Start(ctx)
	require.NoError(t, err)

	// give worker time to start
	time.Sleep(time.Second)

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
	b.Run("go-pg/v10", func(b *testing.B) {
		benchmarkWorker(b, adapterTesting.OpenTestPoolGoPGv10(b))
	})
}

func benchmarkWorker(b *testing.B, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	w := NewWorker(c, WorkMap{"Nil": nilWorker})

	for i := 0; i < b.N; i++ {
		if err := c.Enqueue(ctx, &Job{Type: "Nil"}); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WorkOne(ctx)
	}
}

func nilWorker(context.Context, *Job) error {
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
	t.Run("go-pg/v10", func(t *testing.T) {
		testWorkerWorkReturnsError(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testWorkerWorkReturnsError(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	called := 0
	wm := WorkMap{
		"MyJob": func(ctx context.Context, j *Job) error {
			called++
			return errors.New("the error msg")
		},
	}

	jobLockedHook := new(mockHook)
	unknownJobTypeHook := new(mockHook)
	jobDoneHook := new(mockHook)

	w := NewWorker(
		c,
		wm,
		WithWorkerHooksJobLocked(jobLockedHook.handler),
		WithWorkerHooksUnknownJobType(unknownJobTypeHook.handler),
		WithWorkerHooksJobDone(jobDoneHook.handler),
	)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork = w.WorkOne(ctx)
	assert.True(t, didWork)
	assert.Equal(t, 1, called)

	assert.Equal(t, 1, jobLockedHook.called)
	assert.NotNil(t, jobLockedHook.j)
	assert.NoError(t, jobLockedHook.err)

	assert.Equal(t, 0, unknownJobTypeHook.called)

	assert.Equal(t, 1, jobDoneHook.called)
	assert.NotNil(t, jobDoneHook.j)
	assert.Error(t, jobDoneHook.err)

	j := findOneJob(t, connPool)
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
	t.Run("go-pg/v10", func(t *testing.T) {
		testWorkerWorkRescuesPanic(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testWorkerWorkRescuesPanic(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	called := 0
	wm := WorkMap{
		"MyJob": func(ctx context.Context, j *Job) error {
			called++
			panic("the panic msg")
		},
	}
	w := NewWorker(c, wm)

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	w.WorkOne(ctx)
	assert.Equal(t, 1, called)

	j := findOneJob(t, connPool)
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
	t.Run("go-pg/v10", func(t *testing.T) {
		testWorkerWorkOneTypeNotInMap(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testWorkerWorkOneTypeNotInMap(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	wm := WorkMap{}

	jobLockedHook := new(mockHook)
	unknownJobTypeHook := new(mockHook)
	jobDoneHook := new(mockHook)

	w := NewWorker(
		c,
		wm,
		WithWorkerHooksJobLocked(jobLockedHook.handler),
		WithWorkerHooksUnknownJobType(unknownJobTypeHook.handler),
		WithWorkerHooksJobDone(jobDoneHook.handler),
	)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	assert.Equal(t, 0, jobLockedHook.called)
	assert.Equal(t, 0, unknownJobTypeHook.called)
	assert.Equal(t, 0, jobDoneHook.called)

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork = w.WorkOne(ctx)
	assert.True(t, didWork)

	assert.Equal(t, 1, jobLockedHook.called)
	assert.NotNil(t, jobLockedHook.j)
	assert.NoError(t, jobLockedHook.err)

	assert.Equal(t, 1, unknownJobTypeHook.called)
	assert.NotNil(t, jobDoneHook.j)
	assert.Error(t, jobDoneHook.err)

	assert.Equal(t, 0, jobDoneHook.called)

	j := findOneJob(t, connPool)
	require.NotNil(t, j)

	assert.Equal(t, int32(1), j.ErrorCount)
	require.NotEqual(t, pgtype.Null, j.LastError.Status)
	assert.Contains(t, j.LastError.String, `unknown job type: "MyJob"`)
}
