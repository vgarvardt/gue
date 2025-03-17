package gue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/sync/errgroup"

	"github.com/sadpenguinn/gue/v6/adapter"
	adapterTesting "github.com/sadpenguinn/gue/v6/adapter/testing"
	adapterZap "github.com/sadpenguinn/gue/v6/adapter/zap"
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
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerWorkOne(t, openFunc(t))
		})
	}
}

func testWorkerWorkOne(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

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

	w, err := NewWorker(
		c,
		wm,
		WithWorkerHooksJobLocked(jobLockedHook.handler),
		WithWorkerHooksUnknownJobType(unknownJobTypeHook.handler),
		WithWorkerHooksJobDone(jobDoneHook.handler),
	)
	require.NoError(t, err)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
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
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerRun(t, openFunc(t))
		})
	}
}

func testWorkerRun(t *testing.T, connPool adapter.ConnPool) {
	c, err := NewClient(connPool)
	require.NoError(t, err)

	w, err := NewWorker(c, WorkMap{})
	require.NoError(t, err)

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

func TestWorkerPool_Run(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerPoolRun(t, openFunc(t))
		})
	}
}

func testWorkerPoolRun(t *testing.T, connPool adapter.ConnPool) {
	c, err := NewClient(connPool)
	require.NoError(t, err)

	var (
		m          sync.Mutex
		jobsWorked int
	)

	w, err := NewWorkerPool(c, WorkMap{
		"dummy-job": func(ctx context.Context, j *Job) error {
			m.Lock()
			defer m.Unlock()

			assert.NotEqual(t, WorkerIdxUnknown, GetWorkerIdx(ctx))

			jobsWorked++
			return nil
		},
	}, 2)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	jobsToWork := 15
	for i := 0; i < jobsToWork; i++ {
		err := c.Enqueue(ctx, &Job{Type: "dummy-job"})
		require.NoError(t, err)
	}

	var grp errgroup.Group
	grp.Go(func() error {
		return w.Run(ctx)
	})

	// give worker time to work the jobs
	time.Sleep(5 * time.Second)

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

	assert.Equal(t, jobsToWork, jobsWorked)
}

func TestWorkerPool_WorkOne(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerPoolWorkOne(t, openFunc(t))
		})
	}
}

func testWorkerPoolWorkOne(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

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

	w, err := NewWorkerPool(
		c,
		wm,
		3,
		WithPoolHooksJobLocked(jobLockedHook.handler),
		WithPoolHooksUnknownJobType(unknownJobTypeHook.handler),
		WithPoolHooksJobDone(jobDoneHook.handler),
	)
	require.NoError(t, err)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
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

func TestWorkerWorkReturnsError(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerWorkReturnsError(t, openFunc(t))
		})
	}
}

func testWorkerWorkReturnsError(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

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

	w, err := NewWorker(
		c,
		wm,
		WithWorkerHooksJobLocked(jobLockedHook.handler),
		WithWorkerHooksUnknownJobType(unknownJobTypeHook.handler),
		WithWorkerHooksJobDone(jobDoneHook.handler),
	)
	require.NoError(t, err)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	job := Job{Type: "MyJob"}
	err = c.Enqueue(ctx, &job)
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

	j, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.Equal(t, int32(1), j.ErrorCount)
	assert.True(t, j.LastError.Valid)
	assert.Equal(t, "the error msg", j.LastError.String)
}

func TestWorkerWorkRescuesPanic(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerWorkRescuesPanic(t, openFunc(t))
		})
	}
}

func testWorkerWorkRescuesPanic(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()
	observed, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(observed)

	c, err := NewClient(connPool)
	require.NoError(t, err)

	var handlerCalled int
	wm := WorkMap{
		"MyJob": func(ctx context.Context, j *Job) error {
			handlerCalled++
			panic("the panic msg")
		},
	}

	var hookDoneCalled int
	w, err := NewWorker(c, wm, WithWorkerLogger(adapterZap.New(logger)), WithWorkerHooksJobDone(func(ctx context.Context, j *Job, err error) {
		hookDoneCalled++
		assert.ErrorIs(t, err, ErrJobPanicked)
	}))
	require.NoError(t, err)

	job := Job{Type: "MyJob"}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	w.WorkOne(ctx)
	require.Equal(t, 1, handlerCalled)
	require.Equal(t, 1, hookDoneCalled)

	j, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.Equal(t, int32(1), j.ErrorCount)
	assert.True(t, j.LastError.Valid)
	assert.Contains(t, j.LastError.String, "the panic msg\n")
	// basic check if a stacktrace is there - not the stacktrace format itself
	assert.Contains(t, j.LastError.String, "worker.go:")
	assert.Contains(t, j.LastError.String, "worker_test.go:")

	panicLogs := logs.FilterLevelExact(zapcore.ErrorLevel).FilterMessage("Job panicked").All()
	require.Len(t, panicLogs, 1)
}

func TestWorkerWorkWithWorkerHooksJobDonePanic(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerWorkWithWorkerHooksJobDonePanic(t, openFunc(t))
		})
	}
}

func testWorkerWorkWithWorkerHooksJobDonePanic(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	called := 0
	wm := WorkMap{
		"MyJob": func(ctx context.Context, j *Job) error {
			called++
			return nil
		},
	}
	w, err := NewWorker(c, wm, WithWorkerHooksJobDone(func(ctx context.Context, j *Job, err error) {
		panic("panic from the hook job done")
	}))
	require.NoError(t, err)

	job := Job{Type: "MyJob"}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	w.WorkOne(ctx)
	assert.Equal(t, 1, called)

	j, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.Equal(t, int32(1), j.ErrorCount)
	assert.True(t, j.LastError.Valid)
	assert.Contains(t, j.LastError.String, "panic from the hook job done\n")
	// basic check if a stacktrace is there - not the stacktrace format itself
	assert.Contains(t, j.LastError.String, "worker.go:")
	assert.Contains(t, j.LastError.String, "worker_test.go:")
}

func TestWorkerWorkOneTypeNotInMap(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerWorkOneTypeNotInMap(t, openFunc(t))
		})
	}
}

func testWorkerWorkOneTypeNotInMap(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	wm := WorkMap{}

	jobLockedHook := new(mockHook)
	unknownJobTypeHook := new(mockHook)
	jobDoneHook := new(mockHook)

	w, err := NewWorker(
		c,
		wm,
		WithWorkerHooksJobLocked(jobLockedHook.handler),
		WithWorkerHooksUnknownJobType(unknownJobTypeHook.handler),
		WithWorkerHooksJobDone(jobDoneHook.handler),
	)
	require.NoError(t, err)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	assert.Equal(t, 0, jobLockedHook.called)
	assert.Equal(t, 0, unknownJobTypeHook.called)
	assert.Equal(t, 0, jobDoneHook.called)

	job := Job{Type: "MyJob"}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	didWork = w.WorkOne(ctx)
	assert.True(t, didWork)

	assert.Equal(t, 1, jobLockedHook.called)
	assert.NotNil(t, jobLockedHook.j)
	assert.NoError(t, jobLockedHook.err)

	assert.Equal(t, 1, unknownJobTypeHook.called)
	assert.NotNil(t, unknownJobTypeHook.j)
	assert.Error(t, unknownJobTypeHook.err)

	assert.Equal(t, 0, jobDoneHook.called)

	j, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.Equal(t, int32(1), j.ErrorCount)
	assert.True(t, j.LastError.Valid)
	assert.Contains(t, j.LastError.String, `unknown job type: "MyJob"`)
}

func TestWorkerWorkOneUnknownTypeWM(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerWorkOneUnknownTypeWM(t, openFunc(t))
		})
	}
}

func testWorkerWorkOneUnknownTypeWM(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	wm := WorkMap{}

	var unknownWFCalled int
	unknownJobTypeHook := new(mockHook)

	w, err := NewWorker(
		c,
		wm,
		WithWorkerHooksUnknownJobType(unknownJobTypeHook.handler),
		WithWorkerUnknownJobWorkFunc(func(ctx context.Context, j *Job) error {
			unknownWFCalled++
			return nil
		}),
	)
	require.NoError(t, err)

	didWork := w.WorkOne(ctx)
	assert.False(t, didWork)

	assert.Equal(t, 0, unknownJobTypeHook.called)

	// random job type, because we do not really care in this case
	err = c.Enqueue(ctx, &Job{Type: ulid.MustNew(ulid.Now(), ulid.DefaultEntropy()).String()})
	require.NoError(t, err)

	didWork = w.WorkOne(ctx)
	assert.True(t, didWork)

	assert.Equal(t, 0, unknownJobTypeHook.called)
	assert.Equal(t, 1, unknownWFCalled)
}

// TestWorker_WorkOne_errorHookTx tests that JobDone hooks are running in the same transaction as the errored job
func TestWorker_WorkOneErrorHookTx(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testWorkerWorkOneErrorHookTx(t, openFunc(t))
		})
	}
}

func testWorkerWorkOneErrorHookTx(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	called := 0
	jobErr := errors.New("the error msg")
	wm := WorkMap{
		"MyJob": func(ctx context.Context, j *Job) error {
			called++
			return jobErr
		},
	}

	jobDoneHook := func(ctx context.Context, j *Job, err error) {
		assert.Error(t, err)
		assert.Equal(t, jobErr, err)

		// ensure that transaction is still active
		var count int64
		txErr := j.Tx().QueryRow(ctx, `SELECT COUNT(1) FROM gue_jobs`).Scan(&count)
		assert.NoError(t, txErr)
		assert.Greater(t, count, int64(0))
	}

	w, err := NewWorker(
		c,
		wm,
		WithWorkerHooksJobDone(jobDoneHook),
	)
	require.NoError(t, err)

	job := Job{Type: "MyJob"}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	didWork := w.WorkOne(ctx)
	assert.True(t, didWork)
	assert.Equal(t, 1, called)
}

func TestNewWorker_GracefulShutdown(t *testing.T) {
	connPool := adapterTesting.OpenTestPoolLibPQ(t)

	c, err := NewClient(connPool)
	require.NoError(t, err)

	var jobCancelled bool
	wm := WorkMap{
		"MyJob": func(ctx context.Context, j *Job) error {
			select {
			case <-ctx.Done():
				jobCancelled = true
			case <-time.After(5 * time.Second):
				jobCancelled = false
			}

			return nil
		},
	}

	ctxNonGraceful, cancelNonGraceful := context.WithTimeout(context.Background(), time.Second)
	defer cancelNonGraceful()

	err = c.Enqueue(ctxNonGraceful, &Job{Type: "MyJob"})
	require.NoError(t, err)

	wNonGraceful, err := NewWorker(c, wm)
	require.NoError(t, err)

	chDone := make(chan bool)
	t.Cleanup(func() {
		close(chDone)
	})

	go func() {
		err := wNonGraceful.Run(ctxNonGraceful)
		assert.NoError(t, err)
		chDone <- true
	}()

	<-chDone
	require.True(t, jobCancelled)

	ctxGraceful, cancelGraceful := context.WithTimeout(context.Background(), time.Second)
	defer cancelGraceful()

	err = c.Enqueue(ctxGraceful, &Job{Type: "MyJob"})
	require.NoError(t, err)

	wGraceful, err := NewWorker(c, wm, WithWorkerGracefulShutdown(nil))
	require.NoError(t, err)

	go func() {
		err := wGraceful.Run(ctxGraceful)
		assert.NoError(t, err)
		chDone <- true
	}()

	<-chDone
	require.False(t, jobCancelled)
}

func TestNewWorker_JobTTL(t *testing.T) {
	connPool := adapterTesting.OpenTestPoolLibPQ(t)

	c, err := NewClient(connPool)
	require.NoError(t, err)

	var jobCancelled bool
	wm := WorkMap{
		"MyJob": func(ctx context.Context, j *Job) error {
			select {
			case <-ctx.Done():
				jobCancelled = true
			case <-time.After(5 * time.Second):
				jobCancelled = false
			}

			return nil
		},
	}

	wNoJobTTL, err := NewWorker(c, wm)
	require.NoError(t, err)
	wWithJobTTL, err := NewWorker(c, wm, WithWorkerJobTTL(2*time.Second))
	require.NoError(t, err)

	err = c.Enqueue(context.Background(), &Job{Type: "MyJob"})
	require.NoError(t, err)
	err = c.Enqueue(context.Background(), &Job{Type: "MyJob"})
	require.NoError(t, err)

	didWork := wNoJobTTL.WorkOne(context.Background())
	require.True(t, didWork)
	require.False(t, jobCancelled)

	didWork = wWithJobTTL.WorkOne(context.Background())
	require.True(t, didWork)
	require.True(t, jobCancelled)
}

func TestNewWorkerPool_GracefulShutdown(t *testing.T) {
	connPool := adapterTesting.OpenTestPoolLibPQ(t)

	c, err := NewClient(connPool)
	require.NoError(t, err)

	const numWorkers = 5
	jobCancelled, jobFinished := 0, 0
	wm := WorkMap{
		"MyJob": func(ctx context.Context, j *Job) error {
			select {
			case <-ctx.Done():
				jobCancelled++
			case <-time.After(5 * time.Second):
				jobFinished++
			}

			return nil
		},
	}

	ctxNonGraceful, cancelNonGraceful := context.WithTimeout(context.Background(), time.Second)
	defer cancelNonGraceful()

	for i := 0; i < numWorkers; i++ {
		err = c.Enqueue(ctxNonGraceful, &Job{Type: "MyJob"})
		require.NoError(t, err)
	}

	wNonGraceful, err := NewWorkerPool(c, wm, numWorkers)
	require.NoError(t, err)

	chDone := make(chan bool)
	go func() {
		err := wNonGraceful.Run(ctxNonGraceful)
		assert.NoError(t, err)
		chDone <- true
	}()

	<-chDone
	assert.Equal(t, numWorkers, jobCancelled)
	assert.Equal(t, 0, jobFinished)

	jobCancelled, jobFinished = 0, 0
	ctxGraceful, cancelGraceful := context.WithTimeout(context.Background(), time.Second)
	defer cancelGraceful()

	err = c.Enqueue(ctxGraceful, &Job{Type: "MyJob"})
	require.NoError(t, err)

	wGraceful, err := NewWorkerPool(c, wm, numWorkers, WithPoolGracefulShutdown(nil))
	require.NoError(t, err)

	go func() {
		err := wGraceful.Run(ctxGraceful)
		assert.NoError(t, err)
		chDone <- true
	}()

	<-chDone
	assert.Equal(t, 0, jobCancelled)
	assert.Equal(t, numWorkers, jobFinished)
}
