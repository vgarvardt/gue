package gue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/adapter"
	adapterTesting "github.com/vgarvardt/gue/adapter/testing"
)

func TestLockJob(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	jobType := "MyJob"
	err := c.Enqueue(ctx, &Job{Type: jobType})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)

	require.NotNil(t, j.conn)
	require.NotNil(t, j.pool)
	defer j.Done(ctx)

	// check values of returned Job
	assert.Greater(t, j.ID, int64(0))
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, int16(100), j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, jobType, j.Type)
	assert.Equal(t, []byte(`[]`), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.NotEqual(t, pgtype.Present, j.LastError.Status)

	// check for advisory lock
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	err = j.pool.QueryRow(ctx, query, "advisory", j.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// make sure conn was checked out of pool
	stat := c.pool.Stat()
	total, available := stat.CurrentConnections, stat.AvailableConnections
	assert.Equal(t, total-1, available)

	err = j.Delete(ctx)
	require.NoError(t, err)
}

func TestLockJobAlreadyLocked(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer j.Done(ctx)

	j2, err := c.LockJob(ctx, "")
	require.NoError(t, err)

	if j2 != nil {
		defer j2.Done(ctx)
		require.Fail(t, "wanted no job, got %+v", j2)
	}
}

func TestLockJobNoJob(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestLockJobCustomQueue(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob", Queue: "extra_priority"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	if j != nil {
		j.Done(ctx)
		assert.Fail(t, "expected no job to be found with empty queue name, got %+v", j)
	}

	j, err = c.LockJob(ctx, "extra_priority")
	require.NoError(t, err)
	defer j.Done(ctx)
	require.NotNil(t, j)

	err = j.Delete(ctx)
	require.NoError(t, err)
}

func TestJobConn(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer j.Done(ctx)

	assert.Equal(t, j.conn, j.Conn())
}

func TestJobConnRace(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer j.Done(ctx)

	var wg sync.WaitGroup
	wg.Add(2)

	// call Conn and Done in different goroutines to make sure they are safe from
	// races.
	go func() {
		_ = j.Conn()
		wg.Done()
	}()
	go func() {
		j.Done(ctx)
		wg.Done()
	}()
	wg.Wait()
}

// Test the race condition in LockJob
func TestLockJobAdvisoryRace(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolMaxConnsPGXv3(t, 2))
	ctx := context.Background()

	// *pgx.ConnPool doesn't support pools of only one connection.  Make sure
	// the other one is busy so we know which backend will be used by LockJob
	// below.
	unusedConn, err := c.pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.pool.Release(unusedConn)

	// We use two jobs: the first one is concurrently deleted, and the second
	// one is returned by LockJob after recovering from the race condition.
	for i := 0; i < 2; i++ {
		err := c.Enqueue(ctx, &Job{Type: "MyJob"})
		require.NoError(t, err)
	}

	// helper functions
	getBackendPID := func(conn adapter.Conn) int32 {
		var backendPID int32
		err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&backendPID)
		require.NoError(t, err)
		return backendPID
	}

	waitUntilBackendIsWaiting := func(backendPID int32, name string) {
		conn := adapterTesting.OpenTestConnPGXv3(t)
		i := 0
		for {
			var waiting bool
			err := conn.QueryRow(ctx, `SELECT wait_event is not null from pg_stat_activity where pid=$1`, backendPID).Scan(&waiting)
			require.NoError(t, err)

			if waiting {
				break
			} else {
				i++
				if i >= 10000/50 {
					panic(fmt.Sprintf("timed out while waiting for %s", name))
				}

				time.Sleep(50 * time.Millisecond)
			}
		}

	}

	// Reproducing the race condition is a bit tricky.  The idea is to form a
	// lock queue on the relation that looks like this:
	//
	//   AccessExclusive <- AccessShare  <- AccessExclusive ( <- AccessShare )
	//
	// where the leftmost AccessShare lock is the one implicitly taken by the
	// sqlLockJob query.  Once we release the leftmost AccessExclusive lock
	// without releasing the rightmost one, the session holding the rightmost
	// AccessExclusiveLock can run the necessary DELETE before the sqlCheckJob
	// query runs (since it'll be blocked behind the rightmost AccessExclusive
	// Lock).
	//
	deletedJobIDChan := make(chan int64, 1)
	lockJobBackendIDChan := make(chan int32)
	secondAccessExclusiveBackendIDChan := make(chan int32)

	go func() {
		conn := adapterTesting.OpenTestConnPGXv3(t)
		defer func() {
			err := conn.Close(ctx)
			assert.NoError(t, err)
		}()

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `LOCK TABLE que_jobs IN ACCESS EXCLUSIVE MODE`)
		require.NoError(t, err)

		// first wait for LockJob to appear behind us
		backendID := <-lockJobBackendIDChan
		waitUntilBackendIsWaiting(backendID, "LockJob")

		// then for the AccessExclusive lock to appear behind that one
		backendID = <-secondAccessExclusiveBackendIDChan
		waitUntilBackendIsWaiting(backendID, "second access exclusive lock")

		err = tx.Rollback(ctx)
		require.NoError(t, err)
	}()

	go func() {
		conn := adapterTesting.OpenTestConnPGXv3(t)
		defer func() {
			err := conn.Close(ctx)
			assert.NoError(t, err)
		}()

		// synchronization point
		secondAccessExclusiveBackendIDChan <- getBackendPID(conn)

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `LOCK TABLE que_jobs IN ACCESS EXCLUSIVE MODE`)
		require.NoError(t, err)

		// Fake a concurrent transaction grabbing the job
		var jid int64
		err = tx.QueryRow(ctx, `
			DELETE FROM que_jobs
			WHERE job_id =
				(SELECT min(job_id)
				 FROM que_jobs)
			RETURNING job_id
		`).Scan(&jid)
		require.NoError(t, err)

		deletedJobIDChan <- jid

		err = tx.Commit(ctx)
		require.NoError(t, err)
	}()

	conn, err := c.pool.Acquire(ctx)
	require.NoError(t, err)

	ourBackendID := getBackendPID(conn)
	c.pool.Release(conn)

	// synchronization point
	lockJobBackendIDChan <- ourBackendID

	job, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	defer job.Done(ctx)

	deletedJobID := <-deletedJobIDChan

	require.Less(t, deletedJobID, job.ID)
}

func TestJobDelete(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer j.Done(ctx)

	err = j.Delete(ctx)
	require.NoError(t, err)

	// make sure job was deleted
	j2 := findOneJob(t, c.pool)
	assert.Nil(t, j2)
}

func TestJobDone(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	j.Done(ctx)

	// make sure conn and pool were cleared
	assert.Nil(t, j.conn)
	assert.Nil(t, j.pool)

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype = $1 AND objid= $2::bigint"
	err = c.pool.QueryRow(ctx, query, "advisory", j.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// make sure conn was returned to pool
	stat := c.pool.Stat()
	total, available := stat.CurrentConnections, stat.AvailableConnections
	assert.Equal(t, available, total)
}

func TestJobDoneMultiple(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	j.Done(ctx)
	// try calling Done() again
	j.Done(ctx)
}

func TestJobDeleteFromTx(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	// get the job's database connection
	conn := j.Conn()
	require.NotNil(t, conn)

	// start a transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// delete the job
	err = j.Delete(ctx)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// mark as done
	j.Done(ctx)

	// make sure the job is gone
	j2 := findOneJob(t, c.pool)
	assert.Nil(t, j2)
}

func TestJobDeleteFromTxRollback(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j1, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j1)

	// get the job's database connection
	conn := j1.Conn()
	require.NotNil(t, conn)

	// start a transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// delete the job
	err = j1.Delete(ctx)
	require.NoError(t, err)

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	// mark as done
	j1.Done(ctx)

	// make sure the job still exists and matches j1
	j2 := findOneJob(t, c.pool)
	require.NotNil(t, j2)

	assert.Equal(t, j1.ID, j2.ID)
}

func TestJobError(t *testing.T) {
	c := NewClient(adapterTesting.OpenTestPoolPGXv3(t))
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer j.Done(ctx)

	msg := "world\nended"
	err = j.Error(ctx, msg)
	require.NoError(t, err)
	j.Done(ctx)

	// make sure job was not deleted
	j2 := findOneJob(t, c.pool)
	require.NotNil(t, j2)
	defer j2.Done(ctx)

	assert.NotEqual(t, pgtype.Null, j2.LastError.Status)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	err = c.pool.QueryRow(ctx, query, "advisory", j.ID).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, int64(0), count, "advisory lock was not released")

	// make sure conn was returned to pool
	stat := c.pool.Stat()
	total, available := stat.CurrentConnections, stat.AvailableConnections
	assert.Equal(t, available, total)
}
