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
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJob(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJob(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testLockJob(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testLockJob(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
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
	connFind, err := connPool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		connFind.Release()
	}()

	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	err = connFind.QueryRow(ctx, query, "advisory", j.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// make sure conn was checked out of pool
	stat := c.pool.Stat()
	total, available := stat.CurrentConnections, stat.AvailableConnections
	// one connection should be acquired inside the job and one to use for find
	assert.Equal(t, total-2, available)

	err = j.Delete(ctx)
	require.NoError(t, err)
}

func TestLockJobAlreadyLocked(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJobAlreadyLocked(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJobAlreadyLocked(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testLockJobAlreadyLocked(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testLockJobAlreadyLocked(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
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
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJobNoJob(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJobNoJob(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testLockJobNoJob(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testLockJobNoJob(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestLockJobCustomQueue(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJobCustomQueue(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJobCustomQueue(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testLockJobCustomQueue(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testLockJobCustomQueue(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
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
	t.Run("pgx/v3", func(t *testing.T) {
		testJobConn(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobConn(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobConn(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobConn(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
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
	t.Run("pgx/v3", func(t *testing.T) {
		testJobConnRace(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobConnRace(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobConnRace(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobConnRace(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer j.Done(ctx)

	var wg sync.WaitGroup
	wg.Add(2)

	// call Conn and Done in different goroutines to make sure they are safe from races
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
	t.Skip()

	t.Run("pgx/v3", func(t *testing.T) {
		testLockJobAdvisoryRace(t, adapterTesting.OpenTestPoolMaxConnsPGXv3(t, 5))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJobAdvisoryRace(t, adapterTesting.OpenTestPoolMaxConnsPGXv4(t, 5))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testLockJobAdvisoryRace(t, adapterTesting.OpenTestPoolMaxConnsLibPQ(t, 5))
	})
}

func testLockJobAdvisoryRace(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	// acquire connections that are going to be used exclusively during the test,
	// make sure this test gets the pool with max 5 connections as we must be sure
	// which connection is doing what
	accessLockerConn1, err := c.pool.Acquire(ctx)
	require.NoError(t, err)
	defer accessLockerConn1.Release()

	accessLockerConn2, err := c.pool.Acquire(ctx)
	require.NoError(t, err)
	defer accessLockerConn2.Release()

	eventWaiterConn1, err := c.pool.Acquire(ctx)
	require.NoError(t, err)
	defer eventWaiterConn1.Release()

	eventWaiterConn2, err := c.pool.Acquire(ctx)
	require.NoError(t, err)
	defer eventWaiterConn2.Release()

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

	waitUntilBackendIsWaiting := func(conn adapter.Conn, backendPID int32, name string) {
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
		tx, err := accessLockerConn1.Begin(ctx)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `LOCK TABLE que_jobs IN ACCESS EXCLUSIVE MODE`)
		require.NoError(t, err)

		// first wait for LockJob to appear behind us
		backendID := <-lockJobBackendIDChan
		waitUntilBackendIsWaiting(eventWaiterConn1, backendID, "LockJob")

		// then for the AccessExclusive lock to appear behind that one
		backendID = <-secondAccessExclusiveBackendIDChan
		waitUntilBackendIsWaiting(eventWaiterConn2, backendID, "second access exclusive lock")

		err = tx.Rollback(ctx)
		require.NoError(t, err)
	}()

	go func() {
		// synchronization point
		secondAccessExclusiveBackendIDChan <- getBackendPID(accessLockerConn2)

		tx, err := accessLockerConn2.Begin(ctx)
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
	conn.Release()

	// synchronization point
	lockJobBackendIDChan <- ourBackendID

	job, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	defer job.Done(ctx)

	deletedJobID := <-deletedJobIDChan

	require.Less(t, deletedJobID, job.ID)
}

func TestJobDelete(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobDelete(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobDelete(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobDelete(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobDelete(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer j.Done(ctx)

	err = j.Delete(ctx)
	require.NoError(t, err)

	conn, err := connPool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		conn.Release()
	}()

	// make sure job was deleted
	j2 := findOneJob(t, conn)
	assert.Nil(t, j2)
}

func TestJobDone(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobDone(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobDone(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobDone(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobDone(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
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
	connFind, err := connPool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		connFind.Release()
	}()

	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype = $1 AND objid= $2::bigint"
	err = connFind.QueryRow(ctx, query, "advisory", j.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// make sure conn was returned to pool
	stat := c.pool.Stat()
	total, available := stat.CurrentConnections, stat.AvailableConnections
	// one connection should be acquired for find
	assert.Equal(t, available, total-1)
}

func TestJobDoneMultiple(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobDoneMultiple(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobDoneMultiple(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobDoneMultiple(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobDoneMultiple(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
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
	t.Run("pgx/v3", func(t *testing.T) {
		testJobDeleteFromTx(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobDeleteFromTx(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobDeleteFromTx(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobDeleteFromTx(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
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

	connFind, err := connPool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		connFind.Release()
	}()

	// make sure the job is gone
	j2 := findOneJob(t, connFind)
	assert.Nil(t, j2)
}

func TestJobDeleteFromTxRollback(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobDeleteFromTxRollback(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobDeleteFromTxRollback(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobDeleteFromTxRollback(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobDeleteFromTxRollback(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
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

	connFind, err := connPool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		connFind.Release()
	}()

	// make sure the job still exists and matches j1
	j2 := findOneJob(t, connFind)
	require.NotNil(t, j2)

	assert.Equal(t, j1.ID, j2.ID)
}

func TestJobError(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobError(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobError(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobError(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobError(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
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

	connFind, err := connPool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		connFind.Release()
	}()

	// make sure job was not deleted
	j2 := findOneJob(t, connFind)
	require.NotNil(t, j2)
	defer j2.Done(ctx)

	assert.NotEqual(t, pgtype.Null, j2.LastError.Status)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	err = connFind.QueryRow(ctx, query, "advisory", j.ID).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, int64(0), count, "advisory lock was not released")

	// make sure conn was returned to pool
	stat := c.pool.Stat()
	total, available := stat.CurrentConnections, stat.AvailableConnections
	// one connection should be acquired for find
	assert.Equal(t, available, total-1)
}
