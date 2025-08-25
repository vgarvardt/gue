package gue

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrRescheduleJobIn(t *testing.T) {
	for name, openFunc := range allAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testErrRescheduleJobIn(t, openFunc(t))
		})
	}
}

func testErrRescheduleJobIn(t *testing.T, connPool *sql.DB) {
	t.Helper()

	ctx := t.Context()
	now := time.Now()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	j := Job{RunAt: now, Type: "foo"}
	err = c.Enqueue(ctx, &j)
	require.NoError(t, err)
	require.NotEmpty(t, j.ID)

	jLocked1, err := c.LockJobByID(ctx, j.ID)
	require.NoError(t, err)

	errReschedule := ErrRescheduleJobIn(10*time.Second, "reschedule me for later time")
	errRescheduleStr := `rescheduling job in "10s" because "reschedule me for later time"`
	assert.Equal(t, errRescheduleStr, errReschedule.Error())

	err = jLocked1.Error(ctx, errReschedule)
	require.NoError(t, err)

	jLocked2, err := c.LockJobByID(ctx, j.ID)
	require.NoError(t, err)

	assert.Equal(t, int32(1), jLocked2.ErrorCount)
	assert.GreaterOrEqual(t, jLocked2.RunAt.Sub(jLocked1.RunAt), 10*time.Second)
	assert.NotNil(t, jLocked2.LastError)
	assert.Equal(t, errRescheduleStr, *jLocked2.LastError)

	err = jLocked2.Done(ctx)
	require.NoError(t, err)
}

func TestErrRescheduleJobAt(t *testing.T) {
	for name, openFunc := range allAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testErrRescheduleJobAt(t, openFunc(t))
		})
	}
}

func testErrRescheduleJobAt(t *testing.T, connPool *sql.DB) {
	t.Helper()

	ctx := t.Context()
	now := time.Now()
	rescheduleAt := now.Add(3 * time.Hour)

	c, err := NewClient(connPool)
	require.NoError(t, err)

	j := Job{RunAt: now, Type: "foo"}
	err = c.Enqueue(ctx, &j)
	require.NoError(t, err)
	require.NotEmpty(t, j.ID)

	jLocked1, err := c.LockJobByID(ctx, j.ID)
	require.NoError(t, err)

	errReschedule := ErrRescheduleJobAt(rescheduleAt, "reschedule me for later time")
	errRescheduleStr := fmt.Sprintf(`rescheduling job at "%s" because "reschedule me for later time"`, rescheduleAt.String())
	assert.Equal(t, errRescheduleStr, errReschedule.Error())

	err = jLocked1.Error(ctx, errReschedule)
	require.NoError(t, err)

	jLocked2, err := c.LockJobByID(ctx, j.ID)
	require.NoError(t, err)

	assert.Equal(t, int32(1), jLocked2.ErrorCount)
	assert.True(t, jLocked2.RunAt.Round(time.Second).Equal(rescheduleAt.Round(time.Second)))
	require.NotNil(t, jLocked2.LastError)
	assert.Equal(t, errRescheduleStr, *jLocked2.LastError)

	err = jLocked2.Done(ctx)
	require.NoError(t, err)
}

func TestErrDiscardJob(t *testing.T) {
	for name, openFunc := range allAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testErrDiscardJob(t, openFunc(t))
		})
	}
}

func testErrDiscardJob(t *testing.T, connPool *sql.DB) {
	t.Helper()

	ctx := t.Context()
	now := time.Now()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	j := Job{RunAt: now, Type: "foo"}
	err = c.Enqueue(ctx, &j)
	require.NoError(t, err)
	require.NotEmpty(t, j.ID)

	jLocked1, err := c.LockJobByID(ctx, j.ID)
	require.NoError(t, err)

	errReschedule := ErrDiscardJob("no job - no fear of being fired")
	errRescheduleStr := `discarding job because "no job - no fear of being fired"`
	assert.Equal(t, errRescheduleStr, errReschedule.Error())

	err = jLocked1.Error(ctx, errReschedule)
	require.NoError(t, err)

	jLocked2, err := c.LockJobByID(ctx, j.ID)
	require.Error(t, err)
	assert.Nil(t, jLocked2)
}
