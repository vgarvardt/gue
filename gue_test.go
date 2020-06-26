package gue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/adapter"
)

func findOneJob(t testing.TB, q adapter.Queryable) *Job {
	t.Helper()

	j := new(Job)
	err := q.QueryRow(
		context.Background(),
		`SELECT priority, run_at, job_id, job_type, args, error_count, last_error, queue FROM gue_jobs LIMIT 1`,
	).Scan(
		&j.Priority,
		&j.RunAt,
		&j.ID,
		&j.Type,
		&j.Args,
		&j.ErrorCount,
		&j.LastError,
		&j.Queue,
	)
	if err == adapter.ErrNoRows {
		return nil
	}
	require.NoError(t, err)

	return j
}
