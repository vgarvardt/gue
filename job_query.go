package gue

import (
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	sqlSelectFrom           = "SELECT job_id, queue, priority, run_at, job_type, args, error_count, last_error, created_at FROM gue_jobs"
	sqlLimitLock            = "LIMIT 1 FOR UPDATE SKIP LOCKED"
	sqlOrderByPriority      = "ORDER BY priority ASC"
	sqlOrderByRunAtPriority = "ORDER BY run_at, priority ASC"
)

func newLockJobQuery(queue string, jobTypes []string) (string, []any) {
	whereClause, args := buildWhereClause(queue, jobTypes)
	sql := fmt.Sprintf(`%s %s %s %s`, sqlSelectFrom, whereClause, sqlOrderByPriority, sqlLimitLock)
	return sql, args
}

func newLockByIDQuery(id ulid.ULID) (string, []any) {
	return fmt.Sprintf(`%s WHERE job_id = $1 %s`, sqlSelectFrom, sqlLimitLock), []any{id}
}

func newLockNextScheduledJobQuery(queue string, jobTypes []string) (string, []any) {
	whereClause, args := buildWhereClause(queue, jobTypes)
	sql := fmt.Sprintf(`%s %s %s %s`, sqlSelectFrom, whereClause, sqlOrderByRunAtPriority, sqlLimitLock)
	return sql, args
}

func buildWhereClause(queue string, jobTypes []string) (string, []any) {
	whereClause := `WHERE queue = $1 AND run_at <= $2`
	args := []interface{}{queue, time.Now().UTC()}
	if len(jobTypes) > 0 {
		whereClause += " AND job_type = ANY ($3::TEXT[])"
		args = append(args, jobTypes)
	}
	return whereClause, args
}
