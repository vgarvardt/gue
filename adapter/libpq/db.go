package libpq

import (
	"context"
	"database/sql"
	"errors"

	"github.com/sadpenguinn/gue/v6/adapter"
)

// aRow implements adapter.Row using github.com/lib/pq
type aRow struct {
	row *sql.Row
}

// Scan implements adapter.Row.Scan() using github.com/lib/pq
func (r *aRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if errors.Is(err, sql.ErrNoRows) {
		return adapter.ErrNoRows
	}

	return err
}

// aCommandTag implements adapter.CommandTag using github.com/lib/pq
type aCommandTag struct {
	ct sql.Result
}

// RowsAffected implements adapter.CommandTag.RowsAffected() using github.com/lib/pq
func (ct aCommandTag) RowsAffected() int64 {
	ra, err := ct.ct.RowsAffected()
	if err != nil {
		// TODO: log this error at least
		return 0
	}

	return ra
}

// aRows implements adapter.Rows using github.com/lib/pq
type aRows struct {
	rows *sql.Rows
}

// Next implements adapter.Rows.Next() using github.com/lib/pq
func (r *aRows) Next() bool {
	return r.rows.Next()
}

// Scan implements adapter.Rows.Scan() using github.com/lib/pq
func (r *aRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Err implements adapter.Rows.Err() using github.com/lib/pq
func (r *aRows) Err() error {
	return r.rows.Err()
}

// aTx implements adapter.Tx using github.com/lib/pq
type aTx struct {
	tx *sql.Tx
}

// NewTx instantiates new adapter.Tx using github.com/lib/pq
func NewTx(tx *sql.Tx) adapter.Tx {
	return &aTx{tx: tx}
}

// UnwrapTx tries to unwrap driver-specific transaction instance from the interface.
// Returns unwrap success as the second parameter.
func UnwrapTx(tx adapter.Tx) (*sql.Tx, bool) {
	driverTx, ok := tx.(*aTx)
	if !ok {
		return nil, false
	}

	return driverTx.tx, ok
}

// Exec implements adapter.Tx.Exec() using github.com/lib/pq
func (tx *aTx) Exec(ctx context.Context, query string, args ...any) (adapter.CommandTag, error) {
	ct, err := tx.tx.ExecContext(ctx, query, args...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.Tx.QueryRow() using github.com/lib/pq
func (tx *aTx) QueryRow(ctx context.Context, query string, args ...any) adapter.Row {
	return &aRow{tx.tx.QueryRowContext(ctx, query, args...)}
}

// Query implements adapter.Tx.Query() using github.com/lib/pq
func (tx *aTx) Query(ctx context.Context, query string, args ...any) (adapter.Rows, error) {
	rows, err := tx.tx.QueryContext(ctx, query, args...)
	return &aRows{rows}, err
}

// Rollback implements adapter.Tx.Rollback() using github.com/lib/pq
func (tx *aTx) Rollback(_ context.Context) error {
	err := tx.tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return adapter.ErrTxClosed
	}

	return err
}

// Commit implements adapter.Tx.Commit() using github.com/lib/pq
func (tx *aTx) Commit(_ context.Context) error {
	return tx.tx.Commit()
}

type conn struct {
	c *sql.Conn
}

// NewConn instantiates new adapter.Conn using github.com/lib/pq
func NewConn(c *sql.Conn) adapter.Conn {
	return &conn{c}
}

// Ping implements adapter.Conn.Ping() using github.com/lib/pq
func (c *conn) Ping(ctx context.Context) error {
	return c.c.PingContext(ctx)
}

// Begin implements adapter.Conn.Begin() using github.com/lib/pq
func (c *conn) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.c.BeginTx(ctx, nil)
	return NewTx(tx), err
}

// Exec implements adapter.Conn.Exec() using github.com/lib/pq
func (c *conn) Exec(ctx context.Context, query string, args ...any) (adapter.CommandTag, error) {
	r, err := c.c.ExecContext(ctx, query, args...)
	return aCommandTag{r}, err
}

// QueryRow implements adapter.Conn.QueryRow() github.com/lib/pq
func (c *conn) QueryRow(ctx context.Context, query string, args ...any) adapter.Row {
	return &aRow{c.c.QueryRowContext(ctx, query, args...)}
}

// Query implements adapter.Conn.Query() github.com/lib/pq
func (c *conn) Query(ctx context.Context, query string, args ...any) (adapter.Rows, error) {
	rows, err := c.c.QueryContext(ctx, query, args...)
	return &aRows{rows}, err
}

// Release implements adapter.Conn.Release() using github.com/lib/pq
func (c *conn) Release() error {
	return c.c.Close()
}

// connPool implements adapter.ConnPool using github.com/lib/pq
type connPool struct {
	pool *sql.DB
}

// NewConnPool instantiates new adapter.ConnPool using github.com/lib/pq
func NewConnPool(pool *sql.DB) adapter.ConnPool {
	return &connPool{pool}
}

// Ping implements adapter.ConnPool.Ping() using github.com/lib/pq
func (c *connPool) Ping(ctx context.Context) error {
	return c.pool.PingContext(ctx)
}

// Exec implements adapter.ConnPool.Exec() using github.com/lib/pq
func (c *connPool) Exec(ctx context.Context, query string, args ...any) (adapter.CommandTag, error) {
	ct, err := c.pool.ExecContext(ctx, query, args...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.ConnPool.QueryRow() using github.com/lib/pq
func (c *connPool) QueryRow(ctx context.Context, query string, args ...any) adapter.Row {
	return &aRow{c.pool.QueryRowContext(ctx, query, args...)}
}

// Query implements adapter.ConnPool.Query() using github.com/lib/pq
func (c *connPool) Query(ctx context.Context, query string, args ...any) (adapter.Rows, error) {
	rows, err := c.pool.QueryContext(ctx, query, args...)
	return &aRows{rows}, err
}

// Begin implements adapter.ConnPool.Begin() using github.com/lib/pq
func (c *connPool) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.pool.BeginTx(ctx, nil)
	return NewTx(tx), err
}

// Acquire implements adapter.ConnPool.Acquire() using github.com/lib/pq
func (c *connPool) Acquire(ctx context.Context) (adapter.Conn, error) {
	cc, err := c.pool.Conn(ctx)
	return NewConn(cc), err
}

// Close implements adapter.ConnPool.Close() using github.com/lib/pq
func (c *connPool) Close() error {
	return c.pool.Close()
}
