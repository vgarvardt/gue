package pgxv4

import (
	"context"
	"errors"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/sadpenguinn/gue/v6/adapter"
)

// aRow implements adapter.Row using github.com/jackc/pgx/v4
type aRow struct {
	row pgx.Row
}

// Scan implements adapter.Row.Scan() using github.com/jackc/pgx/v4
func (r *aRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return adapter.ErrNoRows
	}

	return err
}

// aCommandTag implements adapter.CommandTag using github.com/jackc/pgx/v4
type aCommandTag struct {
	ct pgconn.CommandTag
}

// RowsAffected implements adapter.CommandTag.RowsAffected() using github.com/jackc/pgx/v4
func (ct aCommandTag) RowsAffected() int64 {
	return ct.ct.RowsAffected()
}

// aRows implements adapter.Rows using github.com/jackc/pgx/v4
type aRows struct {
	rows pgx.Rows
}

// Next implements adapter.Rows.Next() using github.com/jackc/pgx/v4
func (r *aRows) Next() bool {
	return r.rows.Next()
}

// Scan implements adapter.Rows.Scan() using github.com/jackc/pgx/v4
func (r *aRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Err implements adapter.Rows.Err() using github.com/jackc/pgx/v4
func (r *aRows) Err() error {
	return r.rows.Err()
}

// aTx implements adapter.Tx using github.com/jackc/pgx/v4
type aTx struct {
	tx pgx.Tx
}

// NewTx instantiates new adapter.Tx using github.com/jackc/pgx/v4
func NewTx(tx pgx.Tx) adapter.Tx {
	return &aTx{tx: tx}
}

// UnwrapTx tries to unwrap driver-specific transaction instance from the interface.
// Returns unwrap success as the second parameter.
func UnwrapTx(tx adapter.Tx) (pgx.Tx, bool) {
	driverTx, ok := tx.(*aTx)
	if !ok {
		return nil, false
	}

	return driverTx.tx, ok
}

// Exec implements adapter.Tx.Exec() using github.com/jackc/pgx/v4
func (tx *aTx) Exec(ctx context.Context, query string, args ...any) (adapter.CommandTag, error) {
	ct, err := tx.tx.Exec(ctx, query, args...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.Tx.QueryRow() using github.com/jackc/pgx/v4
func (tx *aTx) QueryRow(ctx context.Context, query string, args ...any) adapter.Row {
	return &aRow{tx.tx.QueryRow(ctx, query, args...)}
}

// Query implements adapter.Tx.Query() using github.com/jackc/pgx/v4
func (tx *aTx) Query(ctx context.Context, query string, args ...any) (adapter.Rows, error) {
	rows, err := tx.tx.Query(ctx, query, args...)
	return &aRows{rows}, err
}

// Rollback implements adapter.Tx.Rollback() using github.com/jackc/pgx/v4
func (tx *aTx) Rollback(ctx context.Context) error {
	err := tx.tx.Rollback(ctx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return adapter.ErrTxClosed
	}

	return err
}

// Commit implements adapter.Tx.Commit() using github.com/jackc/pgx/v4
func (tx *aTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

type conn struct {
	c *pgxpool.Conn
}

// NewConn instantiates new adapter.Conn using github.com/jackc/pgx/v4
func NewConn(c *pgxpool.Conn) adapter.Conn {
	return &conn{c}
}

// Ping implements adapter.Conn.Ping() using github.com/jackc/pgx/v4
func (c *conn) Ping(ctx context.Context) error {
	return c.c.Ping(ctx)
}

// Begin implements adapter.Conn.Begin() using github.com/jackc/pgx/v4
func (c *conn) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.c.Begin(ctx)
	return NewTx(tx), err
}

// Exec implements adapter.Conn.Exec() using github.com/jackc/pgx/v4
func (c *conn) Exec(ctx context.Context, query string, args ...any) (adapter.CommandTag, error) {
	r, err := c.c.Exec(ctx, query, args...)
	return aCommandTag{r}, err
}

// QueryRow implements adapter.Conn.QueryRow() github.com/jackc/pgx/v4
func (c *conn) QueryRow(ctx context.Context, query string, args ...any) adapter.Row {
	return &aRow{c.c.QueryRow(ctx, query, args...)}
}

// Query implements adapter.Conn.Query() github.com/jackc/pgx/v4
func (c *conn) Query(ctx context.Context, query string, args ...any) (adapter.Rows, error) {
	rows, err := c.c.Query(ctx, query, args...)
	return &aRows{rows}, err
}

// Release implements adapter.Conn.Release() using github.com/jackc/pgx/v4
func (c *conn) Release() error {
	c.c.Release()
	return nil
}

// connPool implements adapter.ConnPool using github.com/jackc/pgx/v4
type connPool struct {
	pool *pgxpool.Pool
}

// NewConnPool instantiates new adapter.ConnPool using github.com/jackc/pgx/v4
func NewConnPool(pool *pgxpool.Pool) adapter.ConnPool {
	return &connPool{pool}
}

// Ping implements adapter.ConnPool.Ping() using github.com/jackc/pgx/v4
func (c *connPool) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

// Begin implements adapter.ConnPool.Begin() using github.com/jackc/pgx/v4
func (c *connPool) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.pool.Begin(ctx)
	return NewTx(tx), err
}

// Exec implements adapter.ConnPool.Exec() using github.com/jackc/pgx/v4
func (c *connPool) Exec(ctx context.Context, query string, args ...any) (adapter.CommandTag, error) {
	ct, err := c.pool.Exec(ctx, query, args...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.ConnPool.QueryRow() using github.com/jackc/pgx/v4
func (c *connPool) QueryRow(ctx context.Context, query string, args ...any) adapter.Row {
	return &aRow{c.pool.QueryRow(ctx, query, args...)}
}

// Query implements adapter.ConnPool.Query() using github.com/jackc/pgx/v4
func (c *connPool) Query(ctx context.Context, query string, args ...any) (adapter.Rows, error) {
	rows, err := c.pool.Query(ctx, query, args...)
	return &aRows{rows}, err
}

// Acquire implements adapter.ConnPool.Acquire() using github.com/jackc/pgx/v4
func (c *connPool) Acquire(ctx context.Context) (adapter.Conn, error) {
	cc, err := c.pool.Acquire(ctx)
	return NewConn(cc), err
}

// Close implements adapter.ConnPool.Close() using github.com/jackc/pgx/v4
func (c *connPool) Close() error {
	c.pool.Close()
	return nil
}
