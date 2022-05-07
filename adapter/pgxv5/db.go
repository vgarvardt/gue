package pgxv5

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vgarvardt/gue/v4/adapter"
)

// aRow implements adapter.Row using github.com/jackc/pgx/v5
type aRow struct {
	row pgx.Row
}

// Scan implements adapter.Row.Scan() using github.com/jackc/pgx/v5
func (r *aRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if err == pgx.ErrNoRows {
		return adapter.ErrNoRows
	}

	return err
}

// aCommandTag implements adapter.CommandTag using github.com/jackc/pgx/v5
type aCommandTag struct {
	ct pgconn.CommandTag
}

// RowsAffected implements adapter.CommandTag.RowsAffected() using github.com/jackc/pgx/v5
func (ct aCommandTag) RowsAffected() int64 {
	return ct.ct.RowsAffected()
}

// aTx implements adapter.Tx using github.com/jackc/pgx/v5
type aTx struct {
	tx pgx.Tx
}

// NewTx instantiates new adapter.Tx using github.com/jackc/pgx/v5
func NewTx(tx pgx.Tx) adapter.Tx {
	return &aTx{tx: tx}
}

// Exec implements adapter.Tx.Exec() using github.com/jackc/pgx/v5
func (tx *aTx) Exec(ctx context.Context, sql string, args ...any) (adapter.CommandTag, error) {
	ct, err := tx.tx.Exec(ctx, sql, args...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.Tx.QueryRow() using github.com/jackc/pgx/v5
func (tx *aTx) QueryRow(ctx context.Context, sql string, args ...any) adapter.Row {
	return &aRow{tx.tx.QueryRow(ctx, sql, args...)}
}

// Rollback implements adapter.Tx.Rollback() using github.com/jackc/pgx/v5
func (tx *aTx) Rollback(ctx context.Context) error {
	err := tx.tx.Rollback(ctx)
	if err == pgx.ErrTxClosed {
		return adapter.ErrTxClosed
	}

	return err
}

// Commit implements adapter.Tx.Commit() using github.com/jackc/pgx/v5
func (tx *aTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

type conn struct {
	c *pgxpool.Conn
}

// Ping implements adapter.Conn.Ping() using github.com/jackc/pgx/v5
func (c *conn) Ping(ctx context.Context) error {
	return c.c.Ping(ctx)
}

// Begin implements adapter.Conn.Begin() using github.com/jackc/pgx/v5
func (c *conn) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.c.Begin(ctx)
	return NewTx(tx), err
}

// Exec implements adapter.Conn.Exec() using github.com/jackc/pgx/v5
func (c *conn) Exec(ctx context.Context, sql string, args ...any) (adapter.CommandTag, error) {
	r, err := c.c.Exec(ctx, sql, args...)
	return aCommandTag{r}, err
}

// QueryRow implements adapter.Conn.QueryRow() github.com/jackc/pgx/v5
func (c *conn) QueryRow(ctx context.Context, sql string, args ...any) adapter.Row {
	return &aRow{c.c.QueryRow(ctx, sql, args...)}
}

// Release implements adapter.Conn.Release() using github.com/jackc/pgx/v5
func (c *conn) Release() error {
	c.c.Release()
	return nil
}

// connPool implements adapter.ConnPool using github.com/jackc/pgx/v5
type connPool struct {
	pool *pgxpool.Pool
}

// NewConnPool instantiates new adapter.ConnPool using github.com/jackc/pgx/v5
func NewConnPool(pool *pgxpool.Pool) adapter.ConnPool {
	return &connPool{pool}
}

// Ping implements adapter.ConnPool.Ping() using github.com/jackc/pgx/v5
func (c *connPool) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

// Begin implements adapter.ConnPool.Begin() using github.com/jackc/pgx/v5
func (c *connPool) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.pool.Begin(ctx)
	return NewTx(tx), err
}

// Exec implements adapter.ConnPool.Exec() using github.com/jackc/pgx/v5
func (c *connPool) Exec(ctx context.Context, sql string, args ...any) (adapter.CommandTag, error) {
	ct, err := c.pool.Exec(ctx, sql, args...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.ConnPool.QueryRow() using github.com/jackc/pgx/v5
func (c *connPool) QueryRow(ctx context.Context, sql string, args ...any) adapter.Row {
	return &aRow{c.pool.QueryRow(ctx, sql, args...)}
}

// Acquire implements adapter.ConnPool.Acquire() using github.com/jackc/pgx/v5
func (c *connPool) Acquire(ctx context.Context) (adapter.Conn, error) {
	cc, err := c.pool.Acquire(ctx)
	return &conn{cc}, err
}

// Close implements adapter.ConnPool.Close() using github.com/jackc/pgx/v5
func (c *connPool) Close() error {
	c.pool.Close()
	return nil
}
