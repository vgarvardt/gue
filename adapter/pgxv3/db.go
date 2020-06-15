package pgxv3

import (
	"context"

	"github.com/jackc/pgx"

	"github.com/vgarvardt/gue/adapter"
)

// Row implements adapter.Row using github.com/jackc/pgx/v3
type Row struct {
	row *pgx.Row
}

// Scan implements adapter.Row.Scan() using github.com/jackc/pgx/v3
func (r *Row) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	if err == pgx.ErrNoRows {
		return adapter.ErrNoRows
	}

	return err
}

// CommandTag implements adapter.CommandTag using github.com/jackc/pgx/v3
type CommandTag struct {
	ct pgx.CommandTag
}

// RowsAffected implements adapter.CommandTag.RowsAffected() using github.com/jackc/pgx/v3
func (ct CommandTag) RowsAffected() int64 {
	return ct.ct.RowsAffected()
}

// Tx implements adapter.Tx using github.com/jackc/pgx/v3
type Tx struct {
	tx *pgx.Tx
}

// Exec implements adapter.Tx.Exec() using github.com/jackc/pgx/v3
func (tx *Tx) Exec(ctx context.Context, sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := tx.tx.ExecEx(ctx, sql, nil, arguments...)
	return CommandTag{ct}, err
}

// QueryRow implements adapter.Tx.QueryRow() using github.com/jackc/pgx/v3
func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	return &Row{tx.tx.QueryRowEx(ctx, sql, nil, args...)}
}

// Rollback implements adapter.Tx.Rollback() using github.com/jackc/pgx/v3
func (tx *Tx) Rollback(ctx context.Context) error {
	err := tx.tx.RollbackEx(ctx)
	if err == pgx.ErrTxClosed {
		return adapter.ErrTxClosed
	}

	return err
}

// Commit implements adapter.Tx.Commit() using github.com/jackc/pgx/v3
func (tx *Tx) Commit(ctx context.Context) error {
	return tx.tx.CommitEx(ctx)
}

// Conn implements adapter.Conn using github.com/jackc/pgx/v3
type Conn struct {
	conn *pgx.Conn
}

// Exec implements adapter.Conn.Exec() using github.com/jackc/pgx/v3
func (c *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := c.conn.ExecEx(ctx, sql, nil, arguments...)
	return CommandTag{ct}, err
}

// QueryRow implements adapter.Conn.QueryRow() using github.com/jackc/pgx/v3
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	return &Row{c.conn.QueryRowEx(ctx, sql, nil, args...)}
}

// Begin implements adapter.Conn.Begin() using github.com/jackc/pgx/v3
func (c *Conn) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.conn.BeginEx(ctx, nil)
	return &Tx{tx}, err
}

// Close implements adapter.Conn.Close() using github.com/jackc/pgx/v3
func (c *Conn) Close(ctx context.Context) error {
	return c.conn.Close()
}

// ConnPool implements adapter.ConnPool using github.com/jackc/pgx/v3
type ConnPool struct {
	pool *pgx.ConnPool
}

// NewConnPool instantiates new adapter.ConnPool using github.com/jackc/pgx/v3
func NewConnPool(pool *pgx.ConnPool) adapter.ConnPool {
	return &ConnPool{pool}
}

// Begin implements adapter.ConnPool.Begin() using github.com/jackc/pgx/v3
func (c *ConnPool) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.pool.BeginEx(ctx, nil)
	return &Tx{tx}, err
}

// Acquire implements adapter.ConnPool.Acquire() using github.com/jackc/pgx/v3
func (c *ConnPool) Acquire(ctx context.Context) (adapter.Conn, error) {
	conn, err := c.pool.AcquireEx(ctx)
	return &Conn{conn}, err
}

// Release implements adapter.ConnPool.Release() using github.com/jackc/pgx/v3
func (c *ConnPool) Release(conn adapter.Conn) {
	c.pool.Release(conn.(*Conn).conn)
}

// Stat implements adapter.ConnPool.Stat() using github.com/jackc/pgx/v3
func (c *ConnPool) Stat() adapter.ConnPoolStat {
	s := c.pool.Stat()
	return adapter.ConnPoolStat{
		MaxConnections:       s.MaxConnections,
		CurrentConnections:   s.CurrentConnections,
		AvailableConnections: s.AvailableConnections,
	}
}

// Close implements adapter.ConnPool.Close() using github.com/jackc/pgx/v3
func (c *ConnPool) Close() error {
	c.pool.Close()
	return nil
}
