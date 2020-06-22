package pgxv4

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/vgarvardt/gue/adapter"
)

// aRow implements adapter.Row using github.com/jackc/pgx/v4
type aRow struct {
	row pgx.Row
}

// Scan implements adapter.Row.Scan() using github.com/jackc/pgx/v4
func (r *aRow) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	if err == pgx.ErrNoRows {
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

// aTx implements adapter.Tx using github.com/jackc/pgx/v4
type aTx struct {
	tx pgx.Tx
}

// NewTx instantiates new adapter.Tx using github.com/jackc/pgx/v4
func NewTx(tx pgx.Tx) adapter.Tx {
	return &aTx{tx: tx}
}

// Exec implements adapter.Tx.Exec() using github.com/jackc/pgx/v4
func (tx *aTx) Exec(ctx context.Context, sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := tx.tx.Exec(ctx, sql, arguments...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.Tx.QueryRow() using github.com/jackc/pgx/v4
func (tx *aTx) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	return &aRow{tx.tx.QueryRow(ctx, sql, args...)}
}

// Rollback implements adapter.Tx.Rollback() using github.com/jackc/pgx/v4
func (tx *aTx) Rollback(ctx context.Context) error {
	err := tx.tx.Rollback(ctx)
	if err == pgx.ErrTxClosed {
		return adapter.ErrTxClosed
	}

	return err
}

// Commit implements adapter.Tx.Commit() using github.com/jackc/pgx/v4
func (tx *aTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

// aConn implements adapter.Conn using github.com/jackc/pgx/v4,
// used for wrapping pool connection.
type aConn struct {
	conn *pgxpool.Conn
}

// Exec implements adapter.Conn.Exec() using github.com/jackc/pgx/v4
func (c *aConn) Exec(ctx context.Context, sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := c.conn.Exec(ctx, sql, arguments...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.Conn.QueryRow() using github.com/jackc/pgx/v4
func (c *aConn) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	return &aRow{c.conn.QueryRow(ctx, sql, args...)}
}

// Begin implements adapter.Conn.Begin() using github.com/jackc/pgx/v4
func (c *aConn) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.conn.Begin(ctx)
	return NewTx(tx), err
}

// Release implements adapter.Conn.Release() using github.com/jackc/pgx/v4
func (c *aConn) Release() {
	c.conn.Release()
}

// connPool implements adapter.ConnPool using github.com/jackc/pgx/v4
type connPool struct {
	pool *pgxpool.Pool
}

// NewConnPool instantiates new adapter.ConnPool using github.com/jackc/pgx/v4
func NewConnPool(pool *pgxpool.Pool) adapter.ConnPool {
	return &connPool{pool}
}

// Acquire implements adapter.ConnPool.Acquire() using github.com/jackc/pgx/v4
func (c *connPool) Acquire(ctx context.Context) (adapter.Conn, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	for name, sql := range adapter.PreparedStatements {
		if _, err := conn.Conn().Prepare(ctx, name, sql); err != nil {
			return nil, err
		}
	}

	return &aConn{conn}, err
}

// Stat implements adapter.ConnPool.Stat() using github.com/jackc/pgx/v4
func (c *connPool) Stat() adapter.ConnPoolStat {
	s := c.pool.Stat()
	return adapter.ConnPoolStat{
		MaxConnections:       int(s.MaxConns()),
		CurrentConnections:   int(s.TotalConns()),
		AvailableConnections: int(s.IdleConns()),
	}
}

// Close implements adapter.ConnPool.Close() using github.com/jackc/pgx/v4
func (c *connPool) Close() error {
	c.pool.Close()
	return nil
}
