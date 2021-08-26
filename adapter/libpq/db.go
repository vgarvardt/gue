package libpq

import (
	"context"
	"database/sql"

	"github.com/vgarvardt/gue/v3/adapter"
)

// aRow implements adapter.Row using github.com/lib/pq
type aRow struct {
	row *sql.Row
}

// Scan implements adapter.Row.Scan() using github.com/lib/pq
func (r *aRow) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	if err == sql.ErrNoRows {
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

// Tx implements adapter.Tx using github.com/lib/pq
type Tx struct {
	tx *sql.Tx
}

// NewTx instantiates new adapter.Tx github.com/lib/pq
func NewTx(tx *sql.Tx) adapter.Tx {
	return &Tx{tx: tx}
}

// Exec implements adapter.Tx.Exec() using github.com/lib/pq
func (tx *Tx) Exec(ctx context.Context, sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := tx.tx.ExecContext(ctx, sql, arguments...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.Tx.QueryRow() using github.com/lib/pq
func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	return &aRow{tx.tx.QueryRowContext(ctx, sql, args...)}
}

// Rollback implements adapter.Tx.Rollback() using github.com/lib/pq
func (tx *Tx) Rollback(ctx context.Context) error {
	err := tx.tx.Rollback()
	if err == sql.ErrTxDone {
		return adapter.ErrTxClosed
	}

	return err
}

// Commit implements adapter.Tx.Commit() using github.com/lib/pq
func (tx *Tx) Commit(ctx context.Context) error {
	return tx.tx.Commit()
}

// connPool implements adapter.ConnPool using github.com/lib/pq
type connPool struct {
	pool *sql.DB
}

// NewConnPool instantiates new adapter.ConnPool using github.com/lib/pq
func NewConnPool(pool *sql.DB) adapter.ConnPool {
	return &connPool{pool}
}

// Exec implements adapter.ConnPool.Exec() using github.com/lib/pq
func (c *connPool) Exec(ctx context.Context, sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := c.pool.ExecContext(ctx, sql, arguments...)
	return aCommandTag{ct}, err
}

// QueryRow implements adapter.ConnPool.QueryRow() using github.com/lib/pq
func (c *connPool) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	return &aRow{c.pool.QueryRowContext(ctx, sql, args...)}
}

// Begin implements adapter.ConnPool.Begin() using github.com/lib/pq
func (c *connPool) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.pool.BeginTx(ctx, nil)
	return NewTx(tx), err
}

// Close implements adapter.ConnPool.Close() using github.com/lib/pq
func (c *connPool) Close() error {
	return c.pool.Close()
}
