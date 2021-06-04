// Package gopgv10 implements github.com/go-pg/pg/v10 adapter.
package gopgv10

import (
	"context"
	"errors"
	"strings"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"

	"github.com/vgarvardt/gue/v2/adapter"
)

// BUG: using Scan actually executes the query. That’s not an issue given
// how Scan is used in gue.

// BUG: github.com/go-pg/pg/v10 does not accept context in Prepare method so
// as a simple workaround we just convert SQL statements to use placeholders.
// The implementation is good enough for SQL statements used in gue but may
// not work in other cases.

func formatSQL(sql string) string {
	// go-pg uses ? instead of $.
	return strings.Replace(sql, "$", "?", -1)
}

func formatArgs(args []interface{}) []interface{} {
	// go-pg starts at 0 instead of 1.
	dst := make([]interface{}, len(args)+1)
	copy(dst[1:], args)
	return dst
}

// aRow implements adapter.Row using github.com/go-pg/pg/v10
type aRow struct {
	ctx  context.Context
	db   orm.DB
	sql  string
	args []interface{}
}

// Scan implements adapter.Row.Scan() using github.com/go-pg/pg/v10
func (r *aRow) Scan(dest ...interface{}) error {
	_, err := r.db.QueryOneContext(r.ctx, pg.Scan(dest...), formatSQL(r.sql), formatArgs(r.args)...)
	if errors.Is(err, pg.ErrNoRows) {
		return adapter.ErrNoRows
	}
	return err
}

// aCommandTag implements adapter.CommandTag using github.com/go-pg/pg/v10
type aCommandTag struct {
	r orm.Result
}

// RowsAffected implements adapter.CommandTag.RowsAffected() using github.com/go-pg/pg/v10
func (ct aCommandTag) RowsAffected() int64 {
	return int64(ct.r.RowsAffected()) // cast from int
}

// aTx implements adapter.Tx using github.com/go-pg/pg/v10
type aTx struct {
	tx *pg.Tx
}

// NewTx instantiates new adapter.Tx using github.com/go-pg/pg/v10
func NewTx(tx *pg.Tx) adapter.Tx {
	return &aTx{tx: tx}
}

// Exec implements adapter.Tx.Exec() using github.com/go-pg/pg/v10
func (tx *aTx) Exec(ctx context.Context, sql string, args ...interface{}) (adapter.CommandTag, error) {
	r, err := tx.tx.ExecContext(ctx, formatSQL(sql), formatArgs(args)...)
	return aCommandTag{r}, err
}

// QueryRow implements adapter.Tx.QueryRow() using github.com/go-pg/pg/v10
func (tx *aTx) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	return &aRow{ctx, tx.tx, sql, args}
}

// Rollback implements adapter.Tx.Rollback() using github.com/go-pg/pg/v10
func (tx *aTx) Rollback(ctx context.Context) error {
	err := tx.tx.RollbackContext(ctx)
	if errors.Is(err, pg.ErrTxDone) {
		return adapter.ErrTxClosed
	}
	return err
}

// Commit implements adapter.Tx.Commit() using github.com/go-pg/pg/v10
func (tx *aTx) Commit(ctx context.Context) error {
	return tx.tx.CommitContext(ctx)
}

type connPool struct {
	db *pg.DB
}

// NewConnPool instantiates new adapter.ConnPool using github.com/go-pg/pg/v10
func NewConnPool(db *pg.DB) adapter.ConnPool {
	return &connPool{db}
}

// Begin implements adapter.ConnPool.Begin() using github.com/go-pg/pg/v10
func (c *connPool) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.db.BeginContext(ctx)
	return NewTx(tx), err
}

// Exec implements adapter.ConnPool.Exec() using github.com/go-pg/pg/v10
func (c *connPool) Exec(ctx context.Context, sql string, args ...interface{}) (adapter.CommandTag, error) {
	r, err := c.db.ExecContext(ctx, formatSQL(sql), formatArgs(args)...)
	return aCommandTag{r}, err
}

// QueryRow implements adapter.ConnPool.QueryRow() using github.com/go-pg/pg/v10
func (c *connPool) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	return &aRow{ctx, c.db, sql, args}
}

// Close implements adapter.ConnPool.Close() using github.com/go-pg/pg/v10
func (c *connPool) Close() error {
	return c.db.Close()
}
