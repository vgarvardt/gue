package pgxv4

import (
	"context"

	"github.com/jackc/pgx/v4"

	"github.com/vgarvardt/gue/adapter"
)

// PrepareStatements prepares the required statements to run que on the provided
// *pgx.Conn. Typically it is used as an AfterConnect func for a
// *pgxpool.Pool. Every connection used by Gue must have the statements prepared
// ahead of time.
func PrepareStatements(ctx context.Context, conn *pgx.Conn) error {
	for name, sql := range adapter.PreparedStatements {
		if _, err := conn.Prepare(ctx, name, sql); err != nil {
			return err
		}
	}

	return nil
}
