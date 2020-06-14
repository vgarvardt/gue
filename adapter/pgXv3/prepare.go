package pgXv3

import (
	"github.com/jackc/pgx"

	"github.com/vgarvardt/gue/adapter"
)

// PrepareStatements prepares the required statements to run que on the provided
// *pgx.Conn. Typically it is used as an AfterConnect func for a
// *pgx.ConnPool. Every connection used by que must have the statements prepared
// ahead of time.
func PrepareStatements(conn *pgx.Conn) error {
	for name, sql := range adapter.PreparedStatements {
		if _, err := conn.Prepare(name, sql); err != nil {
			return err
		}
	}

	return nil
}
