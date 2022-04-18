package testing

import (
	"sync"
	"testing"

	"github.com/vgarvardt/gue/v3/adapter"
)

const defaultPoolConns = 5

var migrations sync.Map

func applyMigrations(schema string) *sync.Once {
	once, _ := migrations.LoadOrStore(schema, &sync.Once{})
	return once.(*sync.Once)
}

// OpenTestPool callback type for opening connection pool with default parameters used in tests
type OpenTestPool func(t testing.TB) adapter.ConnPool

// OpenOpenTestPoolMaxConns callback type for opening connection pool with custom max connections used in tests
type OpenOpenTestPoolMaxConns func(t testing.TB, maxConnections int32) adapter.ConnPool

// AllAdaptersOpenTestPool lists all available adapters with callbacks
var AllAdaptersOpenTestPool = map[string]OpenTestPool{
	"pgx/v4":    OpenTestPoolPGXv4,
	"pgx/v5":    OpenTestPoolPGXv5,
	"lib/pq":    OpenTestPoolLibPQ,
	"go-pg/v10": OpenTestPoolGoPGv10,
}
