# gue

[![GoDev](https://img.shields.io/static/v1?label=godev&message=reference&color=00add8)](https://pkg.go.dev/github.com/sadpenguinn/gue/v6)
[![Coverage Status](https://codecov.io/gh/vgarvardt/gue/branch/master/graph/badge.svg)](https://codecov.io/gh/vgarvardt/gue)
[![ReportCard](https://goreportcard.com/badge/github.com/vgarvardt/gue)](https://goreportcard.com/report/github.com/vgarvardt/gue)
[![License](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)

Gue is Golang queue on top of PostgreSQL that uses transaction-level locks.

Originally this project used to be a fork of [bgentry/que-go](https://github.com/bgentry/que-go)
but because of some backward-compatibility breaking changes and original library author not being very responsive for
PRs I turned fork into standalone project. Version 2 breaks internal backward-compatibility with the original project -
DB table and all the internal logic (queries, algorithms) is completely rewritten.

The name Gue is yet another silly word transformation: Queue -> Que, Go + Que -> Gue.

## Install

```shell
go get -u github.com/sadpenguinn/gue/v6
```

Additionally, you need to apply [DB migration](migrations/schema.sql).

## Usage Example

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/sadpenguinn/gue/v6"
	"github.com/sadpenguinn/gue/v6/adapter/pgxv5"
)

const (
	printerQueue   = "name_printer"
	jobTypePrinter = "PrintName"
)

type printNameArgs struct {
	Name string
}

func main() {
	printName := func(ctx context.Context, j *gue.Job) error {
		var args printNameArgs
		if err := json.Unmarshal(j.Args, &args); err != nil {
			return err
		}
		fmt.Printf("Hello %s!\n", args.Name)
		return nil
	}

	pgxCfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	pgxPool, err := pgxpool.NewWithConfig(context.Background(), pgxCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pgxPool.Close()

	poolAdapter := pgxv5.NewConnPool(pgxPool)

	gc, err := gue.NewClient(poolAdapter)
	if err != nil {
		log.Fatal(err)
	}
	wm := gue.WorkMap{
		jobTypePrinter: printName,
	}

	finishedJobsLog := func(ctx context.Context, j *gue.Job, err error) {
		if err != nil {
			return
		}

		j.Tx().Exec(
			ctx,
			"INSERT INTO finished_jobs_log (queue, type, run_at) VALUES ($1, $2, now())",
			j.Queue,
			j.Type,
		)
	}

	// create a pool w/ 2 workers
	workers, err := gue.NewWorkerPool(gc, wm, 2, gue.WithPoolQueue(printerQueue), gue.WithPoolHooksJobDone(finishedJobsLog))
	if err != nil {
		log.Fatal(err)
	}

	ctx, shutdown := context.WithCancel(context.Background())

	// work jobs in goroutine
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := workers.Run(gctx)
		if err != nil {
			// In a real-world applications, use a better way to shut down
			// application on unrecoverable error. E.g. fx.Shutdowner from
			// go.uber.org/fx module.
			log.Fatal(err)
		}
		return err
	})

	args, err := json.Marshal(printNameArgs{Name: "vgarvardt"})
	if err != nil {
		log.Fatal(err)
	}

	j := &gue.Job{
		Type:  jobTypePrinter,
		Queue: printerQueue,
		Args:  args,
	}
	if err := gc.Enqueue(context.Background(), j); err != nil {
		log.Fatal(err)
	}

	j = &gue.Job{
		Type:  jobTypePrinter,
		Queue: printerQueue,
		RunAt: time.Now().UTC().Add(30 * time.Second), // delay 30 seconds
		Args:  args,
	}
	if err := gc.Enqueue(context.Background(), j); err != nil {
		log.Fatal(err)
	}

	time.Sleep(30 * time.Second) // wait for while

	// send shutdown signal to worker
	shutdown()
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
}

```

## PostgreSQL drivers

Package supports several PostgreSQL drivers using adapter interface internally. Currently, adapters for the following
drivers have been implemented:

- [github.com/jackc/pgx/v5](https://github.com/jackc/pgx)
- [github.com/jackc/pgx/v4](https://github.com/jackc/pgx)
- [github.com/lib/pq](https://github.com/lib/pq)

### `pgx/v5`

```go
package main

import (
  "log"
  "os"

  "github.com/jackc/pgx/v5/pgxpool"

  "github.com/sadpenguinn/gue/v6"
  "github.com/sadpenguinn/gue/v6/adapter/pgxv5"
)

func main() {
  pgxCfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
  if err != nil {
    log.Fatal(err)
  }

  pgxPool, err := pgxpool.NewConfig(context.Background(), pgxCfg)
  if err != nil {
    log.Fatal(err)
  }
  defer pgxPool.Close()

  poolAdapter := pgxv5.NewConnPool(pgxPool)

  gc, err := gue.NewClient(poolAdapter)
  ...
}
```

### `pgx/v4`

```go
package main

import (
  "context"
  "log"
  "os"

  "github.com/jackc/pgx/v4/pgxpool"

  "github.com/sadpenguinn/gue/v6"
  "github.com/sadpenguinn/gue/v6/adapter/pgxv4"
)

func main() {
  pgxCfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
  if err != nil {
    log.Fatal(err)
  }

  pgxPool, err := pgxpool.ConnectConfig(context.Background(), pgxCfg)
  if err != nil {
    log.Fatal(err)
  }
  defer pgxPool.Close()

  poolAdapter := pgxv4.NewConnPool(pgxPool)

  gc, err := gue.NewClient(poolAdapter)
  ...
}
```

### `lib/pq`

```go
package main

import (
  "database/sql"
  "log"
  "os"

  _ "github.com/lib/pq" // register postgres driver

  "github.com/sadpenguinn/gue/v6"
  "github.com/sadpenguinn/gue/v6/adapter/libpq"
)

func main() {
  db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
  if err != nil {
    log.Fatal(err)
  }
  defer db.Close()

  poolAdapter := libpq.NewConnPool(db)

  gc, err := gue.NewClient(poolAdapter)
  ...
}
```

## Logging

Package supports several logging libraries using adapter interface internally. Currently, adapters for the following
drivers have been implemented:

- NoOp (`adapter.NoOpLogger`) - default adapter that does nothing, so it is basically `/dev/null` logger
- Stdlib `log` - adapter that uses [`log`](https://golang.org/pkg/log/) logger for logs output. Instantiate it
  with `adapter.NewStdLogger(...)`.
- Uber `zap` - adapter that uses [`go.uber.org/zap`](https://pkg.go.dev/go.uber.org/zap) logger for logs output.
  Instantiate it with `adapter/zap.New(...)`.
- Olivier Poitrey's `zerolog` - adapter that uses [`github.com/rs/zerolog`](https://pkg.go.dev/github.com/rs/zerolog)
  logger for logs output. Instantiate it with `adapter/zerolog.New(...)`.
- Stdlib `slog` - adapter that uses [`log/slog`](https://pkg.go.dev/log/slog)
  logger for logs output. Instantiate it with `adapter/slog.New(...)`.
