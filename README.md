# gue

[![GoDoc](https://godoc.org/github.com/vgarvardt/gue?status.svg)](https://godoc.org/github.com/vgarvardt/gue)
[![Coverage Status](https://codecov.io/gh/vgarvardt/gue/branch/master/graph/badge.svg)](https://codecov.io/gh/vgarvardt/gue)
[![ReportCard](https://goreportcard.com/badge/github.com/vgarvardt/gue)](https://goreportcard.com/report/github.com/vgarvardt/gue)
[![License](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)

Gue is Golang queue on top of PostgreSQL.

Originally this project used to be a fork of [bgentry/que-go][bgentry/que-go]
but because of some backward-compatibility breaking changes and original library
author not being very responsive for PRs I turned fork into standalone project.
Internally project maintains backward-compatibility with the original one - DB table
and all the internal logic (queries, algorithms) remained the same.

The name Gue is yet another silly word transformation: Queue -> Que, Go + Que -> Gue.

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

    "github.com/jackc/pgx/v4/pgxpool"

    "github.com/vgarvardt/gue"
    "github.com/vgarvardt/gue/adapter/pgxv4"
)

type printNameArgs struct {
    Name string
}

func main() {
    printName := func(j *gue.Job) error {
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

    pgxPool, err := pgxpool.ConnectConfig(context.Background(), pgxCfg)
    if err != nil {
        log.Fatal(err)
    }
    defer pgxPool.Close()

    poolAdapter := pgxv4.NewConnPool(pgxPool)

    gc := gue.NewClient(poolAdapter)
    wm := gue.WorkMap{
        "PrintName": printName,
    }
    // create a pool w/ 2 workers
    workers := gue.NewWorkerPool(gc, wm, 2, gue.WithPoolQueue("name_printer"))

    ctx, shutdown := context.WithCancel(context.Background())

    // work jobs in goroutine
    if err := workers.Start(ctx); err != nil {
        log.Fatal(err)
    }

    args, err := json.Marshal(printNameArgs{Name: "vgarvardt"})
    if err != nil {
        log.Fatal(err)
    }

    j := &gue.Job{
        Type:  "PrintName",
        Args:  args,
    }
    if err := gc.Enqueue(context.Background(), j); err != nil {
        log.Fatal(err)
    }

    j := &gue.Job{
        Type:  "PrintName",
        RunAt: time.Now().UTC().Add(30 * time.Second), // delay 30 seconds
        Args:  args,
    }
    if err := gc.Enqueue(context.Background(), j); err != nil {
        log.Fatal(err)
    }

    time.Sleep(30 * time.Second) // wait for while

    // send shutdown signal to worker
    shutdown()
}
```
 
## PostgreSQL drivers

Package supports several PostgreSQL drivers using adapter interface internally.
Currently, adapters for the following drivers have been implemented:
- [github.com/jackc/pgx/v3][pgx]
- [github.com/jackc/pgx/v4][pgx]

### `pgx/v3`

```go
package main

import(
    "log"
    "os"

    "github.com/jackc/pgx"

    "github.com/vgarvardt/gue"
    "github.com/vgarvardt/gue/adapter/pgxv3"
)

func main() {
    pgxCfg, err := pgx.ParseURI(os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatal(err)
    }
    pgxPool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
        ConnConfig:   pgxCfg,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer pgxPool.Close()

    poolAdapter := pgxv3.NewConnPool(pgxPool)

    gc := gue.NewClient(poolAdapter)
    ...
}
```

### `pgx/v4`

```go
package main

import(
    "context"
    "log"
    "os"

    "github.com/jackc/pgx/v4/pgxpool"

    "github.com/vgarvardt/gue"
    "github.com/vgarvardt/gue/adapter/pgxv4"
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

    gc := gue.NewClient(poolAdapter)
    ...
}
```

## Logging

Package supports several logging libraries using adapter interface internally.
Currently, adapters for the following drivers have been implemented:
- NoOp (`adapter.NoOpLogger`) - default adapter that does nothing, so it is basically `/dev/null` logger
- Stdlib `log` - adapter that uses [`log`](https://golang.org/pkg/log/) logger for logs output.
  Instantiate it with `adapter.NewStdLogger(...)`.
- Uber `zap` - adapter that uses [`go.uber.org/zap`](https://pkg.go.dev/go.uber.org/zap) logger for logs output.
  Instantiate it with `adapter.zap.New(...)`.

## Testing

Linter and tests are running for every Pul Request, but it is possible to run linter
and tests locally using `docker` and `make`.

Run linter: `make link`. This command runs liner in docker container with the project
source code mounted.

Run tests: `make test`. This command runs project dependencies in docker containers
if they are not started yet and runs go tests with coverage.

[bgentry/que-go]: https://github.com/bgentry/que-go
[pgx]: https://github.com/jackc/pgx
[pq]: https://github.com/lib/pq
