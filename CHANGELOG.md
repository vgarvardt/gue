# Changelog

## v6

- DB `adapter` is gone together with all corresponding interfaces and wrapper types, stdlib `database/sql` is used
  directly everywhere
- Logger `adapter` is gone together with all corresponding interfaces and wrapper types, stdlib `log/slog` is used
  everywhere, use existing adapters to wrap a logger used in your application, e.g. `go.uber.org/zap/exp/zapslog`
- `WithWorkerGracefulShutdown` family of options superseded by the `WithWorkerContextFactory`
  family of options. To migrate to this version, replace:

  ```go
  WithWorkerGracefulShutdown(yourCtxBuilderFunc)
  ```

  with:

  ```go
  WithWorkerContextFactory(func(_ context.Context) context.Context {
    return yourCtxBuilderFunc()
  })
  ```

  or:

  ```go
    WithWorkerGracefulShutdown(nil)
  ```

  with:

  ```go
  WithWorkerContextFactory(func(_ context.Context) context.Context {
    return context.Background()
  })
  ```

- OpenTelemetry span attribute names are using snake_case instead of kebab-case to follow OTel naming convention, e.g.
  `job-type` -> `job_type`, `job-queue` -> `job_queue`, `job-id` -> `job_id`, etc.
- min supported Golang version is `1.24`

## v5

### Breaking

- `gue_jobs.job_id` column type changed to `TEXT` and the `Job.ID` field type changed from `int64`
  to [`ULID`](https://github.com/oklog/ulid) to generate ID on the client-side but keep jobs sortable by the primary
  key. Library is not providing any migration routines, it is up to the users to apply a migration. Example can be
  found at [migrations/job_id_to_ulid.sql](./migrations/job_id_to_ulid.sql).
- `gue_jobs.args` column type changed to `BYTEA` - this allows storing any bytes as job args, not only valid JSON;
  library is not providing any migration routines, it is up to the users to apply a migration that may look something
  like `ALTER TABLE gue_jobs ALTER COLUMN args TYPE bytea USING (args::text)::bytea` to change the column type and
  convert existing JSON records to the binary byte array representation
- `Job.Error()` accepts `error` instance instead of error string
- `Job.LastError` type changed from `github.com/jackc/pgtype.Text` to stdlib `database/sql.NullString`
- min tested Postgres version is `11.x`

### New

- Handler may return special typed errors to control rescheduling/discarding of the jobs on the individual basis
    - `ErrRescheduleJobIn()` - reschedule Job after some interval from the current time
    - `ErrRescheduleJobAt()` - reschedule Job to some specific time
    - `ErrDiscardJob()` - discard a Job

## v4

### Breaking

- min supported go version is `1.18`
- min tested Postgres version is `10.x`
- `pgx v3` adapter is gone as it is pretty old already
- `go-pg/pg/v10` adapter is gone as it is in the maintenance mode already
- `NewClient()` returns not only client instance but an error if it fails to init
- `NewWorker()` and `NewWorkerPool()` return not only worker and pool instance but an error if fail to init
- previously deprecated `Worker.Start()` and `WorkerPool.Start()` removed in favour of `Worker.Run()`
  and `WorkerPool.Run()` - please check documentation as they are slightly different
- `Job.Priority` changed its type from `int16` to `JobPriority` that is a wrapper type for `int16`
- `gue/adapter/exponential.Default` became `gue.DefaultExponentialBackoff`
- `gue/adapter/exponential.New()` became `gue.NewExponentialBackoff()`

### New

- `pgx v5` adapter support
- const values for `JobPriority` type to simplify usage of the common values:
    - `JobPriorityHighest`
    - `JobPriorityHigh`
    - `JobPriorityDefault` - set by default when the `Job.Priority` is not explicitly set
    - `JobPriorityLow`
    - `JobPriorityLowest`
- `WorkerPool.WorkOne` method, can be useful for testing purpose mostly
- backoff implementation may return negative value to discard errored job immediately
    - `gue.BackoffNever` backoff implementation discards the job on the first error
- [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go) Metrics are available for `Client` -
  use `WithClientMeter()` option to set meter for the client instance. Available metrics:
    - `gue_client_enqueue` - number of job enqueue tries, exposes `job-type` and `success` attributes
    - `gue_client_lock_job` - number of job lock tries, exposes `job-type` and `success` attributes
- [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go) Metrics are available for `Worker` -
  use `WithWorkerMeter()` or `WithPoolMeter()` option to set meter for the worker instance. Available metrics:
    - `gue_worker_jobs_worked` - number of jobs process tries, exposes `job-type` and `success` attributes
    - `gue_worker_jobs_duration` - histogram of jobs processing duration, exposes `job-type` attribute
- [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go) Tracing is available for `Worker` -
  use `WithWorkerTracer()` or `WithPoolTracer()` option to set tracer for the worker instance
- `GetWorkerIdx` function extracts worker index in the pool from the handler context
