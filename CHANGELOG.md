# Changelog

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
