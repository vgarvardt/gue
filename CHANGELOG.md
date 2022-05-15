# Changelog

## v4

### Breaking

- min supported go version is `1.18`
- min tested Postgres version is `10.x`
- `pgx v3` adapter is gone as it is pretty old already
- `go-pg/pg/v10` adapter is gone as it is in the maintenance mode already
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
