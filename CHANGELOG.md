# Changelog

## v4

### Breaking

- min supported go version is `1.18`
- min tested Postgres version is `10.x`
- `pgx v3` adapter is gone as it is pretty old already
- previously deprecated `Worker.Start()` and `WorkerPool.Start()` removed in favour of `Worker.Run()`
  and `WorkerPool.Run()` - please check documentation as they are slightly different
- `Job.Priority` changed its type from `int16` to `JobPriority` that is a wrapper type for `int16`

### New

- `pgx v5` adapter support
- const values for `JobPriority` type to simplify usage of the common values:
  - `JobPriorityHighest`
  - `JobPriorityHigh`
  - `JobPriorityDefault` - set by default when the `Job.Priority` is not explicitly set
  - `JobPriorityLow`
  - `JobPriorityLowest` 
