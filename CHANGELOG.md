# Changelog

## v4

### Breaking

- min supported go version is `1.18`
- min tested Postgres version is `10.x`
- `pgx v3` adapter is gone as it is pretty old already
- previously deprecated `Worker.Start()` and `WorkerPool.Start()` removed in favour of `Worker.Run()`
  and `WorkerPool.Run()` - please check documentation as they are slightly different

### New

- `pgx v5` adapter support
