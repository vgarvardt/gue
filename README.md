# gue

Gue is Golang queue that uses PostgreSQL's advisory locks.

Originally this project used to a fork of [bgentry/que-go][bgentry/que-go]
but because of some backward-compatibility breaking changes and original library
author not being very responsive for PRs I turned fork into standalone project.

The name Gue is yet another silly word transformation: Queue -> Que, Go + Que -> Gue.
 
## `pgx` PostgreSQL driver

This package uses the [pgx][pgx] Go PostgreSQL driver rather than the more
popular [pq][pq]. Because Que uses session-level advisory locks, we have to hold
the same connection throughout the process of getting a job, working it,
deleting it, and removing the lock.

Pq and the built-in database/sql interfaces do not offer this functionality, so
we'd have to implement our own connection pool. Fortunately, pgx already has a
perfectly usable one built for us. Even better, it offers better performance
than pq due largely to its use of binary encoding.

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
