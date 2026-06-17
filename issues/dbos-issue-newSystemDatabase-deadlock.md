# `NewDBOSContext` hangs (deadlock) instead of returning when system-DB init fails

## Summary

When system-database initialization fails on a **non-retryable** error after the
detection connection has been acquired (e.g. `CREATE SCHEMA` is denied),
`newSystemDatabase` calls `pool.Close()` while a connection acquired earlier is
still checked out. `pgxpool.Pool.Close()` then blocks forever in
`sync.WaitGroup.Wait()`, so `NewDBOSContext` never returns — the caller hangs
instead of receiving the error.

## Environment

- Fork: `github.com/jig/dbos-transact-golang` at commit `7fd6112`
  (`feat(dbos): conductor for cloud + cancellation status race fix`),
  `dbos_version=(devel)`, base upstream `v0.17.0`.
- Go 1.26.4, darwin/arm64.
- PostgreSQL 17.
- pgx `v5.10.0`.

## Reproduction

1. Point DBOS at a Postgres database with a role that can **connect** but lacks
   `CREATE` on the database (so `CREATE SCHEMA dbos` is denied), and where the
   `dbos` schema does not yet exist:

   ```go
   _, err := dbos.NewDBOSContext(ctx, dbos.Config{
       AppName:        "demo",
       DatabaseURL:    "postgres://lowpriv:***@127.0.0.1:5432/somedb?sslmode=disable",
       DatabaseSchema: "dbos",
   })
   // expected: err != nil (permission denied)
   // actual:   NewDBOSContext never returns
   ```

2. Observed log (with a debug logger), then the process hangs:

   ```
   level=INFO  msg="Connecting to system database" schema=dbos
   level=DEBUG msg="Non-retryable error encountered"
               error="failed to create schema dbos: ERROR: permission denied for database somedb (SQLSTATE 42501)"
   <hangs forever>
   ```

   `pg_stat_activity` shows the pool's connections **idle** (no query running);
   the deadlock is entirely on the Go side.

## Root cause

In `dbos/system_database.go`, `newSystemDatabase` acquires a connection to detect
the database type and defers its release:

```go
// ~line 784
conn, err := pool.Acquire(ctx)
if err != nil { ... }
defer conn.Release()                 // ~line 791
```

The subsequent error paths call `pool.Close()` **while `conn` is still held**
(the deferred `conn.Release()` only runs when `newSystemDatabase` returns, which
never happens because `Close` blocks first):

```go
// ~line 798-802  (shouldMigrate failed)
if smErr != nil {
    if customPool == nil { pool.Close() }      // blocks: conn still acquired
    return nil, fmt.Errorf("failed to determine migration status: %v", smErr)
}

// ~line 805-813  (runMigrations failed -> e.g. CREATE SCHEMA denied)
if needsMigration {
    if err := retry(ctx, func() error {
        return runMigrations(ctx, pool, databaseSchema, isCockroach, logger)
    }, withRetrierLogger(logger)); err != nil {
        if customPool == nil { pool.Close() }  // blocks: conn still acquired
        return nil, fmt.Errorf("failed to run migrations: %v", err)
    }
}

// ~line 816-820  (ping failed)
if err := pool.Ping(ctx); err != nil {
    if customPool == nil { pool.Close() }      // blocks: conn still acquired
    return nil, fmt.Errorf("failed to ping database: %v", err)
}
```

`pgxpool.(*Pool).Close()` waits for all acquired connections to be released
(`sync.WaitGroup.Wait()`), but the only `Release` for `conn` is the `defer`,
which is scheduled to run *after* the function returns — i.e. after `Close()`.
Deadlock.

Goroutine dump (`GOTRACEBACK=all`, `SIGQUIT`) of the blocked main goroutine:

```
goroutine 1 [sync.WaitGroup.Wait]:
sync.(*WaitGroup).Wait(...)
github.com/jackc/pgx/v5/pgxpool.(*Pool).Close.func1()
        .../pgxpool/pool.go:459
sync.(*Once).Do(...)
github.com/jackc/pgx/v5/pgxpool.(*Pool).Close(...)
        .../pgxpool/pool.go:457
github.com/dbos-inc/dbos-transact-golang/dbos.newSystemDatabase(...)
github.com/dbos-inc/dbos-transact-golang/dbos.NewDBOSContext(...)
        dbos/dbos.go:658
```

## Affected paths

All three post-`Acquire` error branches in `newSystemDatabase` that call
`pool.Close()` while `conn` is still held:

- `shouldMigrate` failure,
- `runMigrations` failure (the common one: insufficient DDL privileges),
- `pool.Ping` failure.

## Suggested fix

Release the detection connection before closing the pool on these error paths,
e.g. replace the bare `defer conn.Release()` with an explicit release prior to
`pool.Close()`, or restructure so the pool is never closed while a connection is
checked out. For example:

```go
conn, err := pool.Acquire(ctx)
if err != nil { ... }
// release explicitly on every exit path *before* pool.Close()
defer conn.Release()
...
if smErr != nil {
    conn.Release()                 // <-- add
    if customPool == nil { pool.Close() }
    return nil, fmt.Errorf("failed to determine migration status: %v", smErr)
}
```

(A double `Release()` is safe with pgxpool — the deferred one becomes a no-op —
but a cleaner structure would acquire/release within a small helper, or close the
pool only after the function has returned the connection.)

## Impact

Any deployment where the configured role lacks `CREATE` on the database (a common
setup when DBOS shares a database owned by another service and uses a restricted
runtime role) turns a clear "permission denied" error into an indefinite hang at
startup, with no error surfaced to the caller. A caller-side
`context.WithTimeout` does not help because `pool.Close()` does not observe the
context.
