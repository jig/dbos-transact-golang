# CLAUDE.md

Project memory for working on this fork of the **DBOS Transact Go SDK**
(`github.com/dbos-inc/dbos-transact-golang`, package `dbos`).

## Current task

Implement **transactional steps for Postgres** ("Increment A"): a public API that
lets user code run database writes inside the *same* transaction as the DBOS step
checkpoint, so the user's changes and the DBOS workflow transition commit atomically
(exactly-once).

This corresponds to upstream issue
[#336 "Datasources/Transactional Steps"](https://github.com/dbos-inc/dbos-transact-golang/issues/336).

### Scope (do this)

- Postgres only.
- Application tables that live in the **same database** as the DBOS system schema
  (i.e. reachable through the system DB connection pool).
- A thin, idiomatic public wrapper over machinery that **already exists internally**.

### Out of scope (do NOT build)

- Separate application datasource / a different DB or server from the system DB.
- Per-driver datasource plugin packages (the TypeScript model).
- SQLite or CockroachDB support for this feature.
- Any change to the existing `runAsTxn` behaviour beyond what's needed to expose it.

These belong to a later "Increment B" and need maintainer design alignment first.

## What already exists (don't reinvent it)

The hard part is already built and just needs a public surface. Line numbers below are
approximate and will drift with new commits — the symbol names are the durable anchor;
`grep` for them rather than trusting the `~line` hints.

- **`dbos/workflow.go`**
  - `runAsTxn[R any](ctx DBOSContext, fn txn[R], opts ...StepOption) (R, error)`
    (~line 1787) — generic wrapper: nil checks, derives step name via reflection,
    type-erases, calls the method below, converts the typed result. Mirror of the
    public `RunAsStep[R]` (~line 1685).
  - `(c *dbosContext) runAsTxn(_ DBOSContext, fn txnFunc, opts ...StepOption) (any, error)`
    (~line 1817) — the engine. It: begins a tx on the **system DB pool**
    (`c.systemDB.(*sysDB).pool`), calls `checkOperationExecution` for dedup/replay,
    runs the user fn passing the `tx`, writes the checkpoint via `recordOperationResult`
    **inside that same tx**, commits, and wraps everything in `retryWithResult` for
    serialization-failure retries. Isolation comes from `stepOpts.txIsoLevel`
    (defaults to `IsoLevelReadCommitted`).
  - Private types: `txn[R any] func(ctx context.Context, tx Tx) (R, error)` (~1432),
    `txnFunc func(ctx context.Context, tx Tx) (any, error)` (~1429).
  - `stepOptions` struct has `txIsoLevel *IsoLevel` (~1442). There is **no** exported
    setter for it yet — we add one.
  - Functional-options pattern: `type StepOption func(*stepOptions)`; see
    `WithStepName`, `WithStepMaxRetries`, etc. as templates.

- **`dbos/dbq.go`** — public DB abstraction (all exported):
  - `Tx` (`Querier` + `Commit` + `Rollback`), `Pool`, `TxOptions{IsoLevel, ReadOnly}`.
  - `IsoLevel` with `IsoLevelDefault`, `IsoLevelReadCommitted`, `IsoLevelRepeatableRead`,
    `IsoLevelSerializable`.
  - `Querier`: `Exec`, `Query`, `QueryRow`.

- Driver is **pgx/v5** (`github.com/jackc/pgx/v5`).
- Checkpoints live in the `operation_outputs` table (system schema). The exactly-once
  guarantee = the checkpoint row commits in the same tx as the user's writes.

## What to implement

### 1. `dbos/transaction.go` (new file)

- `type Transaction[R any] func(ctx context.Context, tx Tx) (R, error)` — public type
  with the same underlying signature as the private `txn[R]`.
- `func RunAsTransaction[R any](ctx DBOSContext, fn Transaction[R], opts ...StepOption) (R, error)`
  — delegate to the existing `runAsTxn` (convert `Transaction[R]` to `txn[R]`).
- `func WithTxIsolation(level IsoLevel) StepOption` — sets `opts.txIsoLevel = &level`.

Doc comments must state:
- The user's DB writes and the DBOS step checkpoint commit in a single transaction
  (atomic, exactly-once).
- The user MUST issue their SQL through the provided `tx`, not their own pool.
- Only works for tables in the same database as the DBOS system schema.
- Must be called directly from a workflow. If called nested within another step the
  engine passes a **nil** `tx` (steps are flattened), so don't do that — document it.
- The function MUST be safe to re-run: `runAsTxn` wraps the whole closure in
  `retryWithResult`, so on a serialization failure the user fn is re-executed from
  scratch on a fresh `tx` (possibly several times before a commit succeeds). Side
  effects other than writes through `tx` are not rolled back — keep the body to
  `tx` operations and pure computation.

### 2. `dbos/transaction_test.go` (new file)

- Use the existing `setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})`
  helper to get a `DBOSContext`.
- `skipIfSqlite(t, "transactional steps are Postgres-only")` at the top.
- Create a user table (e.g. `test_txn_data(id text primary key, note text)`) in the
  same database — either via DDL inside the transaction, or a separate `pgxpool`
  built from `getDatabaseURL()`. See `TestCustomPool` for the pgx pool pattern and
  the `goleak` ignore-list it uses.
- Register a workflow that calls `RunAsTransaction` to `INSERT` a row through `tx`
  and return it; `RunWorkflow` + `handle.GetResult()`; assert the row is present
  afterwards (query with a separate connection to prove it committed).
- Bonus if cheap: prove exactly-once by replaying the **same** workflow, not a fresh
  one. Pin the ID with `WithWorkflowID(id)` and call `RunWorkflow` twice with that ID;
  a brand-new `RunWorkflow` gets a new workflow ID and would legitimately insert again,
  so it doesn't test the `checkOperationExecution` dedup. Assert the row exists exactly
  once afterwards.

## Conventions

- Match surrounding style: functional options, generics + type-erasure, errors wrapped
  with `newStepExecutionError(workflowID, stepName, err)` and `fmt.Errorf("...: %w", err)`.
- Keep the new file additive; avoid touching `runAsTxn` internals unless strictly needed
  (e.g. exporting the isolation option, which only adds a setter, not behaviour).
- Run `gofmt`/`goimports`; keep `go vet ./...` clean.

## Build & test

Tests need a real Postgres. Connection comes from `DBOS_SYSTEM_DATABASE_URL`, else
defaults to `postgres://postgres:<PGPASSWORD or "dbos">@localhost:5432/dbos?sslmode=disable`.

```bash
# start a throwaway Postgres if needed
docker run --rm -d --name dbos-pg -e POSTGRES_PASSWORD=dbos -p 5432:5432 postgres:16

go build ./...
go vet ./...
go test ./dbos -run Transaction -v
```

`DBOS_TEST_BACKEND=sqlite` switches the suite to SQLite — our test must skip there.

## Definition of done

- `dbos/transaction.go` and `dbos/transaction_test.go` exist.
- `go build ./...` and `go vet ./...` are clean.
- `go test ./dbos -run Transaction` passes against Postgres.
- Public API: `Transaction[R]`, `RunAsTransaction[R]`, `WithTxIsolation` — documented.

## When done

Push to this fork and open a PR against upstream referencing issue #336. Note in the
PR description that this is the same-database increment and that the multi-datasource
design is deliberately deferred — the maintainers asked to design that interface, so
invite their feedback on the `RunAsTransaction` / `WithTxIsolation` naming.
