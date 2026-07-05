# Divergences from `dbos-inc/dbos-transact-golang`

This fork carries a set of changes on top of upstream. This document inventories
them so they can be **re-applied as a delta onto a future, more mature upstream**
(a fresh fork) instead of being maintained by perpetual patching of this tree.

It is a re-application checklist, grouped by feature. Each entry gives the files
touched, the change **type**, the rationale, and notes/risks for re-applying.

- **Additive** — new files or new symbols; re-applies cleanly, low conflict risk.
- **Deletion** — removes an upstream feature; conflicts only when upstream edits
  the same area (it did once already, with PL/pgSQL migrations).
- **Hot-rewrite** — inline changes to upstream's busiest functions; this is where
  every upstream change hurts. Minimise / isolate these first when re-forking.

## Baseline & ground rules

- **Upstream baseline (this re-fork):** `91f7bb0` — "Use listen/notify for streams
  (#372)", the point this `refork` branch starts from. It already includes
  datasources (#358), preemptible sleep (#366), child cancellation (#359), etc.
- **Module renamed:** `go.mod` is `module github.com/jig/dbos-transact-golang`
  (renamed from `dbos-inc/...` across all imports). Consumers (fluxos8) import the
  `jig/...` path; the `go.work` `use ../dbos-transact-golang` picks up this tree.
- **PoC stance:** no PL/pgSQL ever; drop & recreate the schema (no data
  migration). This is a deliberate PoC stance.
- **Total delta** (old fork `main` vs this baseline): ~53 files, +4.8k/-4.2k
  lines including tests. The fork is re-applied onto the baseline as a curated
  cherry-pick of the fork-authored commit clusters, skipping the #354-357 ports
  (now upstream) and the transactional-steps cluster (replaced by upstream
  datasources — see §3).
- **Verification:** the dbos test suite plus fluxos8 (`../../jig/fluxos8`,
  including its golden-history replay harness) must pass. Tests are the safety net
  that guarantees the re-application dropped no behavior.

---

## 1. Durable suspension (zero-RAM long waits) — the flagship feature

A long `Sleep`/`Recv`/`GetEvent`/child `GetResult` over `DurableSleepThreshold`
parks the workflow in the DB (status `DELAYED`) and releases its goroutine; the
queue runner re-enqueues it on wake. Implemented with a `panic(*workflowSuspension)`
recovered at the `RunWorkflow` boundary.

| Area | Files | Type | Notes |
|---|---|---|---|
| Config knob | `dbos.go` (`DurableSleepThreshold`) | Additive | one field + plumbing |
| Suspension core | `workflow.go` (`Sleep`, `RunWorkflow` goroutine recover, `suspendForResult/Recv/Event`, `workflowSuspension`, `errWorkflowSuspended`, `GetResult` cascade) | **Hot-rewrite** | the highest-conflict area; hooks live inside the busiest functions |
| Waiter wakeups | `system_database.go` (`suspendWorkflowToDelayed`, `suspendWorkflowForSleep/Result`, `wakeWorkflowWaiters`, `notifyWorkflowWaiters`, `hasUnconsumedNotification`, `hasWorkflowEvent`, `transitionDelayedWorkflows`) | Additive + hot | new methods, but `Send`/`SetEvent` paths are edited inline |
| Queue resume | `queue.go` (call to `transitionDelayedWorkflows`) | Hot-rewrite (small) | one hook in the runner loop |
| Waiters table | `migrations/38_create_workflow_waiters.sql` (+ `sqlite/`) | Additive | **uses migration number 38** — collides with upstream's 38 (see §6) |
| Tests | `durable_sleep_test.go` (new) | Additive | |

Re-apply notes: do this first on a fresh fork; the rest layers on top. The
`DELAYED` status itself already exists upstream (delayed-enqueue), so it is
reused, not added. Determinism contract (steps before a suspending call must be
`RunAsStep`; `recover()` must re-panic the sentinel) is documented in
`notes/durable-sleep.md`.

## 2. Plain-SQL Postgres (no PL/pgSQL, no LISTEN/NOTIFY)

Notifications are delivered by polling instead of triggers; all PL/pgSQL
functions/triggers and the `search_path` hardening are removed. Postgres becomes
a polling backend like CockroachDB.

| Area | Files | Type | Notes |
|---|---|---|---|
| Drop trigger/func migrations | `migrations/1_initial_dbos_schema_listen_notify.sql`, `10_add_notifications_pkey.sql`, `14_add_pgsql_client_functions.sql`, `20_set_function_search_path.sql` (deleted) | **Deletion** | replaced by a plain mig-10 applied in code (`applyNotificationsPKeyMigration`) |
| Migration plumbing | `system_database.go` (`buildMigrations(schema, isCockroach)` reduced to 2 args; removed `notificationListenerLoop`, `listenNotifyPool`, channel consts; added `notificationPollerLoop`, `_NOTIFICATION_POLL_INTERVAL = 100ms`) | **Hot-rewrite** | |
| Dialect interface | `dialect.go` (removed `SupportsListenNotify` from interface + all impls; cockroach kept only `Name()`) | Deletion | |
| sqlite list | `sqlite_migrations.go` (skip 10/14/20) | Deletion | |
| Client functions test | `pgsql_client_test.go` (deleted, -381) | Deletion | tested the removed helper functions |
| Test | `plpgsql_test.go` (new, asserts zero functions/triggers/plpgsql) | Additive | |

Re-apply notes: this is the most likely to conflict with upstream because
upstream keeps adding PL/pgSQL (e.g. #353's `enqueue_workflow` rewrite and the
streams trigger). On a mature upstream, reconsider whether to keep this as a
hard removal or as a config flag — the interim `DisablePLpgSQL` flag was tried
(94bb0e3) and then dropped in favour of plain-SQL-always (8c1bba6).

## 3. Transactional steps (`RunAsTransaction`)

User SQL + the DBOS checkpoint commit in one transaction, recorded in a
`transaction_outputs` table that can live in a separate application database.

| Area | Files | Type | Notes |
|---|---|---|---|
| Engine | `application_database.go` (new, 328) | Additive | `transaction_outputs` DDL, `recordTransactionResult/Failure`, fork-copy, GC cleanup, step listing, `poolIsPostgres` gating |
| Public API | `transaction.go` (new, 49) | Additive | `RunAsTransaction`, `runAsTxn` |
| Workflow hook | `workflow.go` (`runAsAppTxn`, rollback-on-error semantics) | **Hot-rewrite** | lives alongside the step machinery |
| Pool/Tx abstraction | `dbq.go` (`poolIsPostgres`, `newSQLPool(db, isPostgres)`, `SQLTx`, adapters) | Additive | |
| Config | `dbos.go` (`ApplicationDatabaseURL`, `ApplicationDBPool`, `ApplicationSQLDB`) | Additive | `ApplicationSQLDB` = database/sql Postgres pool for Persist |
| Client | `client.go` (`ClientConfig`: `SystemDBPool`, `SqliteSystemDB`, `ApplicationDatabaseURL`) | Additive | |
| sqlite pool sig | `sqlite_pool.go` (`newSQLPool(db, false)`) | Additive (trivial) | |
| Tests | `transaction_test.go` (new, 596) | Additive | |

Re-apply notes: **watch upstream's `datasources` branch** —
it is the official generalisation of this (`NewDataSource` over `*pgxpool.Pool | *sql.DB`,
`transaction_completion` table). If it has merged by re-fork time, adopt it
instead of porting this, and drop most of §3.

## 4. Graceful shutdown → PENDING + `recovery_attempts` reset

A workflow interrupted by a clean engine shutdown is left `PENDING` (not finalized
`ERROR`) so recovery resumes it, and its `recovery_attempts` is reset so repeated
deploys don't dead-letter it. A real crash takes neither path, so crash-loop
protection is preserved.

The branch matches the shutdown by its cancellation **cause**: `Shutdown` cancels
the root context with the `errShutdownInitiated` sentinel, and only that cause
leaves the workflow `PENDING`. A caller cancelling its own context
(`WithCancelCause` etc.) finalises the workflow (`ERROR`), as upstream does —
the original condition could not tell the two apart and left user-cancelled
workflows `PENDING`, to be silently re-executed on the next launch (surfaced as
`TestSelect` flakiness).

| Area | Files | Type | Notes |
|---|---|---|---|
| Leave-PENDING + reset | `workflow.go` (`RunWorkflow` shutdown branch) | **Hot-rewrite** | gated on `context.Cause(...) == errShutdownInitiated` (`dbos.go`) |
| Reset helper | `system_database.go` (`resetWorkflowRecoveryAttempts`) | Additive | reached via `systemDatabase.concrete()` (see §6) |
| Tests | `workflows_test.go` (`TestWorkflowLeftPendingOnShutdown`, `TestGracefulRebootDoesNotExhaustRecoveryAttempts`, `TestUserCancellationFinalizesWorkflow`) | Additive | |

Re-apply notes: the principled long-term fix (lease/heartbeat liveness instead of
a dispatch counter) is deferred.

## 5. DBOSError code preservation across replay

A recorded step/workflow error is rebuilt as a `*DBOSError` with its original
`Code` on replay, so `errors.As`/code checks behave identically after recovery.

| Files | Type |
|---|---|
| `errors.go` (rebuild from "DBOS Error <code>: ..."), `serialization.go` (de/serializeWorkflowError) | Hot-rewrite (small) |
| `errors_test.go` (new) | Additive |

## 6. Step-ID stability across transient retries

`Recv` / `GetEvent` / `Sleep` used to allocate their step ID(s) inside the
`retryWithResult` closure (in `sysDB.recv` / `getEvent` / `sleep`), so a
transient error (SQLITE_BUSY, dropped connection) leaked IDs on each attempt:
the recorded history gets a gap and a later replay re-executes inside it
(caught by fluxos8's golden-history harness). The IDs are now reserved once by
the caller, outside the retry loop, and passed via the existing re-entry
fields (`recvInput.stepID` / `sleepStepID`, etc.); the callee-side allocation
remains as a defensive fallback only.

Supporting change: `systemDatabase` gains `concrete() *sysDB` and runtime code
uses it instead of `.(*sysDB)` type assertions, so tests can wrap the system
database with fault-injecting facades (embedding the interface inherits
`concrete()`).

| Area | Files | Type | Notes |
|---|---|---|---|
| Caller-side ID reservation | `workflow.go` (`Recv`, `GetEvent`, `Sleep`) | **Hot-rewrite** (small) | |
| `concrete()` seam | `system_database.go` (interface + impl), assertion sites in `workflow.go`, `client.go`, `dbos.go` | Mechanical | behaviour unchanged for the real `*sysDB` |
| Tests | `retry_step_ids_test.go` (new) | Additive | facade asserts every attempt carries the same pre-assigned IDs |

## 7. Gates as a runtime primitive (fluxos8 ADR 0012)

Design: `notes/GATES-DESIGN.md`. A gate is a recv with authoritative,
transactional state: `GateRecv` opens it (idempotent upsert before waiting),
closes it in the same transaction as the recv checkpoint; `DeliverToGate`
verifies open+audience and records an audited outcome atomically, signalling
only when delivered; `IgnoreDelivery` is the D6 policy mark.

| Area | Files | Type | Notes |
|---|---|---|---|
| Migration 39 (3 tables) | `migrations/39_create_workflow_gates.sql` (+ sqlite) | Additive | symbolic audiences; delivery audit with outcomes |
| Runtime API + sysDB ops | `gates.go` (new), `recvInput.gate` hook in `workflow.go`/`system_database.go` | Additive + small hot-rewrite (recv close joins its checkpoint tx; send accepts a pre-assigned message_uuid) | recv step name/shape unchanged → replay-compatible |
| Interface | `DBOSContext` + `systemDatabase` gain the 3 ops | Additive | |
| Tests | `gates_test.go` (new) | Additive | lifecycle, rejects, ignore, timeout-close |
| Read audience + inbox (fluxos8 ADR 0013 / 0012 D5) | `migrations/40_create_workflow_read_audience.sql` (+ sqlite), `readaudience.go` (AddReadAudience, ReadAllowed = read ∪ gate rows, ListOpenGatesFor/ListDeliveriesBy/ListInitiatedBy), interface additions | Additive | 'initiator' principal type doubles as the my-workflows index |

## 8. Upstream changes deliberately NOT taken

| Upstream | Why skipped |
|---|---|
| #353 mig `38_update_enqueue_workflow` (PL/pgSQL) | enqueue is done in Go; violates §2. **Number 38 is reused** by this fork's `workflow_waiters` |
| #353 mig `39_create_streams_trigger` (LISTEN/NOTIFY) | streams read by polling here |
| #353 mig `40_add_attributes` (+ sqlite) | plain SQL & compatible, but no Go consumer yet — pull with the attribute-filter feature |

Re-apply note: the migration-number collision (fork 38 = `workflow_waiters` vs
upstream 38/39/40) is the single nastiest re-application detail. On a fresh fork,
renumber the fork-specific migrations above the upstream range (e.g. 1000+) to
end the collision permanently — drop & recreate makes this free.

## 9. Infra / docs

| Files | Type | Notes |
|---|---|---|
| `.gitignore` (new) | Additive | ignores `dbos.lock` |
| `README.md` (+176) | Additive | sections: durable sleep, plain-SQL Postgres, transactional steps |
| `notes/durable-sleep.md` (new) | Additive | design note |
| `chaos_tests/CLAUDE.md` (+34) | Additive | session notes |
| Test helpers | `utils_test.go`, `client_test.go`, `dbos_test.go`, `migration_test.go` | Hot (small) | backend switch (`DBOS_TEST_BACKEND=sqlite`), migration-version asserts |
| `system_database_deadlock_test.go` (new) | Additive | init-deadlock regression |

---

## Re-forking strategy (when upstream is mature)

1. Start the fresh fork at an upstream **tag**, not main.
2. Re-apply in order: §2 (plain-SQL) → §1 (suspension) → §3 (transactional steps,
   or adopt `datasources` instead) → §4 → §5. Additive items first, hot-rewrites
   last and one at a time.
3. **Renumber fork migrations to 1000+** to kill the §6 collision.
4. Convert the hot-rewrites (§1, §2, §3, §4 in `workflow.go`/`system_database.go`)
   into the smallest possible **hooks** that call code in new files, so the next
   upstream bump barely conflicts.
5. Push the genuinely general pieces upstream (durable suspension; the SQL pool —
   already converging via `datasources`) so they leave the delta entirely.
