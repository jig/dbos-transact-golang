# Design note: a Temporal-style durable `Sleep`

Status: **implemented** (opt-in via `Config.DurableSleepThreshold`). See
"Implementation" at the end. The original exploration is kept below, with one
correction: the "GC of parked goroutines" mechanism described in "The hard
part" was wrong and is not what was built.

## Background

While building a small example app on this fork (an HTTP gateway + background
worker + a workflow that waits for a reply via `Recv`, then `Sleep`s), two
things surfaced:

1. **Graceful shutdown turned waiting workflows into `ERROR`.** A workflow
   blocked in `Recv` gets `context.Canceled` when `dbos.Shutdown` cancels the
   context; returning that error records the workflow as terminal `ERROR`, which
   recovery does **not** resume — losing the in-flight instance. A hard crash
   (`kill -9`), by contrast, leaves it `PENDING` and recovery resumes it.

   Workaround used in the example app (not in this library): run the durable
   waits on `dbos.WithoutCancel(ctx)`, so shutdown does not unblock them; the
   worker exits after its grace period with the instance still `PENDING` and a
   restarted worker recovers it. (See the fluxos5 example repo.)

2. **`Sleep` is an in-process, non-cancellable `time.Sleep`.**

## How `Sleep` works today

- Public: `dbos.Sleep` -> `dbosContext.Sleep` (`dbos/workflow.go:2863`)
- Impl: `sysDB.sleep` (`dbos/system_database.go:2623`)
- The wait itself: **`dbos/system_database.go:2709` -> `time.Sleep(remainingDuration)`**

Durability does **not** come from the in-memory sleep. On first execution the
wake-up instant is recorded as a step output (`endTime = now + d`,
`system_database.go:2673`). On recovery the recorded `endTime` is read back
(`:2655`) and only the **remaining** time is slept (`remaining = time.Until(endTime)`,
`:2705`). So `Sleep(2 years)` is durable across restarts — it wakes at the right
wall-clock instant regardless of crashes.

The cost (the "smell"):

- A sleeping workflow **holds a live goroutine** for the whole remaining
  duration; it is not yielded/dequeued. `Sleep(2y)` parks a goroutine for 2
  years while the process is up.
- The `time.Sleep` is **non-cancellable**, so a graceful `Shutdown` cannot drain
  it; the goroutine is abandoned at the grace timeout and recovered later.

## Temporal (Go SDK) for comparison

`workflow.Sleep` does **not** call `time.Sleep`. It issues a `StartTimer`
command; the timer lives **durably on the Temporal Service**, and the workflow
is suspended (and, for long timers, **evicted** from the worker's sticky cache).
While sleeping it consumes **no worker goroutine/CPU**. When the timer fires the
server schedules a Workflow Task; a worker **replays** the event history (fresh
goroutine) and continues. Timers are **cancellable** (ctx cancel -> CancelTimer
-> `Sleep` returns `ctx.Err()`).

Net: Temporal has none of the smell — durable, cancellable, scales to years,
zero worker cost during the wait. The price is the **determinism/replay**
programming model.

## Can this fork get a Temporal-style `Sleep`? Yes, in principle

The pieces already exist here:

- **Replay by re-execution.** Recovery already re-runs the workflow function
  from the top and skips completed steps via `checkOperationExecution`
  (memoised output). Same principle as Temporal's replay, with per-step
  memoisation in Postgres instead of an event-history blob.
- **Durable delayed queue.** `WorkflowStatusDelayed`, `DelayUntil`,
  `WithDelay(d)`, `SetWorkflowDelay(...)` already exist, and the queue runner
  already promotes `DELAYED -> ENQUEUED` when the delay expires and re-dequeues.

### Sketch

```
Sleep(d):
  endTime := record_step(now + d)              // durable, idempotent (already done)
  if remaining(endTime) > threshold:
      set_status(DELAYED, delay_until = endTime)  // free the worker
      <suspend the goroutine WITHOUT recording an outcome>
  else:
      time.Sleep(remaining)                    // short sleeps: in-process, cheap
```

On `delay_until` the queue runner re-dequeues -> replay (prior steps memoised)
-> reaches `Sleep` again -> `endTime` passed -> continue.

### The hard part: suspending a goroutine in Go

You cannot kill a goroutine. Options to stop executing the function:

- (a) **Park it** (block on a channel) — stays alive, no benefit (today's state).
- (b) **Unwind via `panic`** — frees the stack but **runs user `defer`s**, which
  is wrong for a *pause* (not an end).
- (c) **Cooperative yield** (thread a sentinel error up the whole stack) — not
  transparent; the workflow code must cooperate.

**Correction (this section was wrong):** Go does **not** garbage-collect
blocked goroutines — the runtime keeps a reference to every goroutine
(`allgs`), which is exactly why goroutine leaks exist. There is no
"evict without running defers" in Go. The Temporal Go SDK itself unwinds
evicted workflow coroutines with `runtime.Goexit()`, which **does run
deferred functions**. So any suspension mechanism in Go (panic sentinel,
`runtime.Goexit`) runs user defers on suspension; this must be documented,
not avoided. The implementation below uses a panic sentinel recovered by the
workflow runner goroutine.

## Costs (this is a change of contract, not just an optimisation)

1. **Loses "plain Go, runs once".** With suspend-on-sleep, all non-step code
   before the `Sleep` **re-runs on every wake** (not only on rare crashes). This
   imposes the **same determinism discipline as Temporal** (`time.Now()`,
   `rand`, map iteration, I/O must be steps) and will surface latent bugs. This
   is DBOS's main selling point, so it must be opt-in.
2. **`defer`/resource semantics across `Sleep`.** Suspension unwinds the
   goroutine via panic, so user defers **do run on every suspension** (and the
   resources they release are simply re-acquired on replay if acquired in
   steps... which they must not be — non-durable resources cannot be held
   across a suspending `Sleep` either way). A `recover()` in workflow code
   that swallows unknown panic values breaks suspension. Must be documented.
3. **Replay is DB-heavy.** Each wake does one `checkOperationExecution` per prior
   step -> O(steps) round-trips per wake, O(steps^2) for loops with repeated
   sleeps. Needs a `ContinueAsNew`-like escape hatch.
4. **Engineering.** Suspend/resume control flow + DELAYED integration +
   replay-on-wake + interplay with deadlines/cancel/`workflowsWg`/
   `activeWorkflowIDs`, plus determinism tests. Medium-to-large.

## Cheaper alternatives

- **Cancellable sleep (tiny, ~5 lines).** Replace the bare `time.Sleep` at
  `system_database.go:2709` with:
  ```go
  select {
  case <-time.After(remaining):
  case <-ctx.Done():
      return remaining, ctx.Err()
  }
  ```
  Fixes clean shutdown drain. Does **not** free the goroutine during the wait
  (still holds it), so it does not solve "holds a goroutine for 2 years".
  Caveat: this would make a cancelled `Sleep` return `ctx.Err()`, i.e. it would
  turn shutdown into `ERROR` again unless paired with the `WithoutCancel`
  pattern or a "leave PENDING on shutdown" policy — so think about the desired
  shutdown semantics first.

- **Threshold hybrid (best cost/benefit).** Short sleeps stay in-process (cheap,
  no replay); long sleeps (> X) take the suspend-via-`DELAYED` + replay path.
  Pay the replay cost only when it is worth it.

## Recommendation / next steps

- If only the shutdown smell matters: cancellable sleep is trivial (but decide
  shutdown semantics: `ERROR` vs leave-`PENDING`/recover).
- For true Temporal-style scale on long waits: implement the **opt-in**
  threshold hybrid (e.g. `dbos.SleepDurable(ctx, d)` or a configurable
  threshold), so existing workflows keep their run-once semantics.
- Prototype plan:
  1. Add a `suspendWorkflow` sentinel/path in the run-workflow goroutine that
     writes `DELAYED` + `delay_until`, decrements `workflowsWg`, clears
     `activeWorkflowIDs`, and unwinds the goroutine (panic sentinel; defers
     run — see correction above).
  2. Make `sleep` choose in-process vs suspend based on a threshold.
  3. Confirm the queue runner re-dequeues `DELAYED` -> replay -> continue.
  4. Tests: long sleep across a worker restart; cancellation; determinism of
     non-step code on replay; loop-with-sleep replay cost.

## Key references in this repo

(Function names, not line numbers — lines drift.)

- `dbosContext.Sleep` in `dbos/workflow.go`
- `sysDB.sleep` in `dbos/system_database.go` (records/reads the durable
  `endTime` step output; `skipSleep` returns the remaining duration)
- Delayed-queue primitives: `WorkflowStatusDelayed`, `DelayUntil`, `WithDelay`,
  `SetWorkflowDelay`, `transitionDelayedWorkflows`
- Recovery / replay: `dbos/recovery.go`, `checkOperationExecution` in
  `dbos/system_database.go`

## Implementation (June 2026)

Opt-in threshold hybrid, as recommended above:

- `Config.DurableSleepThreshold time.Duration` — 0 (default) disables
  suspension entirely (upstream behavior). When > 0, any `dbos.Sleep` whose
  *remaining* duration exceeds the threshold suspends the workflow.
- Suspension path (`dbosContext.Sleep` in `dbos/workflow.go`):
  1. `sysDB.sleep(skipSleep: true)` records (or reads back) the durable
     wake-up time and returns the remaining duration.
  2. `sysDB.suspendWorkflowForSleep` atomically transitions
     `PENDING -> DELAYED` with `delay_until = endTime`, assigns
     `queue_name = COALESCE(queue_name, '_dbos_internal_queue')` (direct-run
     workflows get parked on the internal queue; enqueued workflows keep their
     queue), clears `started_at`, and resets `recovery_attempts` to 0 (a
     suspension is voluntary, so every wake-up gets a fresh retry budget —
     otherwise a workflow sleeping in a loop would hit
     MAX_RECOVERY_ATTEMPTS_EXCEEDED). Guarded on `status = PENDING`: if the
     workflow was cancelled concurrently, the update affects 0 rows and Sleep
     falls back to the in-process wait.
  3. `panic(&workflowSuspension{...})` unwinds the user code (defers run);
     the runner goroutine in `dbosContext.RunWorkflow` recovers *only* this
     sentinel (anything else is re-panicked), skips outcome recording, and
     sends `errWorkflowSuspended` on the in-memory outcome channel.
- Wake-up: the queue runner's `transitionDelayedWorkflows` promotes
  `DELAYED -> ENQUEUED` when `delay_until` expires; dequeue re-executes the
  workflow function from the top with completed steps memoized; the `Sleep`
  step finds its recorded `endTime`, remaining ≈ 0, and continues.
- In-memory handles: `workflowHandle.GetResult` intercepts
  `errWorkflowSuspended` and falls back to polling the DB
  (`workflowPollingHandle`), so callers still get the final result.
- Scale: the partial index `idx_workflow_status_delayed`
  (`delay_until_epoch_ms WHERE status = 'DELAYED'`, migration 16) makes the
  wake-up scan cheap with millions of sleeping workflows; a suspended
  workflow costs zero goroutines/RAM.
- Caveats (documented on `dbos.Sleep`): code before a suspending `Sleep`
  re-runs on every wake-up (must be in steps if non-deterministic or
  side-effecting); user `recover()` must re-panic unknown values; a parent
  blocked on a child's `GetResult` still holds its own goroutine; dequeue
  filters by `application_version`, so workflows sleep across restarts of the
  *same* app version (standard DBOS recovery semantics).
- Tests: `dbos/durable_sleep_test.go`.

### Phase 2: suspend on `GetResult` (await another workflow's result)

Without this, a parent blocked on a child's `GetResult` held its goroutine even
while the child was suspended. Now (same `DurableSleepThreshold` opt-in):

- **Waiter registry**: `workflow_waiters (waiter, awaited)` table (migration 38),
  inserted atomically with the waiter's `PENDING -> DELAYED` transition
  (`sysDB.suspendWorkflowForResult`).
- **Event-driven wake**: every place a workflow becomes terminal —
  `updateWorkflowOutcome` (SUCCESS/ERROR/CANCELLED, now transactional),
  `cancelWorkflows`, and the MAX_RECOVERY_ATTEMPTS_EXCEEDED path in
  `insertWorkflowStatus` — calls `wakeWorkflowWaiters`, which sets the waiters'
  `delay_until = now` and deletes the rows; the queue runner promotes and
  re-executes them as usual.
- **Lost-wake fallback**: the waiter's `delay_until` is set to
  `now + _waiterWakeFallbackInterval` (1h). If a wake is lost (crash between the
  completion and the wake), the waiter replays at the fallback and re-suspends
  if the awaited workflow is still running. The completed-before-registered race
  is closed by re-checking the awaited status right after committing the waiter
  row and self-waking.
- **Where it hooks in**:
  - `workflowPollingHandle.GetResult` (queued children, replayed handles): polls
    in-process up to the threshold (grace), then suspends.
  - `workflowHandle.GetResult` (direct children): blocks on the channel up to
    the threshold, then suspends. If the child's suspension marker arrives, the
    suspension **cascades immediately** up the parent chain (tested three levels
    deep), so a whole tree waiting on one sleeping leaf costs zero goroutines.
  - A `GetResult` with an explicit timeout never suspends (timeout honored
    in-process). Calls from outside a workflow are unaffected.

### Phase 3: suspend on `Recv` (await a message)

Same opt-in. No `NoTimeout` sentinel was added: the existing timeout semantics
are preserved exactly, only the waiting becomes free.

- If no message arrives within the threshold, `sysDB.recv` returns a suspension
  sentinel (recording nothing — the timeout's sleep step was already memoized on
  first execution) and `dbosContext.Recv` suspends the workflow via
  `suspendWorkflowForResult(X, X, delayUntil)`: the self-waiter row marks
  "suspended waiting for a message".
- `delay_until = min(timeout deadline, now + fallback)`: the deadline preserves
  the recv timeout; the fallback bounds a lost wake-up. On a spurious/fallback
  wake the replayed recv just re-suspends.
- **Event-driven wake**: `send()` now inserts the notification and bumps the
  destination's `delay_until` to now in the same transaction, guarded by the
  self-waiter row (so workflows DELAYED for other reasons — initial enqueue
  delay, durable sleep — are not woken by stray sends).
- The completed-before-registered race is closed by re-checking for an
  unconsumed notification after committing the suspension and self-waking.
- A failed suspension (e.g. concurrent cancel) falls back to the in-process
  wait, re-entering recv with the same step IDs.
- `deleteWorkflows` now also clears waiter rows involving deleted workflows.

Still pending: the same treatment for `GetEvent` (wake = `SetEvent` on the
target; waiter = caller → target, which collides nicely with the existing
result-waiter wake on the target's completion).
