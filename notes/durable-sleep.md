# Design note: a Temporal-style durable `Sleep`

Status: exploration / not implemented. Captures the findings and a plan so it
can be picked up on another machine.

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

Temporal's trick (usable here): park the goroutine on a channel and make it
**unreachable**. A goroutine blocked on a channel with no live references is
**garbage-collected by Go, and its `defer`s do NOT run** (defers only run on
`return`/`panic`). That gives "evict without running defers", like Temporal.
Implementation must, before parking: decrement `workflowsWg`, remove from
`activeWorkflowIDs`, write `DELAYED`, and drop all references so GC can reclaim
the goroutine.

## Costs (this is a change of contract, not just an optimisation)

1. **Loses "plain Go, runs once".** With suspend-on-sleep, all non-step code
   before the `Sleep` **re-runs on every wake** (not only on rare crashes). This
   imposes the **same determinism discipline as Temporal** (`time.Now()`,
   `rand`, map iteration, I/O must be steps) and will surface latent bugs. This
   is DBOS's main selling point, so it must be opt-in.
2. **`defer`/resource semantics across `Sleep`.** Suspend does not run defers
   (like Temporal), so non-durable resources (files, locks, conns) cannot be
   held across a `Sleep`. Must be documented.
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
     `activeWorkflowIDs`, and parks-then-drops the goroutine (GC, no defers).
  2. Make `sleep` choose in-process vs suspend based on a threshold.
  3. Confirm the queue runner re-dequeues `DELAYED` -> replay -> continue.
  4. Tests: long sleep across a worker restart; cancellation; determinism of
     non-step code on replay; loop-with-sleep replay cost.

## Key references in this repo

- `dbos/workflow.go:2863` — `dbosContext.Sleep`
- `dbos/system_database.go:2623` — `sysDB.sleep`
- `dbos/system_database.go:2673/2655/2705/2709` — record endTime / replay / remaining / `time.Sleep`
- Delayed-queue primitives: `WorkflowStatusDelayed`, `DelayUntil`, `WithDelay`,
  `SetWorkflowDelay` (see `dbos/workflow.go`, `dbos/queue.go`)
- Recovery / replay: `dbos/recovery.go`, `checkOperationExecution` in
  `dbos/system_database.go`
