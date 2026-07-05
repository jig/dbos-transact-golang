# chaos_tests

Randomized durability tests for the DBOS Transact Go SDK. Each test repeatedly
interrupts in-flight workflows (crash/recovery cycles with random timing) and
verifies they complete with exactly-once step semantics.

## What's here

- `chaos_test.go` — the test suite:
  - `TestChaosWorkflow` — workflows with multiple steps and transactions.
  - `TestChaosRecv` — send/recv across interruptions.
  - `TestChaosEvents` — SetEvent/GetEvent across interruptions.
  - `TestChaosQueues` — queued workflows across interruptions.
- `TestMain` builds the `cmd/dbos` CLI into `./dbos-cli-test` and uses it to
  start Postgres (`dbos postgres start`) before running.

## Running

Requires Docker (the CLI starts a Postgres container). From this directory:

```bash
go test -v -race -timeout 60m -count=1 ./...
```

CI runs this via `.github/workflows/chaos-tests.yml`. These tests are slow by
design; they are not part of the regular `go test ./dbos` suite.

## Note

Transactional steps are now provided by upstream's data sources
(`dbos.NewDataSource` + `RunAsTransaction(ctx, ds, fn)`); this fork's earlier
`dbos/transaction.go` / `Config.ApplicationSQLDB` implementation was dropped in
the re-fork. See `notes/DIVERGENCES.md` §3 and the README "Transactional Steps"
section.
