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

## History note

This file previously held the working notes of the session that implemented
`RunAsTransaction` (transactional steps). That feature is implemented and
documented in the README ("Transactional Steps") and `dbos/transaction.go`;
the notes were stale (they predated the separate application database support)
and were replaced by this description of the directory.
