package dbos

import "context"

// Transaction is a type-safe transactional step function with output type R.
//
// It receives a Tx bound to the DBOS system database. Any SQL the function
// issues through that Tx commits together with the DBOS step checkpoint in a
// single database transaction, so the user's writes inherit the same
// exactly-once guarantee as the workflow step itself.
type Transaction[R any] func(ctx context.Context, tx Tx) (R, error)

// RunAsTransaction runs fn as a single DBOS step whose database writes and step
// checkpoint commit atomically in one transaction. On serialization failures
// (PG 40001) and deadlocks (PG 40P01) the whole step is retried on a fresh
// transaction; on replay the recorded result is returned without re-running fn
// (exactly-once). For the retry to work, fn must return errors from Tx
// operations as-is (or wrapped), not swallow them.
//
// Usage requirements:
//   - The user MUST issue all SQL through the provided Tx, not through a
//     separate pool or connection. Only writes made through that Tx are part of
//     the atomic checkpoint commit.
//   - The tables must live in the application database, i.e. the same database
//     the checkpoint is recorded in. By default that is the DBOS system database;
//     set Config.ApplicationDatabaseURL (or ApplicationDBPool) to run transactions
//     against a separate Postgres database instead.
//   - It must be called directly from a workflow. If called nested within
//     another step, steps are flattened and the engine passes a nil Tx — do not
//     do that.
//   - fn must be safe to re-run: it can be invoked several times before a commit
//     succeeds (serialization-failure retries re-execute it on a fresh Tx).
//     Keep the body to Tx operations and pure computation; side effects outside
//     the Tx are not rolled back.
//
// Use WithTxIsolation to control the transaction isolation level (defaults to
// read committed).
func RunAsTransaction[R any](ctx DBOSContext, fn Transaction[R], opts ...StepOption) (R, error) {
	return runAsAppTxn(ctx, txn[R](fn), opts...)
}

// WithTxIsolation sets the isolation level of the transaction opened by
// RunAsTransaction. When unset, the transaction uses read committed.
func WithTxIsolation(level IsoLevel) StepOption {
	return func(opts *stepOptions) {
		opts.txIsoLevel = &level
	}
}
