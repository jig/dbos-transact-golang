package dbos

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestRunAsTransaction(t *testing.T) {
	skipIfSqlite(t, "transactional steps are Postgres-only")

	// setupDBOS(checkLeaks) runs goleak after its Shutdown cleanup, and its
	// ignore list already covers the pgx pool background goroutines used by the
	// verification pool below (closed before that check via defer).
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// A separate pool to the same database, used both to create the application
	// table and to read it back out-of-band (proving the workflow's write
	// actually committed).
	verifyPool, err := pgxpool.New(context.Background(), getDatabaseURL())
	require.NoError(t, err)
	defer verifyPool.Close()

	_, err = verifyPool.Exec(context.Background(),
		`CREATE TABLE IF NOT EXISTS test_txn_data (id text PRIMARY KEY, note text)`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = verifyPool.Exec(context.Background(), `DROP TABLE IF EXISTS test_txn_data`)
	})

	const rowID = "row-1"

	// Workflow that inserts a row through the provided Tx, atomically with the
	// step checkpoint.
	insertWorkflow := func(ctx DBOSContext, note string) (string, error) {
		return RunAsTransaction(ctx, func(txCtx context.Context, tx Tx) (string, error) {
			if _, err := tx.Exec(txCtx,
				`INSERT INTO test_txn_data (id, note) VALUES ($1, $2)`, rowID, note); err != nil {
				return "", err
			}
			return rowID, nil
		})
	}

	RegisterWorkflow(dbosCtx, insertWorkflow)
	require.NoError(t, Launch(dbosCtx))

	wfID := uuid.NewString()

	// First run: the row must be visible to a separate connection, proving the
	// user's write committed together with the checkpoint.
	handle, err := RunWorkflow(dbosCtx, insertWorkflow, "hello", WithWorkflowID(wfID))
	require.NoError(t, err)
	id, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, rowID, id)

	var note string
	err = verifyPool.QueryRow(context.Background(),
		`SELECT note FROM test_txn_data WHERE id = $1`, rowID).Scan(&note)
	require.NoError(t, err)
	require.Equal(t, "hello", note)

	// Exactly-once: re-running the SAME workflow ID is idempotent. It returns
	// the recorded result without re-executing the INSERT (which would otherwise
	// violate the primary key), so the row is still present exactly once.
	handle2, err := RunWorkflow(dbosCtx, insertWorkflow, "hello", WithWorkflowID(wfID))
	require.NoError(t, err)
	id2, err := handle2.GetResult()
	require.NoError(t, err)
	require.Equal(t, rowID, id2)

	var count int
	err = verifyPool.QueryRow(context.Background(),
		`SELECT count(*) FROM test_txn_data WHERE id = $1`, rowID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
