package dbos

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

// TestRunAsTransactionSeparateAppDB exercises a transactional step whose user
// tables live in a separate application database, configured via
// Config.ApplicationDatabaseURL. It proves the user's writes and the step
// checkpoint commit together in the application database (not the DBOS system
// database) and that replay stays exactly-once.
func TestRunAsTransactionSeparateAppDB(t *testing.T) {
	skipIfSqlite(t, "transactional steps are Postgres-only")

	ctx := context.Background()
	sysURL := getDatabaseURL()
	resetTestDatabase(t, sysURL)

	// Derive a separate application database URL on the same server. The pool
	// setup creates the database if it does not exist.
	base, err := pgx.ParseConfig(sysURL)
	require.NoError(t, err)
	appURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s_appdb?sslmode=disable",
		base.User, url.QueryEscape(base.Password), base.Host, base.Port, base.Database)

	dbosCtx, err := NewDBOSContext(ctx, Config{
		DatabaseURL:            sysURL,
		ApplicationDatabaseURL: appURL,
		AppName:                "test-app",
	})
	require.NoError(t, err)
	require.NotNil(t, dbosCtx)
	t.Cleanup(func() { Shutdown(dbosCtx, 30*time.Second) })

	// Application table lives in the application database only.
	appPool, err := pgxpool.New(ctx, appURL)
	require.NoError(t, err)
	defer appPool.Close()
	_, err = appPool.Exec(ctx, `DROP TABLE IF EXISTS accounts`)
	require.NoError(t, err)
	_, err = appPool.Exec(ctx, `CREATE TABLE accounts (name TEXT PRIMARY KEY, balance INT NOT NULL)`)
	require.NoError(t, err)
	_, err = appPool.Exec(ctx, `INSERT INTO accounts VALUES ('alice', 100), ('bob', 0)`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = appPool.Exec(ctx, `DROP TABLE IF EXISTS accounts`) })

	// Workflow transfers funds: debit + credit + checkpoint in one transaction.
	transfer := func(wfCtx DBOSContext, amount int) (int, error) {
		return RunAsTransaction(wfCtx, func(txCtx context.Context, tx Tx) (int, error) {
			var balance int
			if err := tx.QueryRow(txCtx, `SELECT balance FROM accounts WHERE name = 'alice'`).Scan(&balance); err != nil {
				return 0, err
			}
			if _, err := tx.Exec(txCtx, `UPDATE accounts SET balance = balance - $1 WHERE name = 'alice'`, amount); err != nil {
				return 0, err
			}
			if _, err := tx.Exec(txCtx, `UPDATE accounts SET balance = balance + $1 WHERE name = 'bob'`, amount); err != nil {
				return 0, err
			}
			return balance - amount, nil
		}, WithTxIsolation(IsoLevelSerializable))
	}

	RegisterWorkflow(dbosCtx, transfer)
	require.NoError(t, Launch(dbosCtx))

	wfID := uuid.NewString()
	handle, err := RunWorkflow(dbosCtx, transfer, 30, WithWorkflowID(wfID))
	require.NoError(t, err)
	remaining, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, 70, remaining)

	// Writes committed in the application database.
	var aliceBal, bobBal int
	require.NoError(t, appPool.QueryRow(ctx, `SELECT balance FROM accounts WHERE name = 'alice'`).Scan(&aliceBal))
	require.NoError(t, appPool.QueryRow(ctx, `SELECT balance FROM accounts WHERE name = 'bob'`).Scan(&bobBal))
	require.Equal(t, 70, aliceBal)
	require.Equal(t, 30, bobBal)

	// Checkpoint recorded in the application database's transaction_outputs.
	var txnCount int
	require.NoError(t, appPool.QueryRow(ctx,
		`SELECT count(*) FROM dbos.transaction_outputs WHERE workflow_uuid = $1`, wfID).Scan(&txnCount))
	require.Equal(t, 1, txnCount)

	// The application table does not exist in the system database.
	sysPool, err := pgxpool.New(ctx, sysURL)
	require.NoError(t, err)
	defer sysPool.Close()
	var existsInSystem bool
	require.NoError(t, sysPool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'accounts')`).Scan(&existsInSystem))
	require.False(t, existsInSystem)

	// Exactly-once: replaying the same workflow ID does not move the money again.
	handle2, err := RunWorkflow(dbosCtx, transfer, 30, WithWorkflowID(wfID))
	require.NoError(t, err)
	remaining2, err := handle2.GetResult()
	require.NoError(t, err)
	require.Equal(t, 70, remaining2)
	require.NoError(t, appPool.QueryRow(ctx, `SELECT balance FROM accounts WHERE name = 'alice'`).Scan(&aliceBal))
	require.Equal(t, 70, aliceBal)
}
