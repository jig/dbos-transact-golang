package dbos

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDisablePLpgSQL verifies that Config.DisablePLpgSQL creates the Postgres
// schema with plain SQL only — no PL/pgSQL trigger functions or client
// functions — and that notifications still work (via polling) end to end.
func TestDisablePLpgSQL(t *testing.T) {
	skipIfSqlite(t, "DisablePLpgSQL is a Postgres-only option")

	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, disablePLpgSQL: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()

	// No PL/pgSQL functions must exist in the schema: neither the LISTEN/NOTIFY
	// trigger functions (migration 1) nor the external-client functions
	// (migration 14).
	var fnCount int
	require.NoError(t, pool.QueryRow(bg, `
		SELECT count(*) FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'dbos'`).Scan(&fnCount))
	assert.Equal(t, 0, fnCount, "no functions should be created when PL/pgSQL is disabled")

	// No triggers on the dbos tables either.
	var trgCount int
	require.NoError(t, pool.QueryRow(bg, `
		SELECT count(*) FROM pg_trigger tg
		JOIN pg_class c ON c.oid = tg.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'dbos' AND NOT tg.tgisinternal`).Scan(&trgCount))
	assert.Equal(t, 0, trgCount, "no triggers should be created when PL/pgSQL is disabled")

	// The dialect must report no LISTEN/NOTIFY support, so the polling loop runs.
	assert.False(t, ctx.(*dbosContext).systemDB.(*sysDB).dialect.SupportsListenNotify(),
		"plain-SQL Postgres must not advertise LISTEN/NOTIFY")

	// End to end: a Send must reach a waiting Recv even though no NOTIFY trigger
	// fires — delivery is by polling.
	recvWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		return Recv[string](ctx, "topic", 30*time.Second)
	}
	RegisterWorkflow(ctx, recvWorkflow)
	require.NoError(t, Launch(ctx))

	id := uuid.NewString()
	handle, err := RunWorkflow(ctx, recvWorkflow, "", WithWorkflowID(id))
	require.NoError(t, err)

	require.NoError(t, Send(ctx, id, "hello", "topic"))

	result, err := handle.GetResult()
	require.NoError(t, err, "Send must reach Recv via polling with PL/pgSQL disabled")
	assert.Equal(t, "hello", result)
}
