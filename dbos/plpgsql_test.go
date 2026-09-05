package dbos

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNoPLpgSQL verifies that the Postgres schema is created with plain SQL only
// — no PL/pgSQL functions or triggers — and that notifications still work (via
// polling) end to end.
func TestNoPLpgSQL(t *testing.T) {
	skipIfSqlite(t, "checks the Postgres schema for PL/pgSQL objects")

	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()

	// No functions must exist in the schema: neither LISTEN/NOTIFY trigger
	// functions nor external-client functions.
	var fnCount int
	require.NoError(t, pool.QueryRow(bg, `
		SELECT count(*) FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'dbos'`).Scan(&fnCount))
	assert.Equal(t, 0, fnCount, "the schema must contain no functions")

	// No triggers on the dbos tables either.
	var trgCount int
	require.NoError(t, pool.QueryRow(bg, `
		SELECT count(*) FROM pg_trigger tg
		JOIN pg_class c ON c.oid = tg.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'dbos' AND NOT tg.tgisinternal`).Scan(&trgCount))
	assert.Equal(t, 0, trgCount, "the schema must contain no triggers")

	// No PL/pgSQL functions, keyed by the plpgsql language.
	var plpgsqlCount int
	require.NoError(t, pool.QueryRow(bg, `
		SELECT count(*) FROM pg_proc p
		JOIN pg_language l ON l.oid = p.prolang
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'dbos' AND l.lanname = 'plpgsql'`).Scan(&plpgsqlCount))
	assert.Equal(t, 0, plpgsqlCount, "no PL/pgSQL functions must exist")

	// End to end: a Send must reach a waiting Recv even though no NOTIFY trigger
	// fires — delivery is by polling.
	recvWorkflow := func(ctx Context, _ string) (string, error) {
		return Recv[string](ctx, "topic", 30*time.Second)
	}
	RegisterWorkflow(ctx, recvWorkflow)
	require.NoError(t, Launch(ctx))

	id := uuid.NewString()
	handle, err := RunWorkflow(ctx, recvWorkflow, "", WithWorkflowID(id))
	require.NoError(t, err)

	require.NoError(t, Send(ctx, id, "hello", "topic"))

	result, err := handle.GetResult()
	require.NoError(t, err, "Send must reach Recv via polling without PL/pgSQL")
	assert.Equal(t, "hello", result)
}
