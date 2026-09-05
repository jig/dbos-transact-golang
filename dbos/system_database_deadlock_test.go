package dbos

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestNewDBOSContextDoesNotDeadlockOnMigrationFailure is a regression test for a
// deadlock where newSystemDatabase held the detection connection (acquired to
// detect the database type) across pool.Close() on the migration-failure error
// path. pgxpool.Pool.Close() blocks until every acquired connection is released,
// so NewContext hung forever instead of returning the "permission denied"
// error.
//
// The scenario: a role that can connect but lacks CREATE on the database, so the
// CREATE SCHEMA in runMigrations is denied (SQLSTATE 42501, non-retryable). With
// the bug, NewContext never returns; with the fix it returns the error fast.
func TestNewDBOSContextDoesNotDeadlockOnMigrationFailure(t *testing.T) {
	skipIfSqlite(t, "requires a real Postgres role without CREATE privilege")

	const (
		roleName     = "dbos_lowpriv_deadlock"
		rolePassword = "lowpriv_pw"
		// A schema that does not exist yet, so newSystemDatabase tries to create
		// it via runMigrations (CREATE SCHEMA), which the low-priv role can't.
		testSchema = "dbos_deadlock_regression"
	)

	superURL := getDatabaseURL()
	adminCfg, err := pgx.ParseConfig(superURL)
	require.NoError(t, err)

	ctx := context.Background()

	// Set up the low-privilege role as superuser. A freshly created role is not
	// the database owner and therefore lacks CREATE on the database, which is
	// what CREATE SCHEMA requires. We also drop the target schema to guarantee a
	// migration attempt.
	adminConn, err := pgx.ConnectConfig(ctx, adminCfg)
	require.NoError(t, err)

	role := pgx.Identifier{roleName}.Sanitize()
	schema := pgx.Identifier{testSchema}.Sanitize()
	db := pgx.Identifier{adminCfg.Database}.Sanitize()
	cleanup := func() {
		c, err := pgx.ConnectConfig(context.Background(), adminCfg)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		bg := context.Background()
		// Terminate any lingering backends owned by the role so DROP ROLE can
		// proceed even if a previous (buggy) run left connections checked out.
		_, _ = c.Exec(bg, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1", roleName)
		_, _ = c.Exec(bg, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		// The GRANT/REVOKE on the database registers a shared dependency on the
		// role; it must be revoked before the role can be dropped.
		_, _ = c.Exec(bg, "REVOKE ALL PRIVILEGES ON DATABASE "+db+" FROM "+role)
		_, _ = c.Exec(bg, "DROP ROLE IF EXISTS "+role)
	}
	cleanup()

	_, err = adminConn.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	require.NoError(t, err)
	_, err = adminConn.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD '"+rolePassword+"'")
	require.NoError(t, err)
	// Be explicit about the privilege the deadlock depends on: the role must be
	// able to connect but must NOT be able to create schemas.
	_, err = adminConn.Exec(ctx, "GRANT CONNECT ON DATABASE "+db+" TO "+role)
	require.NoError(t, err)
	_, err = adminConn.Exec(ctx, "REVOKE CREATE ON DATABASE "+db+" FROM "+role)
	require.NoError(t, err)
	adminConn.Close(ctx)
	t.Cleanup(cleanup)

	// Build the low-privilege connection URL (same host/port/db, different creds).
	lowprivURL := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(roleName, rolePassword),
		Host:     adminCfg.Host + ":" + strconv.Itoa(int(adminCfg.Port)),
		Path:     "/" + adminCfg.Database,
		RawQuery: "sslmode=disable",
	}

	// Run NewContext under a watchdog. pool.Close() does not observe the
	// context, so a context timeout would not break the deadlock — we need an
	// out-of-band timer to fail the test instead of hanging the suite forever.
	type result struct {
		dbosCtx Context
		err     error
	}
	done := make(chan result, 1)
	go func() {
		c, err := NewContext(context.Background(), Config{
			AppName:        "deadlock-test",
			DatabaseURL:    lowprivURL.String(),
			DatabaseSchema: testSchema,
		})
		done <- result{c, err}
	}()

	select {
	case res := <-done:
		if res.dbosCtx != nil {
			Shutdown(res.dbosCtx, 10*time.Second)
		}
		require.Error(t, res.err, "expected a permission error, not a successful init")
		require.True(t,
			strings.Contains(res.err.Error(), "permission denied") ||
				strings.Contains(res.err.Error(), "failed to run migrations"),
			"expected a migration/permission error, got: %v", res.err)
	case <-time.After(30 * time.Second):
		t.Fatal("NewContext hung: pool.Close() deadlocked with the detection connection still checked out (regression)")
	}
}
