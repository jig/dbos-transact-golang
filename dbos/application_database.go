package dbos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// application_database.go: the optional application database for transactional
// steps.
//
// RunAsTransaction commits the user's SQL together with a step checkpoint in one
// transaction. That checkpoint lives in a transaction_outputs table which, unlike
// operation_outputs, has no foreign key to workflow_status — so it can live in a
// database other than the DBOS system database. This mirrors the DBOS Python SDK,
// where transactions run against a separate application database.
//
// When no application database is configured the app pool defaults to the system
// database pool, preserving the previous behaviour. The feature is Postgres-only
// (CockroachDB, being wire-compatible, also works); for a SQLite system database
// the table is simply not created.

// appDatabase holds the pool used by RunAsTransaction plus the bookkeeping needed
// to tear it down.
type appDatabase struct {
	pool   Pool
	schema string
	owned  bool // true when we created the pool and must Close it on shutdown
}

// setupApplicationDB resolves the pool that RunAsTransaction should target and
// ensures the transaction_outputs table exists there.
//
//   - ApplicationDBPool set        → wrap it (not owned).
//   - ApplicationDatabaseURL set   → build a pool for it (owned).
//   - neither                      → reuse the system database pool (not owned).
func setupApplicationDB(ctx context.Context, config *Config, systemPool Pool, schema string, logger *slog.Logger) (*appDatabase, error) {
	var (
		pool  Pool
		owned bool
	)

	switch {
	case config.ApplicationDBPool != nil:
		logger.Info("Using custom application database connection pool")
		pool = newPgxPool(config.ApplicationDBPool)
	case config.ApplicationDatabaseURL != "":
		p, err := newApplicationPool(ctx, config.ApplicationDatabaseURL, config.AppName, logger)
		if err != nil {
			return nil, err
		}
		pool = p
		owned = true
	default:
		// No separate application database: transactions run against the system DB.
		pool = systemPool
	}

	if err := ensureTransactionOutputsTable(ctx, pool, schema); err != nil {
		if owned {
			pool.Close()
		}
		return nil, fmt.Errorf("failed to ensure transaction_outputs table: %w", err)
	}

	return &appDatabase{pool: pool, schema: schema, owned: owned}, nil
}

// newApplicationPool builds a pgx pool for a separate application database,
// matching the connection settings used for the system database pool.
func newApplicationPool(ctx context.Context, databaseURL, appName string, logger *slog.Logger) (Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse application database URL: %w", err)
	}
	cfg.MaxConns = 20
	cfg.MinConns = 0
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = time.Minute * 5
	cfg.ConnConfig.ConnectTimeout = 10 * time.Second
	if appName != "" {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = make(map[string]string)
		}
		cfg.ConnConfig.RuntimeParams["application_name"] = appName
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create application database pool: %w", err)
	}

	maskedURL, _ := maskPassword(pool.Config().ConnString())
	logger.Info("Connecting to application database", "database_url", maskedURL)

	if err := retry(ctx, func() error {
		return createDatabaseIfNotExists(ctx, pool, logger)
	}, withRetrierLogger(logger)); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create application database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping application database: %w", err)
	}
	return newPgxPool(pool), nil
}

// appDialect is the dialect used for transaction_outputs queries. The application
// database is always Postgres-compatible (CockroachDB included), so the Postgres
// dialect is correct for placeholder style, schema prefixing, and unique-violation
// classification alike.
var appDialect Dialect = postgresDialect{}

// ensureTransactionOutputsTable idempotently creates the transaction_outputs
// table (and its schema) in the application database. It is a no-op for non-pgx
// pools (e.g. a SQLite system database), since transactional steps are
// Postgres-only.
func ensureTransactionOutputsTable(ctx context.Context, pool Pool, schema string) error {
	if PgxPool(pool) == nil {
		return nil
	}

	schemaIdent := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaIdent)); err != nil {
		return err
	}

	// No FOREIGN KEY to workflow_status: that is what lets this table live in a
	// database separate from the DBOS system schema. Using IF NOT EXISTS keeps it
	// clear of the dbos_migrations versioning of the system schema.
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %stransaction_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    started_at_epoch_ms BIGINT,
    completed_at_epoch_ms BIGINT,
    serialization TEXT,
    PRIMARY KEY (workflow_uuid, function_id)
)`, appDialect.SchemaPrefix(schema))
	if _, err := pool.Exec(ctx, ddl); err != nil {
		return err
	}
	return nil
}

// checkTransactionExecution looks up a previously recorded transactional-step
// result for (workflowID, stepID) in transaction_outputs, using the supplied
// application-database transaction so the read shares the step's snapshot.
// Returns (nil, nil) when the step has not run yet.
func checkTransactionExecution(ctx context.Context, tx Tx, schema, workflowID string, stepID int, stepName string) (*recordedResult, error) {
	query := appDialect.RewriteQuery(fmt.Sprintf(
		`SELECT output, error, function_name, serialization FROM %stransaction_outputs WHERE workflow_uuid = $1 AND function_id = $2`,
		appDialect.SchemaPrefix(schema)))

	var (
		outputString         *string
		errorStr             *string
		recordedFunctionName string
		serialization        *string
	)
	err := tx.QueryRow(ctx, query, workflowID, stepID).Scan(&outputString, &errorStr, &recordedFunctionName, &serialization)
	if err != nil {
		if err == ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get transaction outputs: %w", err)
	}

	if stepName != recordedFunctionName {
		return nil, newUnexpectedStepError(workflowID, stepID, stepName, recordedFunctionName)
	}

	var storedSerialization string
	if serialization != nil {
		storedSerialization = *serialization
	}
	var recordedErrStr *string
	if errorStr != nil && *errorStr != "" {
		recordedErrStr = errorStr
	}
	return &recordedResult{
		output:        outputString,
		errStr:        recordedErrStr,
		serialization: storedSerialization,
	}, nil
}

// copyTransactionOutputsForFork copies the transactional-step checkpoints of
// originalID with function_id < startStep onto forkedID, so a forked workflow does
// not re-execute (and thus re-apply) transactional steps it is supposed to skip.
// It must run before the forked workflow becomes visible (ENQUEUED) in the system
// database. ON CONFLICT DO NOTHING makes it idempotent across retries.
// No-op for non-pgx pools (SQLite system database), where transactional steps are
// unsupported anyway.
func copyTransactionOutputsForFork(ctx context.Context, pool Pool, schema, originalID, forkedID string, startStep int) error {
	if startStep <= 0 || PgxPool(pool) == nil {
		return nil
	}
	prefix := appDialect.SchemaPrefix(schema)
	query := appDialect.RewriteQuery(fmt.Sprintf(
		`INSERT INTO %stransaction_outputs (workflow_uuid, function_id, function_name, output, error, started_at_epoch_ms, completed_at_epoch_ms, serialization)
		 SELECT $1, function_id, function_name, output, error, started_at_epoch_ms, completed_at_epoch_ms, serialization
		 FROM %stransaction_outputs
		 WHERE workflow_uuid = $2 AND function_id < $3
		 ON CONFLICT DO NOTHING`, prefix, prefix))
	if _, err := pool.Exec(ctx, query, forkedID, originalID, startStep); err != nil {
		return fmt.Errorf("failed to copy transaction outputs from %s to %s: %w", originalID, forkedID, err)
	}
	return nil
}

// deleteTransactionOutputs removes all transactional-step checkpoints of the given
// workflows from the application database. Used to clean up after a failed fork and
// by workflow deletion/garbage collection. No-op for non-pgx pools.
func deleteTransactionOutputs(ctx context.Context, pool Pool, schema string, workflowIDs []string) error {
	if len(workflowIDs) == 0 || PgxPool(pool) == nil {
		return nil
	}
	query := appDialect.RewriteQuery(fmt.Sprintf(
		`DELETE FROM %stransaction_outputs WHERE workflow_uuid = ANY($1)`,
		appDialect.SchemaPrefix(schema)))
	if _, err := pool.Exec(ctx, query, workflowIDs); err != nil {
		return fmt.Errorf("failed to delete transaction outputs: %w", err)
	}
	return nil
}

// listTransactionSteps returns the transactional-step checkpoints of a workflow as
// stepInfo entries, so getWorkflowSteps can merge them with the regular steps from
// operation_outputs (transactional steps are recorded only in transaction_outputs).
// Returns nil for non-pgx pools, where transactional steps are unsupported.
func listTransactionSteps(ctx context.Context, pool Pool, schema, workflowID string, loadOutput bool) ([]stepInfo, error) {
	if PgxPool(pool) == nil {
		return nil, nil
	}
	query := appDialect.RewriteQuery(fmt.Sprintf(
		`SELECT function_id, function_name, output, error, started_at_epoch_ms, completed_at_epoch_ms, serialization
		 FROM %stransaction_outputs
		 WHERE workflow_uuid = $1
		 ORDER BY function_id ASC`, appDialect.SchemaPrefix(schema)))

	rows, err := pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query transaction steps: %w", err)
	}
	defer rows.Close()

	var steps []stepInfo
	for rows.Next() {
		var step stepInfo
		var outputString *string
		var errorString *string
		var startedAtMs, completedAtMs *int64
		var serialization *string

		if err := rows.Scan(&step.StepID, &step.StepName, &outputString, &errorString, &startedAtMs, &completedAtMs, &serialization); err != nil {
			return nil, fmt.Errorf("failed to scan transaction step row: %w", err)
		}
		if startedAtMs != nil {
			step.StartedAt = time.Unix(0, *startedAtMs*int64(time.Millisecond))
		}
		if completedAtMs != nil {
			step.CompletedAt = time.Unix(0, *completedAtMs*int64(time.Millisecond))
		}
		if loadOutput {
			step.Output = outputString
		}
		if serialization != nil {
			step.Serialization = *serialization
		}
		if errorString != nil && *errorString != "" {
			step.Error = errors.New(*errorString)
		}
		steps = append(steps, step)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over transaction step rows: %w", err)
	}
	return steps, nil
}

// recordTransactionResult inserts the transactional step's checkpoint into
// transaction_outputs using the supplied application-database transaction, so the
// checkpoint commits atomically with the user's writes.
func recordTransactionResult(ctx context.Context, tx Tx, schema string, input recordOperationResultDBInput) error {
	query := appDialect.RewriteQuery(fmt.Sprintf(
		`INSERT INTO %stransaction_outputs (workflow_uuid, function_id, output, error, function_name, started_at_epoch_ms, completed_at_epoch_ms, serialization) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		appDialect.SchemaPrefix(schema)))

	_, err := tx.Exec(ctx, query,
		input.workflowID, input.stepID, input.output, input.errStr, input.stepName,
		input.startedAt.UnixMilli(), input.completedAt.UnixMilli(), input.serialization)
	if err != nil {
		if appDialect.IsUniqueViolation(err) {
			return newWorkflowConflictIDError(input.workflowID)
		}
		return err
	}
	return nil
}
