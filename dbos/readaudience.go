package dbos

// Per-instance read audience and the inbox projection queries (fluxos8
// ADR 0013 / ADR 0012 D5). The read check is the union of the instance's
// read-audience rows with its gate-audience rows: whoever may act must be
// able to see what they are acting on. All queries are org-scoped and match
// the caller's expanded principals against symbolic rows, exactly like
// DeliverToGate.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// GatePrincipalInitiator marks the instance's starter in the read-audience
// rows; it also serves the "my workflows" reverse query.
const GatePrincipalInitiator = "initiator"

// AddReadAudience upserts read-audience rows for a workflow instance.
// Widening only: rows are added, never removed (ADR 0013 D2).
func AddReadAudience(ctx DBOSContext, workflowID, org string, principals []GatePrincipal) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.AddReadAudience(ctx, workflowID, org, principals)
}

func (c *dbosContext) AddReadAudience(_ DBOSContext, workflowID, org string, principals []GatePrincipal) error {
	if workflowID == "" {
		return errors.New("workflow ID is required")
	}
	return retry(c, func() error {
		return c.systemDB.addReadAudience(c, workflowID, org, principals)
	}, withRetrierLogger(c.logger))
}

// ReadAllowed reports whether the caller may see the instance: a read-audience
// match, a gate-audience match (any gate, open or closed), or the wildcard.
func ReadAllowed(ctx DBOSContext, workflowID, org, subject string, groups []string) (bool, error) {
	if ctx == nil {
		return false, errors.New("ctx cannot be nil")
	}
	return ctx.ReadAllowed(ctx, workflowID, org, subject, groups)
}

func (c *dbosContext) ReadAllowed(_ DBOSContext, workflowID, org, subject string, groups []string) (bool, error) {
	return retryWithResult(c, func() (bool, error) {
		return c.systemDB.readAllowed(c, workflowID, org, subject, groups)
	}, withRetrierLogger(c.logger))
}

// OpenGateRow is one "waiting on me" inbox entry.
type OpenGateRow struct {
	WorkflowID string
	Gate       string
	Org        string
	OpenedAt   time.Time
	ExpiresAt  *time.Time
}

// ListOpenGatesFor returns the open, unexpired gates whose audience admits the
// caller (org-scoped; exclusions honored) — "waiting on me" (ADR 0012 D5).
func ListOpenGatesFor(ctx DBOSContext, org, subject string, groups []string, limit int) ([]OpenGateRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListOpenGatesFor(ctx, org, subject, groups, limit)
}

func (c *dbosContext) ListOpenGatesFor(_ DBOSContext, org, subject string, groups []string, limit int) ([]OpenGateRow, error) {
	return retryWithResult(c, func() ([]OpenGateRow, error) {
		return c.systemDB.listOpenGatesFor(c, org, subject, groups, limit)
	}, withRetrierLogger(c.logger))
}

// DeliveryRow is one "recent decisions" entry: a delivery attempt by the
// caller and its durable outcome.
type DeliveryRow struct {
	DeliveryID string
	WorkflowID string
	Gate       string
	Outcome    string
	CreatedAt  time.Time
}

// ListDeliveriesBy returns the caller's delivery attempts, newest first.
func ListDeliveriesBy(ctx DBOSContext, org, subject string, limit int) ([]DeliveryRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListDeliveriesBy(ctx, org, subject, limit)
}

func (c *dbosContext) ListDeliveriesBy(_ DBOSContext, org, subject string, limit int) ([]DeliveryRow, error) {
	return retryWithResult(c, func() ([]DeliveryRow, error) {
		return c.systemDB.listDeliveriesBy(c, org, subject, limit)
	}, withRetrierLogger(c.logger))
}

// ListInitiatedBy returns the workflow IDs the caller started, newest first
// ("my workflows": the indexed initiator rows).
func ListInitiatedBy(ctx DBOSContext, org, subject string, limit int) ([]string, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListInitiatedBy(ctx, org, subject, limit)
}

func (c *dbosContext) ListInitiatedBy(_ DBOSContext, org, subject string, limit int) ([]string, error) {
	return retryWithResult(c, func() ([]string, error) {
		return c.systemDB.listInitiatedBy(c, org, subject, limit)
	}, withRetrierLogger(c.logger))
}

/* ------------------------- system database side ------------------------- */

func (s *sysDB) addReadAudience(ctx context.Context, workflowID, org string, principals []GatePrincipal) error {
	if len(principals) == 0 {
		return nil
	}
	q := s.renderSQL(`INSERT INTO %sworkflow_read_audience (workflow_uuid, principal_type, principal, org)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (workflow_uuid, principal_type, principal) DO NOTHING`, s.dialect.SchemaPrefix(s.schema))
	for _, p := range principals {
		if _, err := s.pool.Exec(ctx, q, workflowID, p.Type, p.Principal, org); err != nil {
			return fmt.Errorf("failed to insert read-audience row: %w", err)
		}
	}
	return nil
}

// principalMatchClause renders "this row matches the caller" over a row
// alias, appending its parameters to args (placeholders continue from
// len(args)). Initiator rows match like user rows for reading: the starter
// always sees their instance.
func principalMatchClause(alias, subject string, groups []string, args *[]any) string {
	*args = append(*args, GatePrincipalAll)
	allIdx := len(*args)
	*args = append(*args, GatePrincipalUser, GatePrincipalInitiator, subject)
	userIdx, initIdx, subjIdx := len(*args)-2, len(*args)-1, len(*args)
	clause := fmt.Sprintf("(%s.principal_type = $%d OR (%s.principal_type IN ($%d, $%d) AND %s.principal = $%d)",
		alias, allIdx, alias, userIdx, initIdx, alias, subjIdx)
	if len(groups) > 0 {
		*args = append(*args, GatePrincipalGroup)
		typeIdx := len(*args)
		ph := make([]string, len(groups))
		for i, g := range groups {
			*args = append(*args, g)
			ph[i] = fmt.Sprintf("$%d", len(*args))
		}
		clause += fmt.Sprintf(" OR (%s.principal_type = $%d AND %s.principal IN (%s))",
			alias, typeIdx, alias, strings.Join(ph, ", "))
	}
	return clause + ")"
}

func (s *sysDB) readAllowed(ctx context.Context, workflowID, org, subject string, groups []string) (bool, error) {
	args := []any{workflowID, org}
	readClause := principalMatchClause("r", subject, groups, &args)
	gateClause := principalMatchClause("a", subject, groups, &args)
	q := s.renderSQL(`SELECT
		EXISTS (SELECT 1 FROM %sworkflow_read_audience r
			WHERE r.workflow_uuid = $1 AND (r.org = $2 OR r.org IS NULL OR r.org = '') AND `+readClause+`)
		OR EXISTS (SELECT 1 FROM %sworkflow_gate_audience a
			WHERE a.workflow_uuid = $1 AND (a.org = $2 OR a.org IS NULL OR a.org = '') AND `+gateClause+`)`,
		s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
	var allowed bool
	if err := s.pool.QueryRow(ctx, q, args...).Scan(&allowed); err != nil {
		return false, fmt.Errorf("failed to check read audience: %w", err)
	}
	return allowed, nil
}

func (s *sysDB) listOpenGatesFor(ctx context.Context, org, subject string, groups []string, limit int) ([]OpenGateRow, error) {
	if limit <= 0 {
		limit = 50
	}
	args := []any{org, time.Now().UnixMilli()}
	match := principalMatchClause("a", subject, groups, &args)
	args = append(args, subject)
	exceptClause := fmt.Sprintf("x.principal = $%d", len(args))
	args = append(args, limit)
	limitClause := fmt.Sprintf("LIMIT $%d", len(args))
	pfx := s.dialect.SchemaPrefix(s.schema)
	q := `SELECT g.workflow_uuid, g.gate, g.org, g.opened_at_epoch_ms, g.expires_at_epoch_ms
		FROM ` + pfx + `workflow_gates g
		WHERE g.org = $1 AND g.open AND (g.expires_at_epoch_ms IS NULL OR g.expires_at_epoch_ms > $2)
		AND EXISTS (SELECT 1 FROM ` + pfx + `workflow_gate_audience a
			WHERE a.workflow_uuid = g.workflow_uuid AND a.gate = g.gate AND ` + match + `)
		AND NOT EXISTS (SELECT 1 FROM ` + pfx + `workflow_gate_audience x
			WHERE x.workflow_uuid = g.workflow_uuid AND x.gate = g.gate
			AND x.principal_type = '` + GatePrincipalExcept + `' AND ` + exceptClause + `)
		ORDER BY g.opened_at_epoch_ms DESC ` + limitClause
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list open gates: %w", err)
	}
	defer rows.Close()
	var out []OpenGateRow
	for rows.Next() {
		var r OpenGateRow
		var opened int64
		var expires *int64
		if err := rows.Scan(&r.WorkflowID, &r.Gate, &r.Org, &opened, &expires); err != nil {
			return nil, err
		}
		r.OpenedAt = time.UnixMilli(opened).UTC()
		if expires != nil {
			t := time.UnixMilli(*expires).UTC()
			r.ExpiresAt = &t
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sysDB) listDeliveriesBy(ctx context.Context, org, subject string, limit int) ([]DeliveryRow, error) {
	if limit <= 0 {
		limit = 50
	}
	q := s.renderSQL(`SELECT delivery_uuid, workflow_uuid, gate, outcome, created_at_epoch_ms
		FROM %sworkflow_gate_deliveries
		WHERE by_org = $1 AND by_subject = $2
		ORDER BY created_at_epoch_ms DESC LIMIT $3`, s.dialect.SchemaPrefix(s.schema))
	rows, err := s.pool.Query(ctx, q, org, subject, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list deliveries: %w", err)
	}
	defer rows.Close()
	var out []DeliveryRow
	for rows.Next() {
		var r DeliveryRow
		var created int64
		if err := rows.Scan(&r.DeliveryID, &r.WorkflowID, &r.Gate, &r.Outcome, &created); err != nil {
			return nil, err
		}
		r.CreatedAt = time.UnixMilli(created).UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sysDB) listInitiatedBy(ctx context.Context, org, subject string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 50
	}
	q := s.renderSQL(`SELECT r.workflow_uuid
		FROM %sworkflow_read_audience r JOIN %sworkflow_status w ON w.workflow_uuid = r.workflow_uuid
		WHERE r.org = $1 AND r.principal_type = $2 AND r.principal = $3
		ORDER BY w.created_at DESC LIMIT $4`, s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
	rows, err := s.pool.Query(ctx, q, org, GatePrincipalInitiator, subject, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list initiated workflows: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
