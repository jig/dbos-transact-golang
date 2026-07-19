package sysdb

// System-database side of gates as a runtime primitive (fork; fluxos8 ADR 0012,
// design in notes/GATES-DESIGN.md) and of the per-instance read audience /
// inbox projections (ADR 0013). The public API lives in the dbos package
// (gates.go, readaudience.go); this file owns the SQL.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jig/dbos-transact-golang/dbos/internal/models"

	"github.com/google/uuid"
)

// Gate principal types stored in workflow_gate_audience.
const (
	GatePrincipalUser   = "user"
	GatePrincipalGroup  = "group"
	GatePrincipalAll    = "all"
	GatePrincipalExcept = "except" // categorical exclusion (e.g. the initiator); data, not policy (ADR 0012 D6)
	// GatePrincipalInitiator marks the instance's starter in the read-audience
	// rows; it also serves the "my workflows" reverse query.
	GatePrincipalInitiator = "initiator"
)

// GatePrincipal is one symbolic audience row.
type GatePrincipal struct {
	Type      string // GatePrincipalUser | GatePrincipalGroup | GatePrincipalAll
	Principal string // subid | group name | "*"
}

// GateOutcome is the durable result of a delivery attempt.
type GateOutcome string

const (
	GateDelivered        GateOutcome = "delivered"
	GateRejectedClosed   GateOutcome = "rejected-closed"
	GateRejectedAudience GateOutcome = "rejected-audience"
	GateIgnored          GateOutcome = "ignored"
)

// GateSpec describes the gate a recv opens and owns.
type GateSpec struct {
	Name      string
	Org       string
	Audience  []GatePrincipal
	ExpiresAt time.Time // zero = no deadline
}

// GateTopic is the notification topic a gate's deliveries travel on. It keeps
// the pre-primitive "gate:<name>" convention so live histories recorded by the
// event-based implementation replay unchanged.
func GateTopic(name string) string { return "gate:" + name }

// DeliverInput is one delivery attempt against a gate.
type DeliverInput struct {
	WorkflowID string
	Gate       string
	Subject    string   // validated caller principal
	Org        string   // caller's organisation
	Groups     []string // caller's memberships, resolved by the CALLER's groups provider
	ClaimsJSON string   // serialized validated claims, stored in the audit row
	Payload    any
}

// OpenGateRow is one "waiting on me" inbox entry.
type OpenGateRow struct {
	WorkflowID string
	Gate       string
	Org        string
	OpenedAt   time.Time
	ExpiresAt  *time.Time
}

// DeliveryRow is one "recent decisions" entry: a delivery attempt by the
// caller and its durable outcome.
type DeliveryRow struct {
	DeliveryID string
	WorkflowID string
	Gate       string
	BySubject  string // populated by the per-workflow audit query
	Outcome    string
	CreatedAt  time.Time
}

// OpenGate upserts the gate row and replaces its audience. Idempotent: crash
// between open and the first recv checkpoint re-runs it harmlessly.
func (s *SysDB) OpenGate(ctx context.Context, workflowID string, recvStepID int, g GateSpec) error {
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if g.Org == "" {
		// Default the gate's organisation to the workflow's tenant, so the
		// author-facing API needs no explicit org plumbing.
		q := s.RenderSQL(`SELECT COALESCE(authenticated_user, '') FROM %sworkflow_status WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))
		if err := tx.QueryRow(ctx, q, workflowID).Scan(&g.Org); err != nil {
			return fmt.Errorf("failed to resolve gate org: %w", err)
		}
	}
	now := time.Now().UnixMilli()
	var expires *int64
	if !g.ExpiresAt.IsZero() {
		v := g.ExpiresAt.UnixMilli()
		expires = &v
	}
	upsert := s.RenderSQL(`INSERT INTO %sworkflow_gates
		(workflow_uuid, gate, org, open, expires_at_epoch_ms, opened_at_epoch_ms, closed_at_epoch_ms, recv_step_id)
		VALUES ($1, $2, $3, true, $4, $5, NULL, $6)
		ON CONFLICT (workflow_uuid, gate) DO UPDATE SET
			org = EXCLUDED.org, open = true, expires_at_epoch_ms = EXCLUDED.expires_at_epoch_ms,
			opened_at_epoch_ms = EXCLUDED.opened_at_epoch_ms, closed_at_epoch_ms = NULL,
			recv_step_id = EXCLUDED.recv_step_id`, s.dialect.SchemaPrefix(s.schema))
	if _, err := tx.Exec(ctx, upsert, workflowID, g.Name, g.Org, expires, now, recvStepID); err != nil {
		return fmt.Errorf("failed to upsert gate: %w", err)
	}
	del := s.RenderSQL(`DELETE FROM %sworkflow_gate_audience WHERE workflow_uuid = $1 AND gate = $2`, s.dialect.SchemaPrefix(s.schema))
	if _, err := tx.Exec(ctx, del, workflowID, g.Name); err != nil {
		return fmt.Errorf("failed to clear gate audience: %w", err)
	}
	ins := s.RenderSQL(`INSERT INTO %sworkflow_gate_audience (workflow_uuid, gate, principal_type, principal, org)
		VALUES ($1, $2, $3, $4, $5)`, s.dialect.SchemaPrefix(s.schema))
	for _, p := range g.Audience {
		if _, err := tx.Exec(ctx, ins, workflowID, g.Name, p.Type, p.Principal, g.Org); err != nil {
			return fmt.Errorf("failed to insert gate audience row: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// CloseGate marks the gate closed inside the recv's checkpoint transaction and
// resolves the consumed notification to its delivery audit row.
func (s *SysDB) CloseGate(ctx context.Context, tx Tx, workflowID, gate string, messageUUID *string) (string, error) {
	now := time.Now().UnixMilli()
	upd := s.RenderSQL(`UPDATE %sworkflow_gates SET open = false, closed_at_epoch_ms = $1
		WHERE workflow_uuid = $2 AND gate = $3`, s.dialect.SchemaPrefix(s.schema))
	if _, err := tx.Exec(ctx, upd, now, workflowID, gate); err != nil {
		return "", fmt.Errorf("failed to close gate: %w", err)
	}
	if messageUUID == nil {
		return "", nil
	}
	var deliveryID string
	q := s.RenderSQL(`SELECT delivery_uuid FROM %sworkflow_gate_deliveries WHERE message_uuid = $1`, s.dialect.SchemaPrefix(s.schema))
	if err := tx.QueryRow(ctx, q, *messageUUID).Scan(&deliveryID); err != nil {
		// A plain Send on the gate topic has no audit row; tolerate during the
		// transition from the event-based implementation.
		return "", nil
	}
	return deliveryID, nil
}

// DeliverToGate atomically verifies the gate is open and unexpired, matches
// the caller's principals against the stored audience, records the delivery
// with its outcome, and — only when delivered — signals the waiting workflow.
func (s *SysDB) DeliverToGate(ctx context.Context, in DeliverInput, encodedPayload *string, serialization string) (GateOutcome, string, error) {
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	now := time.Now().UnixMilli()

	// The workflow must exist (404 material, not an audit row).
	var wfStatus models.WorkflowStatusType
	q := s.RenderSQL(`SELECT status FROM %sworkflow_status WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))
	if err := tx.QueryRow(ctx, q, in.WorkflowID).Scan(&wfStatus); err != nil {
		return "", "", models.NewNonExistentWorkflowError(in.WorkflowID)
	}

	outcome := GateDelivered
	var open bool
	var expires *int64
	q = s.RenderSQL(`SELECT open, expires_at_epoch_ms FROM %sworkflow_gates
		WHERE workflow_uuid = $1 AND gate = $2`, s.dialect.SchemaPrefix(s.schema))
	err = tx.QueryRow(ctx, q, in.WorkflowID, in.Gate).Scan(&open, &expires)
	switch {
	case err != nil: // no gate row: never opened (or pre-primitive workflow)
		outcome = GateRejectedClosed
	case !open, expires != nil && *expires <= now:
		outcome = GateRejectedClosed
	case wfStatus != models.WorkflowStatusPending && wfStatus != models.WorkflowStatusEnqueued && wfStatus != models.WorkflowStatusDelayed:
		outcome = GateRejectedClosed
	}

	if outcome == GateDelivered {
		match, err := s.audienceMatches(ctx, tx, in)
		if err != nil {
			return "", "", err
		}
		if !match {
			outcome = GateRejectedAudience
		}
	}

	deliveryID := uuid.NewString()
	var messageUUID *string
	if outcome == GateDelivered {
		id := uuid.NewString()
		messageUUID = &id
		if err := s.Send(ctx, WorkflowSendInput{
			DestinationID: in.WorkflowID,
			Message:       encodedPayload,
			Topic:         GateTopic(in.Gate),
			Serialization: serialization,
			Tx:            tx,
			MessageUUID:   messageUUID,
		}); err != nil {
			return "", "", fmt.Errorf("failed to signal gate delivery: %w", err)
		}
	}

	digest := sha256.Sum256([]byte(strOrEmpty(encodedPayload)))
	ins := s.RenderSQL(`INSERT INTO %sworkflow_gate_deliveries
		(delivery_uuid, workflow_uuid, gate, by_subject, by_org, claims, payload_digest, message_uuid, outcome, created_at_epoch_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, s.dialect.SchemaPrefix(s.schema))
	if _, err := tx.Exec(ctx, ins, deliveryID, in.WorkflowID, in.Gate, in.Subject, in.Org,
		in.ClaimsJSON, hex.EncodeToString(digest[:]), messageUUID, string(outcome), now); err != nil {
		return "", "", fmt.Errorf("failed to record delivery: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", fmt.Errorf("failed to commit delivery: %w", err)
	}
	return outcome, deliveryID, nil
}

// audienceMatches checks the caller's expanded principals against the gate's
// symbolic audience rows.
func (s *SysDB) audienceMatches(ctx context.Context, tx Tx, in DeliverInput) (bool, error) {
	args := []any{in.WorkflowID, in.Gate, GatePrincipalAll, GatePrincipalUser, in.Subject, GatePrincipalGroup}
	groupClause := "FALSE"
	if len(in.Groups) > 0 {
		ph := make([]string, len(in.Groups))
		for i, g := range in.Groups {
			args = append(args, g)
			ph[i] = fmt.Sprintf("$%d", len(args))
		}
		groupClause = "principal IN (" + strings.Join(ph, ", ") + ")"
	}
	q := s.RenderSQL(`SELECT EXISTS (SELECT 1 FROM %sworkflow_gate_audience
		WHERE workflow_uuid = $1 AND gate = $2 AND (
			principal_type = $3
			OR (principal_type = $4 AND principal = $5)
			OR (principal_type = $6 AND `+groupClause+`)
		))
		AND NOT EXISTS (SELECT 1 FROM %sworkflow_gate_audience
		WHERE workflow_uuid = $1 AND gate = $2
			AND principal_type = '`+GatePrincipalExcept+`' AND principal = $5)`,
		s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
	var match bool
	if err := tx.QueryRow(ctx, q, args...).Scan(&match); err != nil {
		return false, fmt.Errorf("failed to match gate audience: %w", err)
	}
	return match, nil
}

// IgnoreDelivery marks a delivered audit row as ignored by workflow policy
// (ADR 0012 D6). Idempotent.
func (s *SysDB) IgnoreDelivery(ctx context.Context, deliveryID string) error {
	q := s.RenderSQL(`UPDATE %sworkflow_gate_deliveries SET outcome = $1
		WHERE delivery_uuid = $2 AND outcome = $3`, s.dialect.SchemaPrefix(s.schema))
	if _, err := s.pool.Exec(ctx, q, string(GateIgnored), deliveryID, string(GateDelivered)); err != nil {
		return fmt.Errorf("failed to mark delivery ignored: %w", err)
	}
	return nil
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

/* ---------------- read audience & inbox projections (ADR 0013) ---------------- */

// AddReadAudience upserts read-audience rows for a workflow instance.
// Widening only: rows are added, never removed (ADR 0013 D2).
func (s *SysDB) AddReadAudience(ctx context.Context, workflowID, org string, principals []GatePrincipal) error {
	if len(principals) == 0 {
		return nil
	}
	q := s.RenderSQL(`INSERT INTO %sworkflow_read_audience (workflow_uuid, principal_type, principal, org)
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

// ReadAllowed reports whether the caller may see the instance: a read-audience
// match, a gate-audience match (any gate, open or closed), or the wildcard.
func (s *SysDB) ReadAllowed(ctx context.Context, workflowID, org, subject string, groups []string) (bool, error) {
	args := []any{workflowID, org}
	readClause := principalMatchClause("r", subject, groups, &args)
	gateClause := principalMatchClause("a", subject, groups, &args)
	q := s.RenderSQL(`SELECT
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

// ListOpenGatesFor returns the open, unexpired gates whose audience admits the
// caller (org-scoped; exclusions honored) — "waiting on me" (ADR 0012 D5).
func (s *SysDB) ListOpenGatesFor(ctx context.Context, org, subject string, groups []string, limit int) ([]OpenGateRow, error) {
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

// ListDeliveriesBy returns the caller's delivery attempts, newest first.
func (s *SysDB) ListDeliveriesBy(ctx context.Context, org, subject string, limit int) ([]DeliveryRow, error) {
	if limit <= 0 {
		limit = 50
	}
	q := s.RenderSQL(`SELECT delivery_uuid, workflow_uuid, gate, outcome, created_at_epoch_ms
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

// ListDeliveriesFor returns one instance's delivery audit, oldest first.
func (s *SysDB) ListDeliveriesFor(ctx context.Context, workflowID string, limit int) ([]DeliveryRow, error) {
	if limit <= 0 {
		limit = 100
	}
	q := s.RenderSQL(`SELECT delivery_uuid, workflow_uuid, gate, by_subject, outcome, created_at_epoch_ms
		FROM %sworkflow_gate_deliveries
		WHERE workflow_uuid = $1
		ORDER BY created_at_epoch_ms ASC LIMIT $2`, s.dialect.SchemaPrefix(s.schema))
	rows, err := s.pool.Query(ctx, q, workflowID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflow deliveries: %w", err)
	}
	defer rows.Close()
	var out []DeliveryRow
	for rows.Next() {
		var r DeliveryRow
		var created int64
		if err := rows.Scan(&r.DeliveryID, &r.WorkflowID, &r.Gate, &r.BySubject, &r.Outcome, &created); err != nil {
			return nil, err
		}
		r.CreatedAt = time.UnixMilli(created).UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListInitiatedBy returns the workflow IDs the caller started, newest first
// ("my workflows": the indexed initiator rows).
func (s *SysDB) ListInitiatedBy(ctx context.Context, org, subject string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 50
	}
	q := s.RenderSQL(`SELECT r.workflow_uuid
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
