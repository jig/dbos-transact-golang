package dbos

// Gates as a runtime primitive (fluxos8 ADR 0012, design in
// notes/GATES-DESIGN.md). A gate is a recv with authoritative, transactional
// state: opening joins the recv's pre-wait bookkeeping, closing joins the
// recv's checkpoint transaction, and delivery is conditional and atomic —
// verify open, verify audience, append the audit row and signal the waiter in
// one transaction. Audiences are stored symbolically (group rows carry the
// name, never expanded members); the caller expands ITS memberships at
// delivery time via its own groups provider.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Gate principal types stored in workflow_gate_audience.
const (
	GatePrincipalUser  = "user"
	GatePrincipalGroup = "group"
	GatePrincipalAll   = "all"
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

// gateSpec rides recvInput: the gate this recv opens and owns.
type gateSpec struct {
	Name      string
	Org       string
	Audience  []GatePrincipal
	ExpiresAt time.Time // zero = no deadline
}

// gateTopic is the notification topic a gate's deliveries travel on. It keeps
// the pre-primitive "gate:<name>" convention so live histories recorded by the
// event-based implementation replay unchanged.
func gateTopic(name string) string { return "gate:" + name }

// GateRecvInput opens a gate and waits for a delivery.
type GateRecvInput struct {
	Gate     string
	Org      string          // owning organisation (the workflow's tenant)
	Audience []GatePrincipal // empty = closed to everyone
	Timeout  time.Duration   // in-process/durable wait budget (caller-derived, deterministic)
	Expires  time.Time       // absolute gate deadline recorded for delivery checks (zero = none)
}

// GateRecv opens (or re-opens) the named gate and blocks until an eligible
// delivery arrives or the timeout fires; the gate closes in the same
// transaction as the recv checkpoint. Returns the payload and the delivery
// audit row ID ("" on timeout).
func GateRecv[T any](ctx DBOSContext, in GateRecvInput) (T, string, error) {
	var zero T
	if ctx == nil {
		return zero, "", errors.New("ctx cannot be nil")
	}
	msg, deliveryID, err := ctx.GateRecv(ctx, in)
	if err != nil {
		return zero, deliveryID, err
	}
	typed, err := convertRecvResult[T](ctx, msg)
	return typed, deliveryID, err
}

// convertRecvResult decodes a *recvResult into the caller's type, mirroring
// the public Recv[R] conversion.
func convertRecvResult[R any](ctx DBOSContext, msg any) (R, error) {
	var zero R
	if msg == nil {
		return zero, nil
	}
	if _, ok := ctx.(*dbosContext); !ok { // mocked path
		typed, ok := msg.(R)
		if !ok {
			workflowID, _ := GetWorkflowID(ctx)
			return zero, newWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", msg))
		}
		return typed, nil
	}
	result, ok := msg.(*recvResult)
	if !ok {
		workflowID, _ := GetWorkflowID(ctx)
		return zero, newWorkflowUnexpectedResultType(workflowID, "*recvResult", fmt.Sprintf("%T", msg))
	}
	if result.message == nil {
		return zero, nil
	}
	msgDecoder, err := resolveDecoder[R](result.serialization, getCustomSerializerFromCtx(ctx))
	if err != nil {
		return zero, err
	}
	typed, err := msgDecoder.Decode(result.message)
	if err != nil {
		return zero, fmt.Errorf("decoding received message to type %T: %w", *new(R), err)
	}
	return typed, nil
}

func (c *dbosContext) GateRecv(_ DBOSContext, in GateRecvInput) (any, string, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, "", newStepExecutionError("", "DBOS.recv", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}
	if wfState.isWithinStep {
		return nil, "", newStepExecutionError(wfState.workflowID, "DBOS.recv", fmt.Errorf("cannot call GateRecv within a step"))
	}
	if in.Gate == "" {
		return nil, "", errors.New("gate name cannot be empty")
	}
	input := recvInput{
		Topic:         gateTopic(in.Gate),
		Timeout:       in.Timeout,
		serialization: resolveEncoder(c).Name(),
		gate:          &gateSpec{Name: in.Gate, Org: in.Org, Audience: in.Audience, ExpiresAt: in.Expires},
	}
	if c.config.DurableSleepThreshold > 0 {
		input.suspendThreshold = c.config.DurableSleepThreshold
	}
	// Reserve the step IDs once, outside the transient-error retry loop (see
	// Recv): allocating per attempt gaps the recorded history.
	stepID := wfState.nextStepID()
	sleepStepID := wfState.nextStepID()
	input.stepID = &stepID
	input.sleepStepID = &sleepStepID
	recvRetryOpts := []retryOption{withRetrierLogger(c.logger)}
	if sysDB := c.systemDB.concrete(); sysDB != nil && sysDB.isCockroachDB {
		recvRetryOpts = append(recvRetryOpts, withRetryCondition(cockroachDialect{}.IsRetryableTransaction))
	}
	result, err := retryWithResult(c, func() (*recvResult, error) {
		return c.systemDB.recv(c, input)
	}, recvRetryOpts...)
	if err != nil || result == nil || !result.suspend {
		return result, deliveryIDOf(result), err
	}

	// No delivery within the threshold: durably suspend (the gate STAYS open —
	// a parked gate is the normal long-lived shape). Does not return on success.
	c.suspendForRecv(wfState, input.Topic, result.delayUntil)

	// Suspension failed (e.g. concurrent cancellation): wait in-process,
	// re-entering with the same step IDs.
	input.suspendThreshold = 0
	input.stepID = &result.stepID
	input.sleepStepID = &result.sleepStepID
	result, err = retryWithResult(c, func() (*recvResult, error) {
		return c.systemDB.recv(c, input)
	}, recvRetryOpts...)
	return result, deliveryIDOf(result), err
}

func deliveryIDOf(r *recvResult) string {
	if r == nil {
		return ""
	}
	return r.deliveryID
}

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

// DeliverToGate atomically verifies the gate is open and unexpired, matches
// the caller's principals against the stored audience, records the delivery
// with its outcome, and — only when delivered — signals the waiting workflow.
// Rejections commit their audit row and signal nothing. The outcome is a
// value, not an error: callers map it to their transport (403/409/204).
func DeliverToGate(ctx DBOSContext, in DeliverInput) (GateOutcome, string, error) {
	if ctx == nil {
		return "", "", errors.New("ctx cannot be nil")
	}
	return ctx.DeliverToGate(ctx, in)
}

func (c *dbosContext) DeliverToGate(_ DBOSContext, in DeliverInput) (GateOutcome, string, error) {
	if in.WorkflowID == "" || in.Gate == "" || in.Subject == "" {
		return "", "", errors.New("workflow ID, gate and subject are required")
	}
	encoded, err := resolveEncoder(c).Encode(in.Payload)
	if err != nil {
		return "", "", fmt.Errorf("failed to serialize payload: %w", err)
	}
	type outcomePair struct {
		outcome    GateOutcome
		deliveryID string
	}
	res, err := retryWithResult(c, func() (outcomePair, error) {
		o, id, err := c.systemDB.deliverToGate(c, in, encoded, resolveEncoder(c).Name())
		return outcomePair{o, id}, err
	}, withRetrierLogger(c.logger))
	return res.outcome, res.deliveryID, err
}

// IgnoreDelivery marks a delivered audit row as ignored by workflow policy
// (ADR 0012 D6). Idempotent; at-least-once semantics are fine — an already
// ignored row stays ignored.
func IgnoreDelivery(ctx DBOSContext, deliveryID string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.IgnoreDelivery(ctx, deliveryID)
}

func (c *dbosContext) IgnoreDelivery(_ DBOSContext, deliveryID string) error {
	if deliveryID == "" {
		return nil
	}
	return retry(c, func() error {
		return c.systemDB.ignoreDelivery(c, deliveryID)
	}, withRetrierLogger(c.logger))
}

/* ------------------------- system database side ------------------------- */

// openGate upserts the gate row and replaces its audience. Idempotent: crash
// between open and the first recv checkpoint re-runs it harmlessly.
func (s *sysDB) openGate(ctx context.Context, workflowID string, recvStepID int, g gateSpec) error {
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	now := time.Now().UnixMilli()
	var expires *int64
	if !g.ExpiresAt.IsZero() {
		v := g.ExpiresAt.UnixMilli()
		expires = &v
	}
	upsert := s.renderSQL(`INSERT INTO %sworkflow_gates
		(workflow_uuid, gate, org, open, expires_at_epoch_ms, opened_at_epoch_ms, closed_at_epoch_ms, recv_step_id)
		VALUES ($1, $2, $3, true, $4, $5, NULL, $6)
		ON CONFLICT (workflow_uuid, gate) DO UPDATE SET
			org = EXCLUDED.org, open = true, expires_at_epoch_ms = EXCLUDED.expires_at_epoch_ms,
			opened_at_epoch_ms = EXCLUDED.opened_at_epoch_ms, closed_at_epoch_ms = NULL,
			recv_step_id = EXCLUDED.recv_step_id`, s.dialect.SchemaPrefix(s.schema))
	if _, err := tx.Exec(ctx, upsert, workflowID, g.Name, g.Org, expires, now, recvStepID); err != nil {
		return fmt.Errorf("failed to upsert gate: %w", err)
	}
	del := s.renderSQL(`DELETE FROM %sworkflow_gate_audience WHERE workflow_uuid = $1 AND gate = $2`, s.dialect.SchemaPrefix(s.schema))
	if _, err := tx.Exec(ctx, del, workflowID, g.Name); err != nil {
		return fmt.Errorf("failed to clear gate audience: %w", err)
	}
	ins := s.renderSQL(`INSERT INTO %sworkflow_gate_audience (workflow_uuid, gate, principal_type, principal, org)
		VALUES ($1, $2, $3, $4, $5)`, s.dialect.SchemaPrefix(s.schema))
	for _, p := range g.Audience {
		if _, err := tx.Exec(ctx, ins, workflowID, g.Name, p.Type, p.Principal, g.Org); err != nil {
			return fmt.Errorf("failed to insert gate audience row: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// closeGate marks the gate closed inside the recv's checkpoint transaction and
// resolves the consumed notification to its delivery audit row.
func (s *sysDB) closeGate(ctx context.Context, tx Tx, workflowID, gate string, messageUUID *string) (string, error) {
	now := time.Now().UnixMilli()
	upd := s.renderSQL(`UPDATE %sworkflow_gates SET open = false, closed_at_epoch_ms = $1
		WHERE workflow_uuid = $2 AND gate = $3`, s.dialect.SchemaPrefix(s.schema))
	if _, err := tx.Exec(ctx, upd, now, workflowID, gate); err != nil {
		return "", fmt.Errorf("failed to close gate: %w", err)
	}
	if messageUUID == nil {
		return "", nil
	}
	var deliveryID string
	q := s.renderSQL(`SELECT delivery_uuid FROM %sworkflow_gate_deliveries WHERE message_uuid = $1`, s.dialect.SchemaPrefix(s.schema))
	if err := tx.QueryRow(ctx, q, *messageUUID).Scan(&deliveryID); err != nil {
		// A plain Send on the gate topic has no audit row; tolerate during the
		// transition from the event-based implementation.
		return "", nil
	}
	return deliveryID, nil
}

func (s *sysDB) deliverToGate(ctx context.Context, in DeliverInput, encodedPayload *string, serialization string) (GateOutcome, string, error) {
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	now := time.Now().UnixMilli()

	// The workflow must exist (404 material, not an audit row).
	var wfStatus WorkflowStatusType
	q := s.renderSQL(`SELECT status FROM %sworkflow_status WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))
	if err := tx.QueryRow(ctx, q, in.WorkflowID).Scan(&wfStatus); err != nil {
		return "", "", newNonExistentWorkflowError(in.WorkflowID)
	}

	outcome := GateDelivered
	var open bool
	var expires *int64
	q = s.renderSQL(`SELECT open, expires_at_epoch_ms FROM %sworkflow_gates
		WHERE workflow_uuid = $1 AND gate = $2`, s.dialect.SchemaPrefix(s.schema))
	err = tx.QueryRow(ctx, q, in.WorkflowID, in.Gate).Scan(&open, &expires)
	switch {
	case err != nil: // no gate row: never opened (or pre-primitive workflow)
		outcome = GateRejectedClosed
	case !open, expires != nil && *expires <= now:
		outcome = GateRejectedClosed
	case wfStatus != WorkflowStatusPending && wfStatus != WorkflowStatusEnqueued && wfStatus != WorkflowStatusDelayed:
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
		if err := s.send(ctx, WorkflowSendInput{
			DestinationID: in.WorkflowID,
			Message:       encodedPayload,
			Topic:         gateTopic(in.Gate),
			serialization: serialization,
			tx:            tx,
			messageUUID:   messageUUID,
		}); err != nil {
			return "", "", fmt.Errorf("failed to signal gate delivery: %w", err)
		}
	}

	digest := sha256.Sum256([]byte(strOrEmpty(encodedPayload)))
	ins := s.renderSQL(`INSERT INTO %sworkflow_gate_deliveries
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
func (s *sysDB) audienceMatches(ctx context.Context, tx Tx, in DeliverInput) (bool, error) {
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
	q := s.renderSQL(`SELECT EXISTS (SELECT 1 FROM %sworkflow_gate_audience
		WHERE workflow_uuid = $1 AND gate = $2 AND (
			principal_type = $3
			OR (principal_type = $4 AND principal = $5)
			OR (principal_type = $6 AND `+groupClause+`)
		))`, s.dialect.SchemaPrefix(s.schema))
	var match bool
	if err := tx.QueryRow(ctx, q, args...).Scan(&match); err != nil {
		return false, fmt.Errorf("failed to match gate audience: %w", err)
	}
	return match, nil
}

func (s *sysDB) ignoreDelivery(ctx context.Context, deliveryID string) error {
	q := s.renderSQL(`UPDATE %sworkflow_gate_deliveries SET outcome = $1
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
