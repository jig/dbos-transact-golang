package dbos

// Gates as a runtime primitive (fluxos8 ADR 0012, design in
// notes/GATES-DESIGN.md). A gate is a recv with authoritative, transactional
// state: opening joins the recv's pre-wait bookkeeping, closing joins the
// recv's checkpoint transaction, and delivery is conditional and atomic —
// verify open, verify audience, append the audit row and signal the waiter in
// one transaction. Audiences are stored symbolically (group rows carry the
// name, never expanded members); the caller expands ITS memberships at
// delivery time via its own groups provider. The SQL side lives in
// internal/sysdb/gates.go; this file is the public API.

import (
	"errors"
	"fmt"
	"time"

	"github.com/jig/dbos-transact-golang/dbos/internal/models"
	"github.com/jig/dbos-transact-golang/dbos/internal/sysdb"
)

// Gate principal types stored in workflow_gate_audience.
const (
	GatePrincipalUser   = sysdb.GatePrincipalUser
	GatePrincipalGroup  = sysdb.GatePrincipalGroup
	GatePrincipalAll    = sysdb.GatePrincipalAll
	GatePrincipalExcept = sysdb.GatePrincipalExcept // categorical exclusion (e.g. the initiator); data, not policy (ADR 0012 D6)
)

// GatePrincipal is one symbolic audience row.
type GatePrincipal = sysdb.GatePrincipal

// GateOutcome is the durable result of a delivery attempt.
type GateOutcome = sysdb.GateOutcome

const (
	GateDelivered        = sysdb.GateDelivered
	GateRejectedClosed   = sysdb.GateRejectedClosed
	GateRejectedAudience = sysdb.GateRejectedAudience
	GateIgnored          = sysdb.GateIgnored
)

// gateTopic is the notification topic a gate's deliveries travel on.
func gateTopic(name string) string { return sysdb.GateTopic(name) }

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
func GateRecv[T any](ctx Context, in GateRecvInput) (T, string, error) {
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
func convertRecvResult[R any](ctx Context, msg any) (R, error) {
	var zero R
	if msg == nil {
		return zero, nil
	}
	if _, ok := ctx.(*dbosContext); !ok { // mocked path
		typed, ok := msg.(R)
		if !ok {
			workflowID, _ := GetWorkflowID(ctx)
			return zero, models.NewWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", msg))
		}
		return typed, nil
	}
	result, ok := msg.(*recvResult)
	if !ok {
		workflowID, _ := GetWorkflowID(ctx)
		return zero, models.NewWorkflowUnexpectedResultType(workflowID, "*recvResult", fmt.Sprintf("%T", msg))
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

func (c *dbosContext) GateRecv(_ Context, in GateRecvInput) (any, string, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, "", models.NewStepExecutionError("", "DBOS.recv", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}
	if wfState.isWithinStep {
		return nil, "", models.NewStepExecutionError(wfState.workflowID, "DBOS.recv", fmt.Errorf("cannot call GateRecv within a step"))
	}
	if in.Gate == "" {
		return nil, "", errors.New("gate name cannot be empty")
	}
	// The recv step ID precedes its internal timeout sleep's; both are allocated
	// up front so the recorded layout matches Recv's (replay-compatible).
	stepID := wfState.nextStepID()
	sleepStepID := wfState.nextStepID()
	gate := &sysdb.GateSpec{Name: in.Gate, Org: in.Org, Audience: in.Audience, ExpiresAt: in.Expires}
	result, err := c.recvWithGate(wfState, wfState.workflowID, gateTopic(in.Gate), in.Timeout, stepID, sleepStepID, gate)
	if err != nil || result == nil {
		return result, "", err
	}
	rr, ok := result.(*recvResult)
	if !ok {
		return result, "", err
	}
	return rr, rr.deliveryID, nil
}

// DeliverInput is one delivery attempt against a gate.
type DeliverInput = sysdb.DeliverInput

// DeliverToGate atomically verifies the gate is open and unexpired, matches
// the caller's principals against the stored audience, records the delivery
// with its outcome, and — only when delivered — signals the waiting workflow.
// Rejections commit their audit row and signal nothing. The outcome is a
// value, not an error: callers map it to their transport (403/409/204).
func DeliverToGate(ctx Context, in DeliverInput) (GateOutcome, string, error) {
	if ctx == nil {
		return "", "", errors.New("ctx cannot be nil")
	}
	return ctx.DeliverToGate(ctx, in)
}

func (c *dbosContext) DeliverToGate(_ Context, in DeliverInput) (GateOutcome, string, error) {
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
	res, err := sysdb.RetryWithResult(c, func() (outcomePair, error) {
		o, id, err := c.systemDB.DeliverToGate(c, in, encoded, resolveEncoder(c).Name())
		return outcomePair{o, id}, err
	}, sysdb.WithRetrierLogger(c.logger))
	return res.outcome, res.deliveryID, err
}

// IgnoreDelivery marks a delivered audit row as ignored by workflow policy
// (ADR 0012 D6). Idempotent; at-least-once semantics are fine — an already
// ignored row stays ignored.
func IgnoreDelivery(ctx Context, deliveryID string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.IgnoreDelivery(ctx, deliveryID)
}

func (c *dbosContext) IgnoreDelivery(_ Context, deliveryID string) error {
	if deliveryID == "" {
		return nil
	}
	return sysdb.Retry(c, func() error {
		return c.systemDB.IgnoreDelivery(c, deliveryID)
	}, sysdb.WithRetrierLogger(c.logger))
}
