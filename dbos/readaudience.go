package dbos

// Per-instance read audience and the inbox projection queries (fluxos8
// ADR 0013 / ADR 0012 D5). The read check is the union of the instance's
// read-audience rows with its gate-audience rows: whoever may act must be
// able to see what they are acting on. All queries are org-scoped and match
// the caller's expanded principals against symbolic rows, exactly like
// DeliverToGate.

import (
	"errors"

	"github.com/jig/dbos-transact-golang/dbos/internal/sysdb"
)

// GatePrincipalInitiator marks the instance's starter in the read-audience
// rows; it also serves the "my workflows" reverse query.
const GatePrincipalInitiator = sysdb.GatePrincipalInitiator

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
	return sysdb.Retry(c, func() error {
		return c.systemDB.AddReadAudience(c, workflowID, org, principals)
	}, sysdb.WithRetrierLogger(c.logger))
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
	return sysdb.RetryWithResult(c, func() (bool, error) {
		return c.systemDB.ReadAllowed(c, workflowID, org, subject, groups)
	}, sysdb.WithRetrierLogger(c.logger))
}

// OpenGateRow is one "waiting on me" inbox entry.
type OpenGateRow = sysdb.OpenGateRow

// ListOpenGatesFor returns the open, unexpired gates whose audience admits the
// caller (org-scoped; exclusions honored) — "waiting on me" (ADR 0012 D5).
func ListOpenGatesFor(ctx DBOSContext, org, subject string, groups []string, limit int) ([]OpenGateRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListOpenGatesFor(ctx, org, subject, groups, limit)
}

func (c *dbosContext) ListOpenGatesFor(_ DBOSContext, org, subject string, groups []string, limit int) ([]OpenGateRow, error) {
	return sysdb.RetryWithResult(c, func() ([]OpenGateRow, error) {
		return c.systemDB.ListOpenGatesFor(c, org, subject, groups, limit)
	}, sysdb.WithRetrierLogger(c.logger))
}

// DeliveryRow is one "recent decisions" entry: a delivery attempt by the
// caller and its durable outcome.
type DeliveryRow = sysdb.DeliveryRow

// ListDeliveriesBy returns the caller's delivery attempts, newest first.
func ListDeliveriesBy(ctx DBOSContext, org, subject string, limit int) ([]DeliveryRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListDeliveriesBy(ctx, org, subject, limit)
}

func (c *dbosContext) ListDeliveriesBy(_ DBOSContext, org, subject string, limit int) ([]DeliveryRow, error) {
	return sysdb.RetryWithResult(c, func() ([]DeliveryRow, error) {
		return c.systemDB.ListDeliveriesBy(c, org, subject, limit)
	}, sysdb.WithRetrierLogger(c.logger))
}

// ListDeliveriesFor returns one instance's delivery audit, oldest first.
func ListDeliveriesFor(ctx DBOSContext, workflowID string, limit int) ([]DeliveryRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListDeliveriesFor(ctx, workflowID, limit)
}

func (c *dbosContext) ListDeliveriesFor(_ DBOSContext, workflowID string, limit int) ([]DeliveryRow, error) {
	return sysdb.RetryWithResult(c, func() ([]DeliveryRow, error) {
		return c.systemDB.ListDeliveriesFor(c, workflowID, limit)
	}, sysdb.WithRetrierLogger(c.logger))
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
	return sysdb.RetryWithResult(c, func() ([]string, error) {
		return c.systemDB.ListInitiatedBy(c, org, subject, limit)
	}, sysdb.WithRetrierLogger(c.logger))
}
