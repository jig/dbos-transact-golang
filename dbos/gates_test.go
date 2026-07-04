package dbos

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// gateRowState reads the authoritative gate row.
func gateRowState(t *testing.T, c DBOSContext, wfID, gate string) (open bool, found bool) {
	t.Helper()
	s := c.(*dbosContext).systemDB.concrete()
	q := s.renderSQL(`SELECT open FROM %sworkflow_gates WHERE workflow_uuid = $1 AND gate = $2`, s.dialect.SchemaPrefix(s.schema))
	err := s.pool.QueryRow(context.Background(), q, wfID, gate).Scan(&open)
	if err != nil {
		return false, false
	}
	return open, true
}

func deliveryOutcome(t *testing.T, c DBOSContext, deliveryID string) string {
	t.Helper()
	s := c.(*dbosContext).systemDB.concrete()
	q := s.renderSQL(`SELECT outcome FROM %sworkflow_gate_deliveries WHERE delivery_uuid = $1`, s.dialect.SchemaPrefix(s.schema))
	var outcome string
	require.NoError(t, s.pool.QueryRow(context.Background(), q, deliveryID).Scan(&outcome))
	return outcome
}

func waitGateOpenRow(t *testing.T, c DBOSContext, wfID, gate string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if open, found := gateRowState(t, c, wfID, gate); found && open {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("gate %s/%s never opened", wfID, gate)
}

// TestGatePrimitive drives the fluxos8 ADR 0012 semantics end to end:
// authoritative open/close, conditional atomic delivery with audited
// outcomes, and the D6 ignore mark.
func TestGatePrimitive(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})

	gateWF := func(ctx DBOSContext, _ string) (string, error) {
		payload, deliveryID, err := GateRecv[string](ctx, GateRecvInput{
			Gate: "approve", Org: "org-1",
			Audience: []GatePrincipal{{Type: GatePrincipalGroup, Principal: "approvers"}},
			Timeout:  10 * time.Second,
		})
		if err != nil {
			return "", err
		}
		return payload + "|" + deliveryID, nil
	}
	timeoutWF := func(ctx DBOSContext, _ string) (string, error) {
		_, deliveryID, err := GateRecv[string](ctx, GateRecvInput{
			Gate: "approve", Org: "org-1",
			Audience: []GatePrincipal{{Type: GatePrincipalUser, Principal: "u1"}},
			Timeout:  300 * time.Millisecond,
		})
		var dbosErr *DBOSError
		if errors.As(err, &dbosErr) && dbosErr.Code == TimeoutError {
			return "timed-out|" + deliveryID, nil
		}
		return "", fmt.Errorf("expected timeout, got payload (err=%v)", err)
	}
	RegisterWorkflow(dbosCtx, gateWF)
	RegisterWorkflow(dbosCtx, timeoutWF)
	require.NoError(t, Launch(dbosCtx))

	deliver := func(wfID, subject string, groups []string) (GateOutcome, string, error) {
		return DeliverToGate(dbosCtx, DeliverInput{
			WorkflowID: wfID, Gate: "approve",
			Subject: subject, Org: "org-1", Groups: groups,
			ClaimsJSON: `{"subject":"` + subject + `"}`, Payload: "hello",
		})
	}

	t.Run("unknown workflow is an error, not an audit row", func(t *testing.T) {
		_, _, err := deliver("no-such-wf", "u2", []string{"approvers"})
		require.Error(t, err)
	})

	t.Run("full lifecycle", func(t *testing.T) {
		h, err := RunWorkflow(dbosCtx, gateWF, "", WithWorkflowID("gate-wf-1"))
		require.NoError(t, err)
		waitGateOpenRow(t, dbosCtx, "gate-wf-1", "approve")

		// Audience miss: atomically rejected, audited, gate untouched.
		outcome, missID, err := deliver("gate-wf-1", "u9", nil)
		require.NoError(t, err)
		require.Equal(t, GateRejectedAudience, outcome)
		require.Equal(t, string(GateRejectedAudience), deliveryOutcome(t, dbosCtx, missID))
		if open, found := gateRowState(t, dbosCtx, "gate-wf-1", "approve"); !found || !open {
			t.Fatal("audience miss must not close the gate")
		}

		// Eligible delivery: delivered, workflow resumes with the SAME delivery ID.
		outcome, delID, err := deliver("gate-wf-1", "u2", []string{"approvers"})
		require.NoError(t, err)
		require.Equal(t, GateDelivered, outcome)
		res, err := h.GetResult()
		require.NoError(t, err)
		require.Equal(t, "hello|"+delID, res)
		require.Equal(t, string(GateDelivered), deliveryOutcome(t, dbosCtx, delID))

		// The gate closed in the recv's checkpoint tx: late delivery rejects.
		if open, found := gateRowState(t, dbosCtx, "gate-wf-1", "approve"); !found || open {
			t.Fatal("gate must be closed after consumption")
		}
		outcome, lateID, err := deliver("gate-wf-1", "u2", []string{"approvers"})
		require.NoError(t, err)
		require.Equal(t, GateRejectedClosed, outcome)
		require.Equal(t, string(GateRejectedClosed), deliveryOutcome(t, dbosCtx, lateID))

		// D6: workflow policy marks a delivered row ignored (idempotent).
		require.NoError(t, IgnoreDelivery(dbosCtx, delID))
		require.NoError(t, IgnoreDelivery(dbosCtx, delID))
		require.Equal(t, string(GateIgnored), deliveryOutcome(t, dbosCtx, delID))
	})

	t.Run("timeout closes the gate", func(t *testing.T) {
		h, err := RunWorkflow(dbosCtx, timeoutWF, "", WithWorkflowID("gate-wf-2"))
		require.NoError(t, err)
		res, err := h.GetResult()
		require.NoError(t, err)
		require.Equal(t, "timed-out|", res) // no delivery ID on timeout
		if open, found := gateRowState(t, dbosCtx, "gate-wf-2", "approve"); !found || open {
			t.Fatal("gate must be closed after timeout")
		}
		outcome, _, err := deliver("gate-wf-2", "u1", nil)
		require.NoError(t, err)
		require.Equal(t, GateRejectedClosed, outcome)
	})
}
