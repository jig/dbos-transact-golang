# Gates as a runtime primitive — fork-side design (fluxos8 ADR 0012)

Implements fluxos8's ADR 0012 D1–D4 in this fork: gate open/deliver/close as
transactional runtime state, replacing the event+Send convention. D5 (inbox
queries) only needs the schema below; the queries land with fluxos8's M4.

## Schema (migration 39, pg + sqlite)

```sql
CREATE TABLE workflow_gates (
    workflow_uuid       TEXT NOT NULL,
    gate                TEXT NOT NULL,
    org                 TEXT,              -- owning organisation (workflow's authenticated_user)
    open                BOOLEAN NOT NULL,
    expires_at_epoch_ms BIGINT,            -- NULL = no deadline
    opened_at_epoch_ms  BIGINT NOT NULL,
    closed_at_epoch_ms  BIGINT,
    recv_step_id        INTEGER,           -- recv step of the CURRENT open (re-opens upsert)
    PRIMARY KEY (workflow_uuid, gate),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status ON DELETE CASCADE
);
CREATE INDEX idx_workflow_gates_open ON workflow_gates (org) WHERE open;

CREATE TABLE workflow_gate_audience (
    workflow_uuid  TEXT NOT NULL,
    gate           TEXT NOT NULL,
    principal_type TEXT NOT NULL,          -- 'user' | 'group' | 'all'
    principal      TEXT NOT NULL,          -- subid | group name | '*'
    org            TEXT,
    PRIMARY KEY (workflow_uuid, gate, principal_type, principal),
    FOREIGN KEY (workflow_uuid, gate) REFERENCES workflow_gates ON DELETE CASCADE
);
-- inbox: principal -> open gates; the PK covers gate -> principals
CREATE INDEX idx_gate_audience_principal ON workflow_gate_audience (org, principal_type, principal);

CREATE TABLE workflow_gate_deliveries (
    delivery_uuid       TEXT PRIMARY KEY,
    workflow_uuid       TEXT NOT NULL,
    gate                TEXT NOT NULL,
    by_subject          TEXT NOT NULL,
    by_org              TEXT,
    claims              TEXT,              -- serialized validated claims (audit)
    payload_digest      TEXT,              -- sha256 hex; payload itself travels in the notification
    outcome             TEXT NOT NULL,     -- delivered | rejected-closed | rejected-audience | ignored
    created_at_epoch_ms BIGINT NOT NULL,
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status ON DELETE CASCADE
);
CREATE INDEX idx_gate_deliveries_by ON workflow_gate_deliveries (by_org, by_subject, created_at_epoch_ms);
```

Audiences stay **symbolic** (`group:` rows are names, never expanded members);
the caller's memberships are expanded at delivery/query time (ADR 0012 D5).

## Runtime API

```go
type GatePrincipal struct{ Type, Principal string } // 'user'|'group'|'all'

type GateRecvInput struct {
    Gate     string
    Org      string
    Audience []GatePrincipal // empty = closed to everyone
    Deadline time.Time       // zero = no deadline
}
// GateRecv = recv with gate bookkeeping. Returns the payload and deliveryID.
func GateRecv[T any](ctx DBOSContext, in GateRecvInput) (T, string, error)

type DeliverInput struct {
    WorkflowID string
    Gate       string
    Subject    string   // validated caller
    Org        string
    Groups     []string // caller's memberships, resolved by the CALLER's Groups provider
    Claims     any      // serialized into the audit row
    Payload    any
}
type GateOutcome string // GateDelivered | GateRejectedClosed | GateRejectedAudience
func DeliverToGate(ctx DBOSContext, in DeliverInput) (GateOutcome, string, error)

// IgnoreDelivery marks a delivered row 'ignored' (workflow policy discarded it,
// ADR 0012 D6). Idempotent, at-least-once.
func IgnoreDelivery(ctx DBOSContext, deliveryID string) error
```

Transactionality:
- **Open**: gate + audience rows upserted in the same tx as the recv's durable
  deadline record (the first checkpoint the recv writes). Re-opens (loop
  iterations) upsert the same PK.
- **Deliver**: one tx — lock gate row; check open && not expired; match caller
  principals (user / any group name / `*`) against audience rows; insert the
  delivery row with its outcome; only when `delivered`: insert the
  notification and wake the waiter (existing send machinery). Rejections
  commit their audit row atomically and signal nothing.
- **Close**: same tx as the recv's recordOperationResult (message consumed or
  timeout). Suspension does NOT close (a parked gate stays open — that is the
  normal long-lived shape).

## Replay compatibility (the transition constraint)

Live histories today contain, per Await: `gate-now` (step), `DBOS.setEvent`
(gate open), `DBOS.recv` + `DBOS.sleep`, `DBOS.setEvent` (gate close).

- GateRecv records its recv under the SAME step name/shape (`DBOS.recv` +
  `DBOS.sleep`, two pre-reserved IDs) so recorded histories replay unchanged.
- The notification envelope gains `deliveryId`; old recorded recv outputs
  simply lack the field (JSON-compatible), so decoding is backward-safe.
- fluxos8's `operator.Await` KEEPS the `gate-now` step and both `SetEvent`
  steps during the transition (they are recorded in live histories; removing
  them shifts step IDs → UnexpectedStep). They become dead weight retired
  later via `Patch` once pre-primitive instances drain. Gate rows are NOT
  steps, so adding them costs no history position.

## Decided (ADR 0012 open questions)

- Gate rows are **checkpoint-transaction only** for now; joining the combined
  Persist tx (ADR 0002) is deferred until a workflow needs a domain write
  atomic with a gate transition.
- Delivery audit stores the **digest** of the payload plus serialized claims;
  the payload itself only travels in the (consumable) notification. PII stays
  out of the audit table.
- Inbox pagination/ordering: deferred to fluxos8 M4 (queries, not schema).
