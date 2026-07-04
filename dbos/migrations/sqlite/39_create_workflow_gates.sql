-- Migration 39: gates as a runtime primitive (see the pg variant for docs).

CREATE TABLE IF NOT EXISTS workflow_gates (
    workflow_uuid       TEXT NOT NULL,
    gate                TEXT NOT NULL,
    org                 TEXT,
    open                INTEGER NOT NULL,
    expires_at_epoch_ms BIGINT,
    opened_at_epoch_ms  BIGINT NOT NULL,
    closed_at_epoch_ms  BIGINT,
    recv_step_id        INTEGER,
    PRIMARY KEY (workflow_uuid, gate),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status (workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_workflow_gates_open ON workflow_gates (org) WHERE open;

CREATE TABLE IF NOT EXISTS workflow_gate_audience (
    workflow_uuid  TEXT NOT NULL,
    gate           TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    principal      TEXT NOT NULL,
    org            TEXT,
    PRIMARY KEY (workflow_uuid, gate, principal_type, principal),
    FOREIGN KEY (workflow_uuid, gate) REFERENCES workflow_gates (workflow_uuid, gate)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_gate_audience_principal ON workflow_gate_audience (org, principal_type, principal);

CREATE TABLE IF NOT EXISTS workflow_gate_deliveries (
    delivery_uuid       TEXT PRIMARY KEY,
    workflow_uuid       TEXT NOT NULL,
    gate                TEXT NOT NULL,
    by_subject          TEXT NOT NULL,
    by_org              TEXT,
    claims              TEXT,
    payload_digest      TEXT,
    message_uuid        TEXT,
    outcome             TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL,
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status (workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_gate_deliveries_by ON workflow_gate_deliveries (by_org, by_subject, created_at_epoch_ms);
