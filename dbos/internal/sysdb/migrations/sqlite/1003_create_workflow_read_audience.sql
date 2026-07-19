-- Migration 40: per-instance read audience (fluxos8 ADR 0013). Symbolic rows
-- like gate audiences; principal_type additionally admits 'initiator' (the
-- caller that started the instance - also serves the "my workflows" query).
-- The read check is the union of these rows with the instance's gate-audience
-- rows (whoever may act must be able to see).

CREATE TABLE IF NOT EXISTS workflow_read_audience (
    workflow_uuid  TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    principal      TEXT NOT NULL,
    org            TEXT,
    PRIMARY KEY (workflow_uuid, principal_type, principal),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status (workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_read_audience_principal ON workflow_read_audience (org, principal_type, principal);
