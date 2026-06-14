CREATE TABLE IF NOT EXISTS workflow_waiters (
    waiter_workflow_uuid TEXT NOT NULL,
    awaited_workflow_uuid TEXT NOT NULL,
    PRIMARY KEY (waiter_workflow_uuid, awaited_workflow_uuid)
);
CREATE INDEX IF NOT EXISTS idx_workflow_waiters_awaited ON workflow_waiters (awaited_workflow_uuid);
