CREATE TABLE workflow_waiters (
    waiter_workflow_uuid TEXT NOT NULL,
    awaited_workflow_uuid TEXT NOT NULL,
    PRIMARY KEY (waiter_workflow_uuid, awaited_workflow_uuid)
);
CREATE INDEX idx_workflow_waiters_awaited ON workflow_waiters (awaited_workflow_uuid);
