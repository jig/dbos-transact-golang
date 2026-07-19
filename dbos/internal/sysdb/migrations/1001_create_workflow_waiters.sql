-- Migration 1001 (fork; high number avoids upstream collision): Create the workflow_waiters table. A row (waiter, awaited)
-- records that workflow `waiter` is suspended (status DELAYED) until workflow
-- `awaited` reaches a terminal state. Completion of the awaited workflow wakes
-- its waiters by moving their delay_until to now and deleting the rows.

CREATE TABLE IF NOT EXISTS %s."workflow_waiters" (
    waiter_workflow_uuid TEXT NOT NULL,
    awaited_workflow_uuid TEXT NOT NULL,
    PRIMARY KEY (waiter_workflow_uuid, awaited_workflow_uuid)
);
CREATE INDEX IF NOT EXISTS "idx_workflow_waiters_awaited" ON %s."workflow_waiters" ("awaited_workflow_uuid");
