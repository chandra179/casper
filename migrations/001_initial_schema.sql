CREATE TABLE IF NOT EXISTS tasks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type       VARCHAR(255) NOT NULL,
    tenant_id       VARCHAR(64) NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    priority        INT NOT NULL DEFAULT 0,
    scheduled_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    max_retries     INT NOT NULL DEFAULT 3,
    retry_count     INT NOT NULL DEFAULT 0,
    version         BIGINT NOT NULL DEFAULT 0,
    claimed_by      VARCHAR(255),
    claimed_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    error_message   TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tasks_poll ON tasks (status, scheduled_at, priority DESC)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_tasks_tenant ON tasks (tenant_id, status, scheduled_at)
    WHERE status = 'PENDING';

CREATE TABLE IF NOT EXISTS processed_tasks (
    task_id     UUID PRIMARY KEY,
    worker_id   VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
