-- MCP tasks that goclaw watches after a modern tools/call returned a task,
-- and the wake records that tell an ACP client when a task needs attention.

CREATE TABLE IF NOT EXISTS mcp_tasks (
    id             UUID PRIMARY KEY,
    tenant_id      UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    agent_id       UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    server_id      UUID NOT NULL REFERENCES mcp_servers(id) ON DELETE CASCADE,
    task_id        TEXT NOT NULL,
    session_key    TEXT NOT NULL DEFAULT '',
    acp_session_id TEXT NOT NULL DEFAULT '',
    user_id        TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL,
    status_message TEXT,
    event_cursor   TEXT,
    request_caps   JSONB NOT NULL DEFAULT '{}',
    thread_context JSONB,
    answered_keys  JSONB NOT NULL DEFAULT '[]',
    pr_woken       BOOLEAN NOT NULL DEFAULT false,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    terminal_at    TIMESTAMPTZ,
    expires_at     TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, server_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_mcp_tasks_open
    ON mcp_tasks (expires_at) WHERE terminal_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_mcp_tasks_tenant_agent
    ON mcp_tasks (tenant_id, agent_id, task_id);

CREATE TABLE IF NOT EXISTS mcp_task_wakes (
    wake_id         TEXT PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    task_row_id     UUID NOT NULL REFERENCES mcp_tasks(id) ON DELETE CASCADE,
    thread_key      TEXT NOT NULL,
    reason          TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending',
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_mcp_task_wakes_due
    ON mcp_task_wakes (next_attempt_at) WHERE state IN ('pending', 'no_sink');
CREATE INDEX IF NOT EXISTS idx_mcp_task_wakes_task
    ON mcp_task_wakes (task_row_id);
CREATE INDEX IF NOT EXISTS idx_mcp_task_wakes_thread
    ON mcp_task_wakes (tenant_id, thread_key, delivered_at);
