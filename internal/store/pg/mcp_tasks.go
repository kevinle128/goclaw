package pg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// PGMCPTaskStore implements store.MCPTaskStore with PostgreSQL.
type PGMCPTaskStore struct {
	db *sql.DB
}

// NewPGMCPTaskStore creates a PostgreSQL MCP task store.
func NewPGMCPTaskStore(db *sql.DB) *PGMCPTaskStore {
	return &PGMCPTaskStore{db: db}
}

const mcpTaskSelectCols = `t.id, t.tenant_id, t.agent_id, t.server_id, t.task_id, t.session_key,
	t.acp_session_id, t.user_id, t.status, t.status_message, t.event_cursor, t.request_caps,
	t.thread_context, t.answered_keys, t.pr_woken, t.created_at, t.updated_at, t.terminal_at,
	t.expires_at`

const mcpWakeSelectCols = `w.wake_id, w.tenant_id, w.task_row_id, w.thread_key, w.reason, w.state,
	w.attempts, w.next_attempt_at, w.created_at, w.delivered_at`

// pgUniqueViolation is the SQLSTATE of a unique constraint violation.
const pgUniqueViolation = "23505"

func scanMCPTask(row interface{ Scan(...any) error }) (*store.MCPTask, error) {
	var t store.MCPTask
	var caps, thread, answered []byte
	if err := row.Scan(
		&t.ID, &t.TenantID, &t.AgentID, &t.ServerID, &t.TaskID, &t.SessionKey,
		&t.ACPSessionID, &t.UserID, &t.Status, &t.StatusMessage, &t.EventCursor, &caps,
		&thread, &answered, &t.PRWoken, &t.CreatedAt, &t.UpdatedAt, &t.TerminalAt,
		&t.ExpiresAt,
	); err != nil {
		return nil, err
	}
	fillMCPTaskJSON(&t, caps, thread, answered)
	return &t, nil
}

// fillMCPTaskJSON decodes the JSON columns of a task row.
func fillMCPTaskJSON(t *store.MCPTask, caps, thread, answered []byte) {
	if len(caps) > 0 {
		t.RequestCaps = json.RawMessage(caps)
	}
	if len(thread) > 0 && string(thread) != "null" {
		t.ThreadContext = json.RawMessage(thread)
	}
	if len(answered) > 0 {
		_ = json.Unmarshal(answered, &t.AnsweredKeys)
	}
}

// CreateTask inserts a task row in the tenant of ctx.
func (s *PGMCPTaskStore) CreateTask(ctx context.Context, t *store.MCPTask) error {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	if t.ID == uuid.Nil {
		t.ID = store.GenNewID()
	}
	t.TenantID = tid
	now := time.Now().UTC()
	t.CreatedAt, t.UpdatedAt = now, now
	caps := t.RequestCaps
	if len(caps) == 0 {
		caps = json.RawMessage(`{}`)
	}
	answered, err := json.Marshal(nonNilStrings(t.AnsweredKeys))
	if err != nil {
		return fmt.Errorf("encode answered keys: %w", err)
	}
	var thread any
	if len(t.ThreadContext) > 0 {
		thread = []byte(t.ThreadContext)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO mcp_tasks
		(id, tenant_id, agent_id, server_id, task_id, session_key, acp_session_id, user_id,
		 status, status_message, event_cursor, request_caps, thread_context, answered_keys,
		 pr_woken, created_at, updated_at, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		t.ID, tid, t.AgentID, t.ServerID, t.TaskID, t.SessionKey, t.ACPSessionID, t.UserID,
		t.Status, t.StatusMessage, t.EventCursor, []byte(caps), thread, answered,
		t.PRWoken, now, now, t.ExpiresAt.UTC(),
	)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		return store.ErrMCPTaskExists
	}
	return err
}

// GetTask returns a task row of the tenant, or nil.
func (s *PGMCPTaskStore) GetTask(ctx context.Context, id uuid.UUID) (*store.MCPTask, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT `+mcpTaskSelectCols+` FROM mcp_tasks t WHERE t.id = $1 AND t.tenant_id = $2`, id, tid)
	t, err := scanMCPTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// GetTaskForAgent returns the newest task row of an agent with a server task id.
func (s *PGMCPTaskStore) GetTaskForAgent(ctx context.Context, agentID uuid.UUID, taskID string) (*store.MCPTask, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT `+mcpTaskSelectCols+` FROM mcp_tasks t
		 WHERE t.tenant_id = $1 AND t.agent_id = $2 AND t.task_id = $3
		 ORDER BY t.created_at DESC LIMIT 1`, tid, agentID, taskID)
	t, err := scanMCPTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// HasOpenTaskForAgent reports whether an agent has an open task row.
func (s *PGMCPTaskStore) HasOpenTaskForAgent(ctx context.Context, agentID uuid.UUID) (bool, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return false, err
	}
	var exists bool
	err = s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM mcp_tasks
		 WHERE tenant_id = $1 AND agent_id = $2 AND terminal_at IS NULL AND expires_at > NOW())`,
		tid, agentID).Scan(&exists)
	return exists, err
}

// ListOpenTasks returns the open task rows of all tenants (system read).
func (s *PGMCPTaskStore) ListOpenTasks(ctx context.Context) ([]store.MCPTask, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+mcpTaskSelectCols+` FROM mcp_tasks t
		 WHERE t.terminal_at IS NULL AND t.expires_at > NOW()
		 ORDER BY t.created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.MCPTask
	for rows.Next() {
		t, err := scanMCPTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

// UpdateTaskCursor stores a non-empty events cursor.
func (s *PGMCPTaskStore) UpdateTaskCursor(ctx context.Context, id uuid.UUID, cursor string) error {
	if cursor == "" {
		return nil
	}
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET event_cursor = $1, updated_at = NOW() WHERE id = $2 AND tenant_id = $3`,
		cursor, id, tid)
	return err
}

// UpdateTaskStatus stores the status from tasks/get.
func (s *PGMCPTaskStore) UpdateTaskStatus(ctx context.Context, id uuid.UUID, u store.MCPTaskStatusUpdate) error {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET status = $1, status_message = $2, updated_at = NOW(),
		 terminal_at = CASE WHEN $3 THEN COALESCE(terminal_at, NOW()) ELSE terminal_at END
		 WHERE id = $4 AND tenant_id = $5`,
		u.Status, u.StatusMessage, u.Terminal, id, tid)
	return err
}

// AddAnsweredKey adds key to answered_keys once.
func (s *PGMCPTaskStore) AddAnsweredKey(ctx context.Context, id uuid.UUID, key string) error {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET answered_keys = answered_keys || jsonb_build_array($1::text), updated_at = NOW()
		 WHERE id = $2 AND tenant_id = $3 AND NOT (answered_keys @> jsonb_build_array($1::text))`,
		key, id, tid)
	return err
}

// RemoveAnsweredKey removes key from answered_keys.
func (s *PGMCPTaskStore) RemoveAnsweredKey(ctx context.Context, id uuid.UUID, key string) error {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET answered_keys = answered_keys - $1::text, updated_at = NOW()
		 WHERE id = $2 AND tenant_id = $3`,
		key, id, tid)
	return err
}

// MarkPRWoken sets pr_woken and reports whether this call changed it.
func (s *PGMCPTaskStore) MarkPRWoken(ctx context.Context, id uuid.UUID) (bool, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET pr_woken = true, updated_at = NOW()
		 WHERE id = $1 AND tenant_id = $2 AND pr_woken = false`, id, tid)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// MarkTaskTerminal sets terminal_at once, without a status change.
func (s *PGMCPTaskStore) MarkTaskTerminal(ctx context.Context, id uuid.UUID) error {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET terminal_at = COALESCE(terminal_at, NOW()), updated_at = NOW()
		 WHERE id = $1 AND tenant_id = $2`, id, tid)
	return err
}

// CreateWake inserts a pending wake; an existing id is left unchanged.
func (s *PGMCPTaskStore) CreateWake(ctx context.Context, w *store.MCPTaskWake) (bool, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return false, err
	}
	now := time.Now().UTC()
	w.TenantID = tid
	if w.State == "" {
		w.State = store.MCPWakeStatePending
	}
	if w.NextAttemptAt.IsZero() {
		w.NextAttemptAt = now
	}
	w.CreatedAt = now
	// The task row must be in the same tenant: the INSERT ... SELECT finds
	// no row otherwise and inserts nothing.
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO mcp_task_wakes (wake_id, tenant_id, task_row_id, thread_key, reason, state, next_attempt_at, created_at)
		 SELECT $1, $2, t.id, $4, $5, $6, $7, $8 FROM mcp_tasks t WHERE t.id = $3 AND t.tenant_id = $2
		 ON CONFLICT (wake_id) DO NOTHING`,
		w.WakeID, tid, w.TaskRowID, w.ThreadKey, w.Reason, w.State, w.NextAttemptAt.UTC(), now)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func scanMCPWake(row interface{ Scan(...any) error }) (*store.MCPTaskWake, error) {
	var w store.MCPTaskWake
	if err := row.Scan(&w.WakeID, &w.TenantID, &w.TaskRowID, &w.ThreadKey, &w.Reason, &w.State,
		&w.Attempts, &w.NextAttemptAt, &w.CreatedAt, &w.DeliveredAt); err != nil {
		return nil, err
	}
	return &w, nil
}

// ListWakesForTask returns the wakes of a task row, oldest first.
func (s *PGMCPTaskStore) ListWakesForTask(ctx context.Context, taskRowID uuid.UUID) ([]store.MCPTaskWake, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+mcpWakeSelectCols+` FROM mcp_task_wakes w
		 WHERE w.task_row_id = $1 AND w.tenant_id = $2 ORDER BY w.created_at, w.wake_id`, taskRowID, tid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.MCPTaskWake
	for rows.Next() {
		w, err := scanMCPWake(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *w)
	}
	return out, rows.Err()
}

// ExpireWakesForTask moves the open wakes of a task row to expired.
func (s *PGMCPTaskStore) ExpireWakesForTask(ctx context.Context, taskRowID uuid.UUID) (int, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE mcp_task_wakes SET state = $1
		 WHERE task_row_id = $2 AND tenant_id = $3 AND state IN ($4, $5)`,
		store.MCPWakeStateExpired, taskRowID, tid, store.MCPWakeStatePending, store.MCPWakeStateNoSink)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// ListDueWakes returns due wakes of all tenants with their task rows (system read).
// The states are literals in the SQL text (not parameters) so that the
// partial index idx_mcp_task_wakes_due matches also under a generic plan.
func (s *PGMCPTaskStore) ListDueWakes(ctx context.Context, now time.Time, limit int) ([]store.MCPTaskWakeDue, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+mcpWakeSelectCols+`, `+mcpTaskSelectCols+`
		 FROM mcp_task_wakes w JOIN mcp_tasks t ON t.id = w.task_row_id AND t.tenant_id = w.tenant_id
		 WHERE w.state IN ('pending', 'no_sink') AND w.next_attempt_at <= $1
		 ORDER BY w.next_attempt_at, w.created_at LIMIT $2`,
		now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.MCPTaskWakeDue
	for rows.Next() {
		var d store.MCPTaskWakeDue
		var caps, thread, answered []byte
		w, t := &d.Wake, &d.Task
		if err := rows.Scan(
			&w.WakeID, &w.TenantID, &w.TaskRowID, &w.ThreadKey, &w.Reason, &w.State,
			&w.Attempts, &w.NextAttemptAt, &w.CreatedAt, &w.DeliveredAt,
			&t.ID, &t.TenantID, &t.AgentID, &t.ServerID, &t.TaskID, &t.SessionKey,
			&t.ACPSessionID, &t.UserID, &t.Status, &t.StatusMessage, &t.EventCursor, &caps,
			&thread, &answered, &t.PRWoken, &t.CreatedAt, &t.UpdatedAt, &t.TerminalAt,
			&t.ExpiresAt,
		); err != nil {
			return nil, err
		}
		fillMCPTaskJSON(t, caps, thread, answered)
		out = append(out, d)
	}
	return out, rows.Err()
}

// MarkWakeOutcome records one delivery attempt of a wake that is still
// pending or no_sink.
func (s *PGMCPTaskStore) MarkWakeOutcome(ctx context.Context, wakeID, state string, nextAttemptAt time.Time) (bool, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE mcp_task_wakes SET state = $1, next_attempt_at = $2, attempts = attempts + 1,
		 delivered_at = CASE WHEN $1 = $3 THEN COALESCE(delivered_at, NOW()) ELSE delivered_at END
		 WHERE wake_id = $4 AND tenant_id = $5 AND state IN ('pending', 'no_sink')`,
		state, nextAttemptAt.UTC(), store.MCPWakeStateDelivered, wakeID, tid)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// WakeThreadStats describes the delivered wakes of a thread.
func (s *PGMCPTaskStore) WakeThreadStats(ctx context.Context, threadKey string, since time.Time) (store.MCPWakeThreadStats, error) {
	var stats store.MCPWakeThreadStats
	tid, err := requireTenantID(ctx)
	if err != nil {
		return stats, err
	}
	err = s.db.QueryRowContext(ctx,
		`SELECT MAX(delivered_at), COUNT(*) FILTER (WHERE delivered_at >= $3)
		 FROM mcp_task_wakes WHERE tenant_id = $1 AND thread_key = $2 AND delivered_at IS NOT NULL`,
		tid, threadKey, since.UTC()).Scan(&stats.LastDeliveredAt, &stats.DeliveredSince)
	return stats, err
}

// nonNilStrings returns s, or an empty slice for nil (so it encodes as []).
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
