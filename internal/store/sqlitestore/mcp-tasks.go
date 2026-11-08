//go:build sqlite || sqliteonly

package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// SQLiteMCPTaskStore implements store.MCPTaskStore with SQLite.
//
// Times are stored as fixed-width UTC text (mcpTimeLayout), the same shape
// as the strftime defaults of the schema, so text comparison orders them.
type SQLiteMCPTaskStore struct {
	db *sql.DB
}

// NewSQLiteMCPTaskStore creates a SQLite MCP task store.
func NewSQLiteMCPTaskStore(db *sql.DB) *SQLiteMCPTaskStore {
	return &SQLiteMCPTaskStore{db: db}
}

// mcpTimeLayout matches strftime('%Y-%m-%dT%H:%M:%fZ').
const mcpTimeLayout = "2006-01-02T15:04:05.000Z"

func mcpTime(t time.Time) string { return t.UTC().Format(mcpTimeLayout) }

const sqliteMCPTaskCols = `t.id, t.tenant_id, t.agent_id, t.server_id, t.task_id, t.session_key,
	t.acp_session_id, t.user_id, t.status, t.status_message, t.event_cursor, t.request_caps,
	t.thread_context, t.answered_keys, t.pr_woken, t.created_at, t.updated_at, t.terminal_at,
	t.expires_at`

const sqliteMCPWakeCols = `w.wake_id, w.tenant_id, w.task_row_id, w.thread_key, w.reason, w.state,
	w.attempts, w.next_attempt_at, w.created_at, w.delivered_at`

// mcpTaskScan holds the scan targets of one task row.
type mcpTaskScan struct {
	t                          store.MCPTask
	caps, thread, answered     sql.NullString
	created, updated, expires  sqliteTime
	terminalValid              sql.NullString
	statusMessage, eventCursor sql.NullString
}

func (s *mcpTaskScan) targets() []any {
	return []any{
		&s.t.ID, &s.t.TenantID, &s.t.AgentID, &s.t.ServerID, &s.t.TaskID, &s.t.SessionKey,
		&s.t.ACPSessionID, &s.t.UserID, &s.t.Status, &s.statusMessage, &s.eventCursor, &s.caps,
		&s.thread, &s.answered, &s.t.PRWoken, &s.created, &s.updated, &s.terminalValid,
		&s.expires,
	}
}

func (s *mcpTaskScan) finish() (*store.MCPTask, error) {
	t := s.t
	if s.statusMessage.Valid {
		v := s.statusMessage.String
		t.StatusMessage = &v
	}
	if s.eventCursor.Valid {
		v := s.eventCursor.String
		t.EventCursor = &v
	}
	if s.caps.Valid && s.caps.String != "" {
		t.RequestCaps = json.RawMessage(s.caps.String)
	}
	if s.thread.Valid && s.thread.String != "" && s.thread.String != "null" {
		t.ThreadContext = json.RawMessage(s.thread.String)
	}
	if s.answered.Valid && s.answered.String != "" {
		if err := json.Unmarshal([]byte(s.answered.String), &t.AnsweredKeys); err != nil {
			return nil, fmt.Errorf("decode answered keys: %w", err)
		}
	}
	t.CreatedAt = s.created.Time
	t.UpdatedAt = s.updated.Time
	t.ExpiresAt = s.expires.Time
	if s.terminalValid.Valid && s.terminalValid.String != "" {
		var terminal sqliteTime
		if err := terminal.Scan(s.terminalValid.String); err != nil {
			return nil, err
		}
		v := terminal.Time
		t.TerminalAt = &v
	}
	return &t, nil
}

func scanSQLiteMCPTask(row interface{ Scan(...any) error }) (*store.MCPTask, error) {
	var s mcpTaskScan
	if err := row.Scan(s.targets()...); err != nil {
		return nil, err
	}
	return s.finish()
}

// mcpWakeScan holds the scan targets of one wake row.
type mcpWakeScan struct {
	w             store.MCPTaskWake
	next, created sqliteTime
	delivered     sql.NullString
}

func (s *mcpWakeScan) targets() []any {
	return []any{&s.w.WakeID, &s.w.TenantID, &s.w.TaskRowID, &s.w.ThreadKey, &s.w.Reason, &s.w.State,
		&s.w.Attempts, &s.next, &s.created, &s.delivered}
}

func (s *mcpWakeScan) finish() (*store.MCPTaskWake, error) {
	w := s.w
	w.NextAttemptAt = s.next.Time
	w.CreatedAt = s.created.Time
	if s.delivered.Valid && s.delivered.String != "" {
		var d sqliteTime
		if err := d.Scan(s.delivered.String); err != nil {
			return nil, err
		}
		v := d.Time
		w.DeliveredAt = &v
	}
	return &w, nil
}

// CreateTask inserts a task row in the tenant of ctx.
func (s *SQLiteMCPTaskStore) CreateTask(ctx context.Context, t *store.MCPTask) error {
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
	caps := string(t.RequestCaps)
	if caps == "" {
		caps = "{}"
	}
	var thread any
	if len(t.ThreadContext) > 0 {
		thread = string(t.ThreadContext)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO mcp_tasks
		(id, tenant_id, agent_id, server_id, task_id, session_key, acp_session_id, user_id,
		 status, status_message, event_cursor, request_caps, thread_context, answered_keys,
		 pr_woken, created_at, updated_at, expires_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		t.ID, tid, t.AgentID, t.ServerID, t.TaskID, t.SessionKey, t.ACPSessionID, t.UserID,
		t.Status, t.StatusMessage, t.EventCursor, caps, thread, jsonStringArray(t.AnsweredKeys),
		t.PRWoken, mcpTime(now), mcpTime(now), mcpTime(t.ExpiresAt),
	)
	if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return store.ErrMCPTaskExists
	}
	return err
}

// GetTask returns a task row of the tenant, or nil.
func (s *SQLiteMCPTaskStore) GetTask(ctx context.Context, id uuid.UUID) (*store.MCPTask, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT `+sqliteMCPTaskCols+` FROM mcp_tasks t WHERE t.id = ? AND t.tenant_id = ?`, id, tid)
	t, err := scanSQLiteMCPTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// GetTaskForAgent returns the newest task row of an agent with a server task id.
func (s *SQLiteMCPTaskStore) GetTaskForAgent(ctx context.Context, agentID uuid.UUID, taskID string) (*store.MCPTask, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT `+sqliteMCPTaskCols+` FROM mcp_tasks t
		 WHERE t.tenant_id = ? AND t.agent_id = ? AND t.task_id = ?
		 ORDER BY t.created_at DESC LIMIT 1`, tid, agentID, taskID)
	t, err := scanSQLiteMCPTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// HasOpenTaskForAgent reports whether an agent has an open task row.
func (s *SQLiteMCPTaskStore) HasOpenTaskForAgent(ctx context.Context, agentID uuid.UUID) (bool, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return false, err
	}
	var n int
	err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mcp_tasks
		 WHERE tenant_id = ? AND agent_id = ? AND terminal_at IS NULL AND expires_at > ?`,
		tid, agentID, mcpTime(time.Now())).Scan(&n)
	return n > 0, err
}

// ListOpenTasks returns the open task rows of all tenants (system read).
func (s *SQLiteMCPTaskStore) ListOpenTasks(ctx context.Context) ([]store.MCPTask, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+sqliteMCPTaskCols+` FROM mcp_tasks t
		 WHERE t.terminal_at IS NULL AND t.expires_at > ? ORDER BY t.created_at`, mcpTime(time.Now()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.MCPTask
	for rows.Next() {
		t, err := scanSQLiteMCPTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

// UpdateTaskCursor stores a non-empty events cursor.
func (s *SQLiteMCPTaskStore) UpdateTaskCursor(ctx context.Context, id uuid.UUID, cursor string) error {
	if cursor == "" {
		return nil
	}
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET event_cursor = ?, updated_at = ? WHERE id = ? AND tenant_id = ?`,
		cursor, mcpTime(time.Now()), id, tid)
	return err
}

// UpdateTaskStatus stores the status from tasks/get.
func (s *SQLiteMCPTaskStore) UpdateTaskStatus(ctx context.Context, id uuid.UUID, u store.MCPTaskStatusUpdate) error {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	now := mcpTime(time.Now())
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET status = ?, status_message = ?, updated_at = ?,
		 terminal_at = CASE WHEN ? THEN COALESCE(terminal_at, ?) ELSE terminal_at END
		 WHERE id = ? AND tenant_id = ?`,
		u.Status, u.StatusMessage, now, u.Terminal, now, id, tid)
	return err
}

// AddAnsweredKey adds key to answered_keys once.
func (s *SQLiteMCPTaskStore) AddAnsweredKey(ctx context.Context, id uuid.UUID, key string) error {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET answered_keys = json_insert(answered_keys, '$[#]', ?), updated_at = ?
		 WHERE id = ? AND tenant_id = ?
		   AND NOT EXISTS (SELECT 1 FROM json_each(mcp_tasks.answered_keys) WHERE value = ?)`,
		key, mcpTime(time.Now()), id, tid, key)
	return err
}

// RemoveAnsweredKey removes key from answered_keys.
func (s *SQLiteMCPTaskStore) RemoveAnsweredKey(ctx context.Context, id uuid.UUID, key string) error {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET answered_keys = (
		   SELECT COALESCE(json_group_array(value), '[]') FROM json_each(mcp_tasks.answered_keys) WHERE value != ?
		 ), updated_at = ?
		 WHERE id = ? AND tenant_id = ?`,
		key, mcpTime(time.Now()), id, tid)
	return err
}

// MarkPRWoken sets pr_woken and reports whether this call changed it.
func (s *SQLiteMCPTaskStore) MarkPRWoken(ctx context.Context, id uuid.UUID) (bool, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET pr_woken = 1, updated_at = ? WHERE id = ? AND tenant_id = ? AND pr_woken = 0`,
		mcpTime(time.Now()), id, tid)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// MarkTaskTerminal sets terminal_at once, without a status change.
func (s *SQLiteMCPTaskStore) MarkTaskTerminal(ctx context.Context, id uuid.UUID) error {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return err
	}
	now := mcpTime(time.Now())
	_, err = s.db.ExecContext(ctx,
		`UPDATE mcp_tasks SET terminal_at = COALESCE(terminal_at, ?), updated_at = ? WHERE id = ? AND tenant_id = ?`,
		now, now, id, tid)
	return err
}

// CreateWake inserts a pending wake; an existing id is left unchanged.
func (s *SQLiteMCPTaskStore) CreateWake(ctx context.Context, w *store.MCPTaskWake) (bool, error) {
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
		 SELECT ?, ?, t.id, ?, ?, ?, ?, ? FROM mcp_tasks t WHERE t.id = ? AND t.tenant_id = ?
		 ON CONFLICT (wake_id) DO NOTHING`,
		w.WakeID, tid, w.ThreadKey, w.Reason, w.State, mcpTime(w.NextAttemptAt), mcpTime(now), w.TaskRowID, tid)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// ListWakesForTask returns the wakes of a task row, oldest first.
func (s *SQLiteMCPTaskStore) ListWakesForTask(ctx context.Context, taskRowID uuid.UUID) ([]store.MCPTaskWake, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+sqliteMCPWakeCols+` FROM mcp_task_wakes w
		 WHERE w.task_row_id = ? AND w.tenant_id = ? ORDER BY w.created_at, w.wake_id`, taskRowID, tid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.MCPTaskWake
	for rows.Next() {
		var sc mcpWakeScan
		if err := rows.Scan(sc.targets()...); err != nil {
			return nil, err
		}
		w, err := sc.finish()
		if err != nil {
			return nil, err
		}
		out = append(out, *w)
	}
	return out, rows.Err()
}

// ExpireWakesForTask moves the open wakes of a task row to expired.
func (s *SQLiteMCPTaskStore) ExpireWakesForTask(ctx context.Context, taskRowID uuid.UUID) (int, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE mcp_task_wakes SET state = ? WHERE task_row_id = ? AND tenant_id = ? AND state IN (?, ?)`,
		store.MCPWakeStateExpired, taskRowID, tid, store.MCPWakeStatePending, store.MCPWakeStateNoSink)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// ListDueWakes returns due wakes of all tenants with their task rows (system read).
// The states are literals in the SQL text (not parameters) so that the
// partial index idx_mcp_task_wakes_due can match.
func (s *SQLiteMCPTaskStore) ListDueWakes(ctx context.Context, now time.Time, limit int) ([]store.MCPTaskWakeDue, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+sqliteMCPWakeCols+`, `+sqliteMCPTaskCols+`
		 FROM mcp_task_wakes w JOIN mcp_tasks t ON t.id = w.task_row_id AND t.tenant_id = w.tenant_id
		 WHERE w.state IN ('pending', 'no_sink') AND w.next_attempt_at <= ?
		 ORDER BY w.next_attempt_at, w.created_at LIMIT ?`,
		mcpTime(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.MCPTaskWakeDue
	for rows.Next() {
		var ws mcpWakeScan
		var ts mcpTaskScan
		if err := rows.Scan(append(ws.targets(), ts.targets()...)...); err != nil {
			return nil, err
		}
		w, err := ws.finish()
		if err != nil {
			return nil, err
		}
		t, err := ts.finish()
		if err != nil {
			return nil, err
		}
		out = append(out, store.MCPTaskWakeDue{Wake: *w, Task: *t})
	}
	return out, rows.Err()
}

// MarkWakeOutcome records one delivery attempt of a wake that is still
// pending or no_sink.
func (s *SQLiteMCPTaskStore) MarkWakeOutcome(ctx context.Context, wakeID, state string, nextAttemptAt time.Time) (bool, error) {
	tid, err := requireTenantID(ctx)
	if err != nil {
		return false, err
	}
	var delivered any
	if state == store.MCPWakeStateDelivered {
		delivered = mcpTime(time.Now())
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE mcp_task_wakes SET state = ?, next_attempt_at = ?, attempts = attempts + 1,
		 delivered_at = COALESCE(delivered_at, ?)
		 WHERE wake_id = ? AND tenant_id = ? AND state IN ('pending', 'no_sink')`,
		state, mcpTime(nextAttemptAt), delivered, wakeID, tid)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// WakeThreadStats describes the delivered wakes of a thread.
func (s *SQLiteMCPTaskStore) WakeThreadStats(ctx context.Context, threadKey string, since time.Time) (store.MCPWakeThreadStats, error) {
	var stats store.MCPWakeThreadStats
	tid, err := requireTenantID(ctx)
	if err != nil {
		return stats, err
	}
	var last sql.NullString
	err = s.db.QueryRowContext(ctx,
		`SELECT MAX(delivered_at), COALESCE(SUM(CASE WHEN delivered_at >= ? THEN 1 ELSE 0 END), 0)
		 FROM mcp_task_wakes WHERE tenant_id = ? AND thread_key = ? AND delivered_at IS NOT NULL`,
		mcpTime(since), tid, threadKey).Scan(&last, &stats.DeliveredSince)
	if err != nil {
		return stats, err
	}
	if last.Valid && last.String != "" {
		var d sqliteTime
		if err := d.Scan(last.String); err != nil {
			return stats, err
		}
		v := d.Time
		stats.LastDeliveredAt = &v
	}
	return stats, nil
}
