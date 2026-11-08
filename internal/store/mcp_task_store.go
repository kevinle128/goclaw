package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
)

// Wake reasons. A wake tells an ACP client that a watched MCP task needs
// attention in the thread that started it.
const (
	MCPWakeReasonInputRequired = "input_required"
	MCPWakeReasonPRReady       = "pr_ready"
	MCPWakeReasonCompleted     = "completed"
	MCPWakeReasonFailed        = "failed"
	MCPWakeReasonCancelled     = "cancelled"
)

// Wake states.
const (
	MCPWakeStatePending     = "pending"
	MCPWakeStateDelivered   = "delivered"
	MCPWakeStateNoSink      = "no_sink"
	MCPWakeStateUnsupported = "unsupported"
	MCPWakeStateExpired     = "expired"
)

// ErrMCPTaskExists is returned by CreateTask when a row with the same
// (tenant, server, task id) exists.
var ErrMCPTaskExists = errors.New("mcp task row already exists")

// MCPTask is one row of mcp_tasks: an MCP task that a modern tools/call
// started and that goclaw watches until it ends or expires.
type MCPTask struct {
	ID            uuid.UUID
	TenantID      uuid.UUID
	AgentID       uuid.UUID
	ServerID      uuid.UUID
	TaskID        string
	SessionKey    string
	ACPSessionID  string
	UserID        string
	Status        string
	StatusMessage *string
	// EventCursor is the last events cursor. Nil means "no cursor"; a null
	// cursor from the server is never stored.
	EventCursor *string
	// RequestCaps is the per-request capability set of the tools/call that
	// started the task (JSON object).
	RequestCaps json.RawMessage
	// ThreadContext is the opaque ACP thread object of the turn that
	// started the task (nil when the turn had none).
	ThreadContext json.RawMessage
	AnsweredKeys  []string
	PRWoken       bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
	TerminalAt    *time.Time
	ExpiresAt     time.Time
}

// HasACPOrigin reports whether the task was started from an ACP turn. Only
// such tasks get wakes.
func (t *MCPTask) HasACPOrigin() bool {
	if t == nil {
		return false
	}
	return t.ACPSessionID != "" || hasJSONValue(t.ThreadContext)
}

// IsAnswered reports whether key is in AnsweredKeys.
func (t *MCPTask) IsAnswered(key string) bool {
	return slices.Contains(t.AnsweredKeys, key)
}

// MCPTaskWake is one row of mcp_task_wakes.
type MCPTaskWake struct {
	WakeID        string
	TenantID      uuid.UUID
	TaskRowID     uuid.UUID
	ThreadKey     string
	Reason        string
	State         string
	Attempts      int
	NextAttemptAt time.Time
	CreatedAt     time.Time
	DeliveredAt   *time.Time
}

// MCPTaskWakeDue is a due wake together with its task row, for the wake
// dispatcher.
type MCPTaskWakeDue struct {
	Wake MCPTaskWake
	Task MCPTask
}

// MCPTaskStatusUpdate is the truth from one tasks/get.
type MCPTaskStatusUpdate struct {
	Status        string
	StatusMessage *string
	// Terminal sets terminal_at (once) when true.
	Terminal bool
}

// MCPWakeThreadStats describes the delivered wakes of one thread.
type MCPWakeThreadStats struct {
	// LastDeliveredAt is the latest delivered_at of the thread (nil if none).
	LastDeliveredAt *time.Time
	// DeliveredSince is the number of wakes delivered at or after the
	// "since" time of the query.
	DeliveredSince int
}

// MCPTaskStore persists watched MCP tasks and their wakes.
//
// Every method takes the tenant from ctx and filters by it, except the two
// system reads ListOpenTasks and ListDueWakes, which the gateway-owned
// watcher manager and wake dispatcher use across tenants.
type MCPTaskStore interface {
	// CreateTask inserts a row. The tenant comes from ctx; ID is set when
	// it is nil. It returns ErrMCPTaskExists on a duplicate task id.
	CreateTask(ctx context.Context, t *MCPTask) error
	// GetTask returns the row with id, or nil when it does not exist in the
	// tenant.
	GetTask(ctx context.Context, id uuid.UUID) (*MCPTask, error)
	// GetTaskForAgent returns the newest row of the agent with the server
	// task id, or nil.
	GetTaskForAgent(ctx context.Context, agentID uuid.UUID, taskID string) (*MCPTask, error)
	// HasOpenTaskForAgent reports whether the agent has a row that is not
	// terminal and not expired.
	HasOpenTaskForAgent(ctx context.Context, agentID uuid.UUID) (bool, error)
	// ListOpenTasks returns the rows of all tenants that are not terminal
	// and not expired. System read for the watcher manager at start.
	ListOpenTasks(ctx context.Context) ([]MCPTask, error)

	// UpdateTaskCursor stores a non-empty events cursor.
	UpdateTaskCursor(ctx context.Context, id uuid.UUID, cursor string) error
	// UpdateTaskStatus stores the status from tasks/get.
	UpdateTaskStatus(ctx context.Context, id uuid.UUID, u MCPTaskStatusUpdate) error
	// AddAnsweredKey adds key to answered_keys (no duplicate).
	AddAnsweredKey(ctx context.Context, id uuid.UUID, key string) error
	// RemoveAnsweredKey removes key from answered_keys.
	RemoveAnsweredKey(ctx context.Context, id uuid.UUID, key string) error
	// MarkPRWoken sets pr_woken and reports whether this call changed it.
	MarkPRWoken(ctx context.Context, id uuid.UUID) (bool, error)
	// MarkTaskTerminal sets terminal_at without a status change (for
	// example when the server no longer knows the task).
	MarkTaskTerminal(ctx context.Context, id uuid.UUID) error

	// CreateWake inserts a wake in state pending. A wake with the same id
	// is left unchanged; created tells whether a row was added.
	CreateWake(ctx context.Context, w *MCPTaskWake) (created bool, err error)
	// ListWakesForTask returns the wakes of a task row, oldest first.
	ListWakesForTask(ctx context.Context, taskRowID uuid.UUID) ([]MCPTaskWake, error)
	// ExpireWakesForTask moves the pending and no_sink wakes of a task row
	// to expired and returns how many changed.
	ExpireWakesForTask(ctx context.Context, taskRowID uuid.UUID) (int, error)

	// ListDueWakes returns wakes of all tenants in state pending or no_sink
	// with next_attempt_at <= now, oldest first, with their task rows.
	// System read for the wake dispatcher.
	ListDueWakes(ctx context.Context, now time.Time, limit int) ([]MCPTaskWakeDue, error)
	// MarkWakeOutcome records one delivery attempt: it sets state and
	// next_attempt_at, adds one to attempts, and sets delivered_at when the
	// state is delivered.
	MarkWakeOutcome(ctx context.Context, wakeID, state string, nextAttemptAt time.Time) error
	// WakeThreadStats describes the delivered wakes of a thread.
	WakeThreadStats(ctx context.Context, threadKey string, since time.Time) (MCPWakeThreadStats, error)
}

// MCPWakeID returns the stable wake id of a trigger: "w_" + hex SHA-256 of
// "<row id>:<reason>:<discriminator>". A retry or a restart gives the same id.
func MCPWakeID(rowID uuid.UUID, reason, discriminator string) string {
	sum := sha256.Sum256([]byte(rowID.String() + ":" + reason + ":" + discriminator))
	return "w_" + hex.EncodeToString(sum[:])
}

// MCPWakeThreadKey returns the key that groups the wakes of one ACP thread:
// hex SHA-256 of the canonical JSON of the thread object. A task without a
// thread object uses "session:<acpSessionID>" as the input, so the wakes of
// one ACP session share a key.
func MCPWakeThreadKey(thread json.RawMessage, acpSessionID string) string {
	input := "session:" + acpSessionID
	if hasJSONValue(thread) {
		if canonical, err := canonicalJSON(thread); err == nil {
			input = string(canonical)
		}
	}
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// canonicalJSON re-encodes a JSON value with sorted object keys and no
// insignificant white space.
func canonicalJSON(raw json.RawMessage) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// hasJSONValue reports whether raw holds a JSON value other than null or an
// empty object.
func hasJSONValue(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	switch string(trimmed) {
	case "", "null", "{}":
		return false
	}
	return true
}
