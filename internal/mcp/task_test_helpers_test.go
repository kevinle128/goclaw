package mcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// memTaskStore is an in-memory store.MCPTaskStore with tenant checks like
// the SQL stores.
type memTaskStore struct {
	mu      sync.Mutex
	rows    map[uuid.UUID]*store.MCPTask
	wakes   map[string]*store.MCPTaskWake
	cursors []string // every stored cursor, in order
	// clock, when set, gives delivered_at (default time.Now).
	clock func() time.Time
	// failWakes makes CreateWake fail while it is true.
	failWakes atomic.Bool
}

func newMemTaskStore() *memTaskStore {
	return &memTaskStore{rows: map[uuid.UUID]*store.MCPTask{}, wakes: map[string]*store.MCPTaskWake{}}
}

var errNoTenant = errors.New("tenant required")

func memTenant(ctx context.Context) (uuid.UUID, error) {
	tid := store.TenantIDFromContext(ctx)
	if tid == uuid.Nil {
		return uuid.Nil, errNoTenant
	}
	return tid, nil
}

func cloneTask(t *store.MCPTask) *store.MCPTask {
	c := *t
	c.AnsweredKeys = slices.Clone(t.AnsweredKeys)
	if t.EventCursor != nil {
		v := *t.EventCursor
		c.EventCursor = &v
	}
	return &c
}

func (s *memTaskStore) row(ctx context.Context, id uuid.UUID) (*store.MCPTask, error) {
	tid, err := memTenant(ctx)
	if err != nil {
		return nil, err
	}
	r, ok := s.rows[id]
	if !ok || r.TenantID != tid {
		return nil, nil
	}
	return r, nil
}

func (s *memTaskStore) CreateTask(ctx context.Context, t *store.MCPTask) error {
	tid, err := memTenant(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.TenantID == tid && r.ServerID == t.ServerID && r.TaskID == t.TaskID {
			return store.ErrMCPTaskExists
		}
	}
	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	t.TenantID = tid
	t.CreatedAt, t.UpdatedAt = time.Now(), time.Now()
	s.rows[t.ID] = cloneTask(t)
	return nil
}

// put stores a row as is (for resume tests).
func (s *memTaskStore) put(t store.MCPTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[t.ID] = cloneTask(&t)
}

func (s *memTaskStore) GetTask(ctx context.Context, id uuid.UUID) (*store.MCPTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.row(ctx, id)
	if r == nil || err != nil {
		return nil, err
	}
	return cloneTask(r), nil
}

func (s *memTaskStore) GetTaskForAgent(ctx context.Context, agentID uuid.UUID, taskID string) (*store.MCPTask, error) {
	tid, err := memTenant(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.TenantID == tid && r.AgentID == agentID && r.TaskID == taskID {
			return cloneTask(r), nil
		}
	}
	return nil, nil
}

func (s *memTaskStore) HasOpenTaskForAgent(ctx context.Context, agentID uuid.UUID) (bool, error) {
	tid, err := memTenant(ctx)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.TenantID == tid && r.AgentID == agentID && r.TerminalAt == nil && r.ExpiresAt.After(time.Now()) {
			return true, nil
		}
	}
	return false, nil
}

func (s *memTaskStore) ListOpenTasks(context.Context) ([]store.MCPTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.MCPTask
	for _, r := range s.rows {
		if r.TerminalAt == nil && r.ExpiresAt.After(time.Now()) {
			out = append(out, *cloneTask(r))
		}
	}
	return out, nil
}

func (s *memTaskStore) update(ctx context.Context, id uuid.UUID, fn func(*store.MCPTask)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.row(ctx, id)
	if err != nil || r == nil {
		return err
	}
	fn(r)
	r.UpdatedAt = time.Now()
	return nil
}

func (s *memTaskStore) UpdateTaskCursor(ctx context.Context, id uuid.UUID, cursor string) error {
	if cursor == "" {
		return nil
	}
	return s.update(ctx, id, func(r *store.MCPTask) {
		c := cursor
		r.EventCursor = &c
		s.cursors = append(s.cursors, cursor)
	})
}

func (s *memTaskStore) UpdateTaskStatus(ctx context.Context, id uuid.UUID, u store.MCPTaskStatusUpdate) error {
	return s.update(ctx, id, func(r *store.MCPTask) {
		r.Status, r.StatusMessage = u.Status, u.StatusMessage
		if u.Terminal && r.TerminalAt == nil {
			now := time.Now()
			r.TerminalAt = &now
		}
	})
}

func (s *memTaskStore) AddAnsweredKey(ctx context.Context, id uuid.UUID, key string) error {
	return s.update(ctx, id, func(r *store.MCPTask) {
		if !slices.Contains(r.AnsweredKeys, key) {
			r.AnsweredKeys = append(r.AnsweredKeys, key)
		}
	})
}

func (s *memTaskStore) RemoveAnsweredKey(ctx context.Context, id uuid.UUID, key string) error {
	return s.update(ctx, id, func(r *store.MCPTask) {
		r.AnsweredKeys = slices.DeleteFunc(r.AnsweredKeys, func(k string) bool { return k == key })
	})
}

func (s *memTaskStore) MarkPRWoken(ctx context.Context, id uuid.UUID) (bool, error) {
	changed := false
	err := s.update(ctx, id, func(r *store.MCPTask) {
		changed = !r.PRWoken
		r.PRWoken = true
	})
	return changed, err
}

func (s *memTaskStore) MarkTaskTerminal(ctx context.Context, id uuid.UUID) error {
	return s.update(ctx, id, func(r *store.MCPTask) {
		if r.TerminalAt == nil {
			now := time.Now()
			r.TerminalAt = &now
		}
	})
}

var errWakeStoreDown = errors.New("wake store down")

func (s *memTaskStore) CreateWake(ctx context.Context, w *store.MCPTaskWake) (bool, error) {
	if s.failWakes.Load() {
		return false, errWakeStoreDown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.row(ctx, w.TaskRowID)
	if err != nil || r == nil {
		return false, err
	}
	if _, ok := s.wakes[w.WakeID]; ok {
		return false, nil
	}
	c := *w
	c.TenantID = r.TenantID
	c.State = store.MCPWakeStatePending
	c.CreatedAt = time.Now()
	s.wakes[w.WakeID] = &c
	return true, nil
}

func (s *memTaskStore) ListWakesForTask(ctx context.Context, taskRowID uuid.UUID) ([]store.MCPTaskWake, error) {
	tid, err := memTenant(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.MCPTaskWake
	for _, w := range s.wakes {
		if w.TaskRowID == taskRowID && w.TenantID == tid {
			out = append(out, *w)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *memTaskStore) ExpireWakesForTask(ctx context.Context, taskRowID uuid.UUID) (int, error) {
	tid, err := memTenant(ctx)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, w := range s.wakes {
		if w.TaskRowID == taskRowID && w.TenantID == tid && (w.State == store.MCPWakeStatePending || w.State == store.MCPWakeStateNoSink) {
			w.State = store.MCPWakeStateExpired
			n++
		}
	}
	return n, nil
}

// ListDueWakes returns the pending and no_sink wakes of all tenants with
// next_attempt_at <= now, oldest first, like the SQL stores.
func (s *memTaskStore) ListDueWakes(_ context.Context, now time.Time, limit int) ([]store.MCPTaskWakeDue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.MCPTaskWakeDue
	for _, w := range s.wakes {
		if (w.State != store.MCPWakeStatePending && w.State != store.MCPWakeStateNoSink) || w.NextAttemptAt.After(now) {
			continue
		}
		r, ok := s.rows[w.TaskRowID]
		if !ok || r.TenantID != w.TenantID {
			continue
		}
		out = append(out, store.MCPTaskWakeDue{Wake: *w, Task: *cloneTask(r)})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Wake.NextAttemptAt.Equal(out[j].Wake.NextAttemptAt) {
			return out[i].Wake.NextAttemptAt.Before(out[j].Wake.NextAttemptAt)
		}
		return out[i].Wake.CreatedAt.Before(out[j].Wake.CreatedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// MarkWakeOutcome records one attempt in the tenant of ctx. delivered_at
// comes from the clock of the store.
func (s *memTaskStore) MarkWakeOutcome(ctx context.Context, wakeID, state string, next time.Time) (bool, error) {
	tid, err := memTenant(ctx)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.wakes[wakeID]
	if !ok || w.TenantID != tid || (w.State != store.MCPWakeStatePending && w.State != store.MCPWakeStateNoSink) {
		return false, nil
	}
	w.State = state
	w.NextAttemptAt = next
	w.Attempts++
	if state == store.MCPWakeStateDelivered && w.DeliveredAt == nil {
		at := s.clockNow()
		w.DeliveredAt = &at
	}
	return true, nil
}

// WakeThreadStats describes the delivered wakes of a thread in the tenant
// of ctx.
func (s *memTaskStore) WakeThreadStats(ctx context.Context, threadKey string, since time.Time) (store.MCPWakeThreadStats, error) {
	var stats store.MCPWakeThreadStats
	tid, err := memTenant(ctx)
	if err != nil {
		return stats, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.wakes {
		if w.TenantID != tid || w.ThreadKey != threadKey || w.DeliveredAt == nil {
			continue
		}
		if stats.LastDeliveredAt == nil || w.DeliveredAt.After(*stats.LastDeliveredAt) {
			at := *w.DeliveredAt
			stats.LastDeliveredAt = &at
		}
		if !w.DeliveredAt.Before(since) {
			stats.DeliveredSince++
		}
	}
	return stats, nil
}

func (s *memTaskStore) clockNow() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

// wake returns a copy of one wake row without a tenant check.
func (s *memTaskStore) wake(id string) store.MCPTaskWake {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.wakes[id]; ok {
		return *w
	}
	return store.MCPTaskWake{}
}

// snapshot returns a copy of a row without a tenant check.
func (s *memTaskStore) snapshot(id uuid.UUID) store.MCPTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.rows[id]; ok {
		return *cloneTask(r)
	}
	return store.MCPTask{}
}

// wakeList returns all wakes of a row, sorted by creation.
func (s *memTaskStore) wakeList(rowID uuid.UUID) []store.MCPTaskWake {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.MCPTaskWake
	for _, w := range s.wakes {
		if w.TaskRowID == rowID {
			out = append(out, *w)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *memTaskStore) cursorHistory() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.cursors)
}

// fakeServerStore returns server records by id, in their tenant only. A
// missing record gives sql.ErrNoRows, like the SQL stores.
type fakeServerStore struct {
	mockMCPStore
	mu      sync.Mutex
	servers map[uuid.UUID]store.MCPServerData
	tenants map[uuid.UUID]uuid.UUID
}

func (f *fakeServerStore) GetServer(ctx context.Context, id uuid.UUID) (*store.MCPServerData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	srv, ok := f.servers[id]
	if !ok || f.tenants[id] != store.TenantIDFromContext(ctx) {
		return nil, sql.ErrNoRows
	}
	return &srv, nil
}

// update changes the record of a server.
func (f *fakeServerStore) update(id uuid.UUID, fn func(*store.MCPServerData)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	srv := f.servers[id]
	fn(&srv)
	f.servers[id] = srv
}

// remove deletes the record of a server.
func (f *fakeServerStore) remove(id uuid.UUID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.servers, id)
}

// remove deletes a task row and its wakes, as a cascade delete does.
func (s *memTaskStore) remove(id uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, id)
	for k, w := range s.wakes {
		if w.TaskRowID == id {
			delete(s.wakes, k)
		}
	}
}

const testEvent = "workflow_run.status_changed"

// taskFixture is a stub server, a pool, the stores and a manager with short
// timings.
type taskFixture struct {
	t        *testing.T
	srv      *mcptest.Server
	pool     *Pool
	tasks    *memTaskStore
	servers  *fakeServerStore
	mgr      *TaskWatchManager
	tenantID uuid.UUID
	agentID  uuid.UUID
	serverID uuid.UUID
}

type fixtureOpts struct {
	extensions map[string]any // nil = tasks and events
	settings   string         // server settings JSON; "" = default task_events
}

const defaultTaskSettings = `{"task_events":{"event":"workflow_run.status_changed","task_argument":"run_id","bootstrap_arguments":{"workflow":"mcp-smoke"}}}`

func newTaskFixture(t *testing.T, o fixtureOpts) *taskFixture {
	t.Helper()
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern, Extensions: o.extensions})
	srv.AddTaskTool("run_workflow", "Start a run")
	pool := NewPool(PoolConfig{})
	t.Cleanup(pool.Stop)

	settings := o.settings
	if settings == "" {
		settings = defaultTaskSettings
	}
	f := &taskFixture{
		t: t, srv: srv, pool: pool, tasks: newMemTaskStore(),
		tenantID: uuid.New(), agentID: uuid.New(), serverID: uuid.New(),
	}
	f.servers = &fakeServerStore{servers: map[uuid.UUID]store.MCPServerData{}, tenants: map[uuid.UUID]uuid.UUID{}}
	data := store.MCPServerData{Name: "archon-mcp", Transport: "streamable-http", URL: srv.URL,
		Settings: json.RawMessage(settings), Enabled: true, TimeoutSec: 10}
	data.ID = f.serverID
	f.servers.servers[f.serverID] = data
	f.servers.tenants[f.serverID] = f.tenantID
	f.mgr = f.newManager()
	return f
}

// newManager builds a manager on the fixture stores with test timings.
func (f *taskFixture) newManager() *TaskWatchManager {
	mgr, err := NewTaskWatchManager(TaskWatchConfig{Tasks: f.tasks, Servers: f.servers, Pool: f.pool})
	if err != nil {
		f.t.Fatal(err)
	}
	mgr.timing = watchTiming{
		heartbeat: 5 * time.Second, safetyPoll: time.Hour,
		defaultPoll: 30 * time.Millisecond, minPoll: 10 * time.Millisecond, maxPoll: 30 * time.Millisecond,
		eventsPollFloor: 10 * time.Millisecond, eventsPollCap: 50 * time.Millisecond,
		backoffBase: 10 * time.Millisecond, backoffMax: 50 * time.Millisecond,
		streamFailures: 3, streamRetryAfter: time.Hour, acquireTimeout: 5 * time.Second,
		unknownGrace: 100 * time.Millisecond, unknownCalls: 5,
	}
	f.t.Cleanup(mgr.Stop)
	return mgr
}

// ctx is a run context of the fixture agent, optionally an ACP turn.
func (f *taskFixture) ctx(acp bool) context.Context {
	ctx := store.WithTenantID(context.Background(), f.tenantID)
	ctx = store.WithAgentID(ctx, f.agentID)
	ctx = store.WithUserID(ctx, "user-1")
	if acp {
		ctx = WithACPOrigin(ctx, ACPOrigin{SessionID: "acp-sess-1", Thread: json.RawMessage(`{"channelId":"ch","rootEventId":"ev"}`)})
	}
	return ctx
}

// openRow stores an open row for a stub task and returns it.
func (f *taskFixture) openRow(taskID string, acp bool, cursor *string) store.MCPTask {
	row := store.MCPTask{
		ID: uuid.New(), TenantID: f.tenantID, AgentID: f.agentID, ServerID: f.serverID,
		TaskID: taskID, Status: TaskStatusWorking, EventCursor: cursor,
		RequestCaps: json.RawMessage(`{"tasks":true,"events":true}`),
		ExpiresAt:   time.Now().Add(time.Hour),
	}
	if acp {
		row.ACPSessionID = "acp-sess-1"
		row.ThreadContext = json.RawMessage(`{"channelId":"ch","rootEventId":"ev"}`)
	}
	f.tasks.put(row)
	return row
}

// eventually waits until cond is true.
func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout: "+msg, args...)
}

//go:fix inline
func strPtr(s string) *string { return new(s) }

// streamCursors returns the cursor param of each events/stream request.
func streamCursors(srv *mcptest.Server) []*string {
	var out []*string
	for _, r := range srv.RequestsFor(methodEventsStream) {
		var c *string
		_ = r.Param("cursor", &c)
		out = append(out, c)
	}
	return out
}

// countFor counts the recorded requests of a method.
func countFor(srv *mcptest.Server, method string) int { return len(srv.RequestsFor(method)) }

// wakeReasons lists the reasons of the wakes of a row.
func wakeReasons(ws []store.MCPTaskWake) []string {
	out := make([]string, len(ws))
	for i, w := range ws {
		out[i] = w.Reason
	}
	return out
}
