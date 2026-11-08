package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

// defaultTaskTTL is the lifetime of a task row when the server sends no ttlMs.
const defaultTaskTTL = 7 * 24 * time.Hour

// maxTaskTTL bounds the lifetime of a task row (it also keeps the duration
// math far from overflow).
const maxTaskTTL = 365 * 24 * time.Hour

// stopWaitLimit bounds how long Stop waits for the watchers to end.
const stopWaitLimit = 15 * time.Second

// TaskWatchConfig configures a TaskWatchManager.
type TaskWatchConfig struct {
	Tasks   store.MCPTaskStore
	Servers store.MCPServerStore
	Pool    *Pool
	// OnWake, when set, runs after a new wake row was created (for example
	// to wake a dispatcher). It runs on the watcher goroutine and must
	// return quickly.
	OnWake func(ctx context.Context, wake store.MCPTaskWake)
}

// TaskWatchManager owns the task watchers of the gateway: one goroutine per
// open mcp_tasks row. It implements TaskStarter for modern tools/call
// requests that return a task, and backs the mcp_task tool.
//
// The watchers run on a context of the manager, not on the context of the
// agent run that started the task, so the run can end at once.
type TaskWatchManager struct {
	tasks   store.MCPTaskStore
	servers store.MCPServerStore
	pool    *Pool
	onWake  func(ctx context.Context, wake store.MCPTaskWake)
	timing  watchTiming

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	watchers map[uuid.UUID]*taskWatcher
	stopped  bool
	wg       sync.WaitGroup
}

// NewTaskWatchManager creates a manager. Watchers can start at once; call
// Start to resume the open rows of earlier runs.
func NewTaskWatchManager(cfg TaskWatchConfig) (*TaskWatchManager, error) {
	if cfg.Tasks == nil || cfg.Servers == nil || cfg.Pool == nil {
		return nil, errors.New("mcp task watch manager: task store, server store and pool are required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &TaskWatchManager{
		tasks:    cfg.Tasks,
		servers:  cfg.Servers,
		pool:     cfg.Pool,
		onWake:   cfg.OnWake,
		timing:   defaultWatchTiming(),
		ctx:      ctx,
		cancel:   cancel,
		watchers: make(map[uuid.UUID]*taskWatcher),
	}, nil
}

// Tasks returns the task store of the manager.
func (m *TaskWatchManager) Tasks() store.MCPTaskStore { return m.tasks }

// Start resumes a watcher for every row that is not terminal and not
// expired. This is the one read across tenants; each watcher then works in
// the tenant of its row.
func (m *TaskWatchManager) Start(ctx context.Context) error {
	rows, err := m.tasks.ListOpenTasks(ctx)
	if err != nil {
		return fmt.Errorf("list open mcp tasks: %w", err)
	}
	for i := range rows {
		m.watch(rows[i])
	}
	slog.Info("mcp.task.watch_resumed", "rows", len(rows))
	return nil
}

// Stop ends all watchers and leaves the rows unchanged.
func (m *TaskWatchManager) Stop() {
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()
	m.cancel()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(stopWaitLimit):
		slog.Warn("mcp.task.watch_stop_timeout", "waited", stopWaitLimit.String())
	}
}

// watching reports whether a watcher runs for the row id.
func (m *TaskWatchManager) watching(id uuid.UUID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.watchers[id]
	return ok
}

// watch starts a watcher for row unless one runs or the manager stopped.
func (m *TaskWatchManager) watch(row store.MCPTask) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	if _, ok := m.watchers[row.ID]; ok {
		return
	}
	baseCtx := store.WithTenantID(m.ctx, row.TenantID)
	baseCtx = store.WithAgentID(baseCtx, row.AgentID)
	if row.UserID != "" {
		baseCtx = store.WithUserID(baseCtx, row.UserID)
	}
	runCtx, cancel := context.WithDeadline(baseCtx, row.ExpiresAt)
	w := &taskWatcher{
		mgr:          m,
		row:          row,
		caps:         parseRequestCaps(row.RequestCaps),
		timing:       m.timing,
		baseCtx:      baseCtx,
		runCtx:       runCtx,
		cancel:       cancel,
		done:         make(chan struct{}),
		dedup:        newEventDedup(dedupWindow),
		pollInterval: m.timing.defaultPoll,
	}
	m.watchers[row.ID] = w
	m.wg.Go(func() {
		defer cancel()
		w.run()
	})
}

// forget removes a watcher that ended.
func (m *TaskWatchManager) forget(w *taskWatcher) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.watchers[w.row.ID] == w {
		delete(m.watchers, w.row.ID)
	}
}

// watchConn is a pool reference of a watcher.
type watchConn struct {
	entry      *poolEntry
	release    func()
	binding    *TaskEventsBinding
	serverName string
}

// acquire takes a pool reference for the server of row, in the tenant of
// ctx. It uses the server record, the server defaults and the per-user
// credentials of the row's user. Channel context credentials of the run
// that started the task are not known here.
func (m *TaskWatchManager) acquire(ctx context.Context, row *store.MCPTask) (*watchConn, error) {
	srv, err := m.servers.GetServer(ctx, row.ServerID)
	if err != nil {
		return nil, fmt.Errorf("load mcp server %s: %w", row.ServerID, err)
	}
	if srv == nil {
		return nil, fmt.Errorf("mcp server %s not found", row.ServerID)
	}
	rs := resolveCredentials(ctx, m.servers, store.MCPAccessInfo{Server: *srv}, row.UserID)
	if rs == nil {
		return nil, fmt.Errorf("mcp server %q is disabled or has no credentials for the task user", srv.Name)
	}
	// The connection belongs to the pool and outlives this watcher, so it
	// must not end with the watcher context.
	connCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.timing.acquireTimeout)
	defer cancel()
	opt := WithProtocolMode(ParseProtocolMode(srv.Settings))

	conn := &watchConn{binding: ParseTaskEvents(srv.Settings), serverName: srv.Name}
	tenantID := store.TenantIDFromContext(ctx)
	if rs.hasUserCreds && row.UserID != "" {
		entry, err := m.pool.AcquireUser(connCtx, tenantID, srv.Name, row.UserID, srv.Transport, srv.Command,
			rs.args, rs.env, srv.URL, rs.headers, srv.TimeoutSec, opt)
		if err != nil {
			return nil, err
		}
		key := UserPoolKey(tenantID, srv.Name, row.UserID)
		conn.entry = entry
		conn.release = func() { m.pool.ReleaseUser(key) }
		return conn, nil
	}
	entry, err := m.pool.Acquire(connCtx, tenantID, srv.Name, srv.Transport, srv.Command,
		rs.args, rs.env, srv.URL, rs.headers, srv.TimeoutSec, opt)
	if err != nil {
		return nil, err
	}
	key := poolKey(tenantID, srv.Name)
	conn.entry = entry
	conn.release = func() { m.pool.Release(key) }
	return conn, nil
}

// StartTask records a task that a modern tools/call started, starts its
// watcher, and returns the tool result at once.
func (m *TaskWatchManager) StartTask(ctx context.Context, start TaskStart) *tools.Result {
	task := start.Task
	status := task.Status
	if status == "" {
		status = TaskStatusWorking
	}
	origin, hasOrigin := ACPOriginFromContext(ctx)
	thread, threadOK := sanitizeThread(origin.Thread)
	if hasOrigin && !threadOK {
		slog.Warn("security.mcp.task_thread_rejected", "server", start.ServerName, "task_id", task.TaskID,
			"bytes", len(origin.Thread), "reason", "not a JSON object of at most 4 KiB")
	}

	agentID := store.AgentIDFromContext(ctx)
	if start.ServerID == uuid.Nil || agentID == uuid.Nil {
		// A config-file server has no row to point at; the task cannot be
		// watched or found by mcp_task.
		slog.Info("mcp.task.not_recorded", "server", start.ServerName, "task_id", task.TaskID, "reason", "no server or agent id")
		return startedResult(start, status, "goclaw does not watch tasks of this server.")
	}

	capsJSON, err := json.Marshal(capsFromOpts(start.Capabilities))
	if err != nil {
		capsJSON = []byte(`{}`)
	}
	row := store.MCPTask{
		AgentID:       agentID,
		ServerID:      start.ServerID,
		TaskID:        task.TaskID,
		SessionKey:    tools.ToolSessionKeyFromCtx(ctx),
		ACPSessionID:  origin.SessionID,
		UserID:        store.UserIDFromContext(ctx),
		Status:        status,
		EventCursor:   nonEmptyCursor(start.EventCursor),
		RequestCaps:   capsJSON,
		ThreadContext: thread,
		ExpiresAt:     taskExpiry(task.TTLMs, time.Now()),
	}
	if task.StatusMessage != "" {
		msg := task.StatusMessage
		row.StatusMessage = &msg
	}
	if err := m.tasks.CreateTask(ctx, &row); err != nil {
		slog.Warn("mcp.task.record_failed", "server", start.ServerName, "task_id", task.TaskID, "error", err.Error())
		return startedResult(start, status, "goclaw could not record the task, so it does not watch it.")
	}
	m.watch(row)
	slog.Info("mcp.task.watch_started", "task_row", row.ID, "server", start.ServerName, "task_id", task.TaskID,
		"acp_origin", row.HasACPOrigin(), "cursor", row.EventCursor != nil)

	if row.HasACPOrigin() {
		return startedResult(start, status, "You will be woken in this thread when it needs input or finishes.")
	}
	return startedResult(start, status, fmt.Sprintf(
		"Call mcp_task with action \"status\" and task_id %q to check it.", task.TaskID))
}

// startedResult is the tool result of a started task. The task id and
// status come from the server, so the text is wrapped as untrusted content.
func startedResult(start TaskStart, status, next string) *tools.Result {
	text := fmt.Sprintf("Archon/MCP task %s started (status %s). %s", start.Task.TaskID, status, next)
	return tools.NewResult(wrapMCPContent(text, start.ServerName, start.ToolName))
}

// taskExpiry returns now + ttlMs (at most one year), or now + 7 days when
// ttlMs is absent or not positive.
func taskExpiry(ttlMs *int64, now time.Time) time.Time {
	if ttlMs == nil || *ttlMs <= 0 {
		return now.Add(defaultTaskTTL)
	}
	if *ttlMs >= maxTaskTTL.Milliseconds() {
		return now.Add(maxTaskTTL)
	}
	return now.Add(time.Duration(*ttlMs) * time.Millisecond)
}
