package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

// loadAgentTools loads the fixture server for the agent through a Manager
// on the fixture pool, as the agent resolver does.
func (f *taskFixture) loadAgentTools(ctx context.Context) (*Manager, *tools.Registry) {
	f.t.Helper()
	f.pool.SetTaskWatch(f.mgr)
	srv, err := f.servers.GetServer(ctx, f.serverID)
	if err != nil {
		f.t.Fatal(err)
	}
	reg := tools.NewRegistry()
	st := &mockMCPStore{accessible: []store.MCPAccessInfo{{Server: *srv}}}
	m := NewManager(reg, WithStore(st), WithPool(f.pool))
	if err := m.LoadForAgent(ctx, f.agentID, ""); err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(m.Stop)
	return m, reg
}

func TestRunWorkflowReturnsAtOnceAndStartsWatcher(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	ctx := f.ctx(true)
	_, reg := f.loadAgentTools(ctx)
	tool, ok := reg.Get("mcp_archon_mcp__run_workflow")
	if !ok {
		t.Fatal("run_workflow is not registered")
	}

	start := time.Now()
	res := tool.Execute(ctx, map[string]any{})
	if res.IsError {
		t.Fatalf("tool error: %s", res.ForLLM)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("tool took %v; it must return at once", elapsed)
	}
	want := "Archon/MCP task task-1 started (status working). You will be woken in this thread when it needs input or finishes."
	if !strings.Contains(res.ForLLM, want) || !strings.Contains(res.ForLLM, "<<<EXTERNAL_UNTRUSTED_CONTENT>>>") {
		t.Fatalf("result = %q", res.ForLLM)
	}

	row, err := f.tasks.GetTaskForAgent(ctx, f.agentID, "task-1")
	if err != nil || row == nil {
		t.Fatalf("no row: %v", err)
	}
	if row.ServerID != f.serverID || row.UserID != "user-1" || row.ACPSessionID != "acp-sess-1" ||
		string(row.ThreadContext) != `{"channelId":"ch","rootEventId":"ev"}` || row.Status != TaskStatusWorking {
		t.Fatalf("row = %+v", row)
	}
	if ttl := time.Until(row.ExpiresAt); ttl < 29*24*time.Hour || ttl > 31*24*time.Hour {
		t.Errorf("expires in %v, want about 30 days from ttlMs", ttl)
	}
	caps := parseRequestCaps(row.RequestCaps)
	if !caps.Tasks || !caps.Events || caps.ElicitationForm {
		t.Errorf("request caps = %+v", caps)
	}
	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodEventsStream) >= 1 }, "watcher did not start")
	if !f.mgr.watching(row.ID) {
		t.Error("no watcher for the row")
	}
}

func TestRunWorkflowWithoutACPOriginPointsToStatus(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	ctx := f.ctx(false)
	_, reg := f.loadAgentTools(ctx)
	tool, _ := reg.Get("mcp_archon_mcp__run_workflow")
	res := tool.Execute(ctx, map[string]any{})
	if res.IsError || !strings.Contains(res.ForLLM, `Call mcp_task with action "status" and task_id "task-1"`) {
		t.Fatalf("result = %q", res.ForLLM)
	}
}

func TestBootstrapPollBeforeToolsCall(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	f.srv.QueuePoll(testEvent, mcptest.PollReply{Cursor: new("boot-1"), NextPollMs: 1000})
	ctx := f.ctx(true)
	_, reg := f.loadAgentTools(ctx)
	tool, _ := reg.Get("mcp_archon_mcp__run_workflow")
	if res := tool.Execute(ctx, map[string]any{}); res.IsError {
		t.Fatal(res.ForLLM)
	}
	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodEventsStream) >= 1 }, "no events/stream")

	order := map[string]int{}
	for i, m := range f.srv.Methods() {
		if _, seen := order[m]; !seen {
			order[m] = i
		}
	}
	if !(order[methodEventsPoll] < order[methodToolsCall] && order[methodToolsCall] < order[methodEventsStream]) {
		t.Fatalf("request order %v", f.srv.Methods())
	}
	poll := f.srv.RequestsFor(methodEventsPoll)[0]
	var cursor *string
	var args map[string]string
	_ = poll.Param("cursor", &cursor)
	_ = poll.Param("arguments", &args)
	if cursor != nil || args["workflow"] != "mcp-smoke" || poll.Header.Get("Mcp-Name") != testEvent {
		t.Fatalf("bootstrap poll cursor %v args %v header %q", cursor, args, poll.Header.Get("Mcp-Name"))
	}
	if cs := streamCursors(f.srv); deref(cs[0]) != "boot-1" {
		t.Fatalf("first events/stream cursor = %s, want boot-1", deref(cs[0]))
	}
}

func TestBootstrapPollFailureDoesNotBlockCall(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	f.srv.Handle(methodEventsPoll, func(context.Context, mcptest.Request) mcptest.Reply {
		return mcptest.ErrorReply(CodeEventsForbidden, "no", nil)
	})
	ctx := f.ctx(true)
	_, reg := f.loadAgentTools(ctx)
	tool, _ := reg.Get("mcp_archon_mcp__run_workflow")
	if res := tool.Execute(ctx, map[string]any{}); res.IsError {
		t.Fatalf("tool error: %s", res.ForLLM)
	}
	row, _ := f.tasks.GetTaskForAgent(ctx, f.agentID, "task-1")
	if row == nil || row.EventCursor != nil {
		t.Fatalf("row = %+v, want a row without a cursor", row)
	}
}

func TestNoBootstrapPollWithoutTaskEventsSetting(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{settings: `{}`})
	ctx := f.ctx(true)
	_, reg := f.loadAgentTools(ctx)
	tool, _ := reg.Get("mcp_archon_mcp__run_workflow")
	if res := tool.Execute(ctx, map[string]any{}); res.IsError {
		t.Fatal(res.ForLLM)
	}
	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodTasksGet) >= 1 }, "watcher does not poll tasks/get")
	for _, m := range f.srv.Methods() {
		if strings.HasPrefix(m, "events/") {
			t.Fatalf("server without a task_events setting got %s", m)
		}
	}
}

func TestWatchManagerResumesOpenRowsAtStart(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	a := f.srv.CreateTask("run-a")
	b := f.srv.CreateTask("run-b")
	f.srv.CreateTask("run-done")
	f.srv.CreateTask("run-old")
	rowA := f.openRow(a.TaskID, true, nil)
	rowB := f.openRow(b.TaskID, false, nil)
	done := f.openRow("run-done", true, nil)
	now := time.Now()
	done.TerminalAt = &now
	f.tasks.put(done)
	old := f.openRow("run-old", true, nil)
	old.ExpiresAt = now.Add(-time.Minute)
	f.tasks.put(old)

	if err := f.mgr.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !f.mgr.watching(rowA.ID) || !f.mgr.watching(rowB.ID) || f.mgr.watching(done.ID) || f.mgr.watching(old.ID) {
		t.Fatal("Start must resume exactly the open rows")
	}
	polled := func(id string) bool {
		for _, r := range f.srv.RequestsFor(methodTasksGet) {
			var tid string
			if r.Param("taskId", &tid) == nil && tid == id {
				return true
			}
		}
		return false
	}
	eventually(t, 3*time.Second, func() bool { return polled("run-a") && polled("run-b") }, "resumed rows are not polled")
	if polled("run-done") || polled("run-old") {
		t.Error("a terminal or expired row was polled")
	}
}

func TestMCPTaskRegisteredWithoutConnection(t *testing.T) {
	tasks := newMemTaskStore()
	pool := NewPool(PoolConfig{})
	t.Cleanup(pool.Stop)
	mgr, err := NewTaskWatchManager(TaskWatchConfig{Tasks: tasks, Servers: &fakeServerStore{}, Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mgr.Stop)
	pool.SetTaskWatch(mgr)
	tenant, agent := uuid.New(), uuid.New()
	ctx := store.WithTenantID(context.Background(), tenant)

	server := func(transport, settings string, enabled bool) store.MCPAccessInfo {
		return store.MCPAccessInfo{Server: store.MCPServerData{Name: "s", Transport: transport, Settings: json.RawMessage(settings), Enabled: enabled}}
	}
	cases := []struct {
		name       string
		accessible []store.MCPAccessInfo
		openRow    bool
		want       bool
	}{
		{"streamable http auto", []store.MCPAccessInfo{server("streamable-http", `{}`, true)}, false, true},
		{"sse", []store.MCPAccessInfo{server("sse", `{"protocol":"auto"}`, true)}, false, false},
		{"legacy setting", []store.MCPAccessInfo{server("streamable-http", `{"protocol":"legacy"}`, true)}, false, false},
		{"stdio", []store.MCPAccessInfo{server("stdio", `{}`, true)}, false, false},
		{"disabled", []store.MCPAccessInfo{server("streamable-http", `{}`, false)}, false, false},
		{"open row only", nil, true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.openRow {
				tasks.put(store.MCPTask{ID: uuid.New(), TenantID: tenant, AgentID: agent, TaskID: "x", Status: "working", ExpiresAt: time.Now().Add(time.Hour)})
			}
			reg := tools.NewRegistry()
			m := NewManager(reg, WithPool(pool))
			m.maybeRegisterTaskTool(ctx, agent, c.accessible)
			_, got := reg.Get(mcpTaskToolName)
			if got != c.want {
				t.Fatalf("mcp_task registered = %v, want %v", got, c.want)
			}
			// A reload removes it with the other MCP tools.
			m.unregisterAllTools()
			if _, still := reg.Get(mcpTaskToolName); still {
				t.Error("mcp_task stays after unregisterAllTools")
			}
		})
	}
}

func TestParseTaskEvents(t *testing.T) {
	b := ParseTaskEvents(json.RawMessage(defaultTaskSettings))
	if b == nil || b.Event != testEvent || b.TaskArgument != "run_id" || string(b.BootstrapArguments) != `{"workflow":"mcp-smoke"}` ||
		b.HeartbeatSec != 30 || b.SafetyPollSec != 300 {
		t.Fatalf("binding = %+v", b)
	}
	for _, bad := range []string{``, `{}`, `{"task_events":{}}`, `{"task_events":{"event":"e"}}`, `{"task_events":"x"}`, `not json`} {
		if got := ParseTaskEvents(json.RawMessage(bad)); got != nil {
			t.Errorf("ParseTaskEvents(%q) = %+v, want nil", bad, got)
		}
	}
	b = ParseTaskEvents(json.RawMessage(`{"task_events":{"event":"e","task_argument":"a","bootstrap_arguments":[1],"heartbeat_sec":2,"safety_poll_sec":60}}`))
	if b == nil || string(b.BootstrapArguments) != `{}` || b.HeartbeatSec != 30 || b.SafetyPollSec != 60 {
		t.Fatalf("defensive parse = %+v", b)
	}
}

func TestStartTaskRejectsLargeThread(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	big := `{"x":"` + strings.Repeat("a", maxThreadContextBytes) + `"}`
	ctx := WithACPOrigin(f.ctx(false), ACPOrigin{SessionID: "s", Thread: json.RawMessage(big)})
	task := f.srv.CreateTask("run-1")
	res := f.mgr.StartTask(ctx, TaskStart{ServerName: "archon-mcp", ServerID: f.serverID, ToolName: "run_workflow",
		Task: CreateTaskResult{TaskID: task.TaskID, Status: "working"}})
	if res.IsError {
		t.Fatal(res.ForLLM)
	}
	row, _ := f.tasks.GetTaskForAgent(ctx, f.agentID, task.TaskID)
	if row == nil || row.ThreadContext != nil || row.ACPSessionID != "s" {
		t.Fatalf("row = %+v, want no thread and the session id", row)
	}
	if ttl := time.Until(row.ExpiresAt); ttl < 6*24*time.Hour || ttl > 8*24*time.Hour {
		t.Errorf("expiry without ttlMs = %v, want 7 days", ttl)
	}
}
