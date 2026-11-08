package mcp

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

func TestWatcherPersistsCursorFromActiveEventHeartbeat(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	task := f.srv.CreateTask("run-1")
	f.srv.SetStreamScript(func(ctx context.Context, st *mcptest.Stream, _ mcptest.Request) {
		_ = st.Active(new("c1"), false)
		_ = st.Heartbeat(nil) // a null cursor is not stored
		_ = st.Event(mcptest.EventOccurrence{EventID: "run-1:1", Name: testEvent, Data: map[string]any{"status": "running"}, Cursor: new("c2")})
		_ = st.Heartbeat(new("c3"))
		<-ctx.Done()
	})
	row := f.openRow(task.TaskID, true, nil)
	f.mgr.watch(row)

	eventually(t, 3*time.Second, func() bool { return len(f.tasks.cursorHistory()) == 3 }, "cursor history %v", f.tasks.cursorHistory())
	if got := f.tasks.cursorHistory(); !slices.Equal(got, []string{"c1", "c2", "c3"}) {
		t.Fatalf("cursors = %v, want [c1 c2 c3]", got)
	}
	if c := f.tasks.snapshot(row.ID).EventCursor; c == nil || *c != "c3" {
		t.Errorf("row cursor = %v, want c3", c)
	}
	// The first stream starts from no cursor (the row had none).
	if cs := streamCursors(f.srv); len(cs) == 0 || cs[0] != nil {
		t.Errorf("first events/stream cursor = %v, want null", cs)
	}
}

func TestWatcherReconnectsWithCursorAfterDrop(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	task := f.srv.CreateTask("run-1")
	var calls atomic.Int32
	f.srv.SetStreamScript(func(ctx context.Context, st *mcptest.Stream, req mcptest.Request) {
		if calls.Add(1) == 1 {
			_ = st.Active(new("c1"), false)
			_ = st.Event(mcptest.EventOccurrence{EventID: "run-1:1", Name: testEvent, Data: map[string]any{}, Cursor: new("c2")})
			return // the connection drops without a final response
		}
		_ = st.Active(new("c5"), false)
		<-ctx.Done()
	})
	f.mgr.watch(f.openRow(task.TaskID, true, new("c0")))

	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodEventsStream) >= 2 }, "no reconnect")
	cs := streamCursors(f.srv)
	if cs[0] == nil || *cs[0] != "c0" || cs[1] == nil || *cs[1] != "c2" {
		t.Fatalf("stream cursors = [%v %v], want [c0 c2]", deref(cs[0]), deref(cs[1]))
	}
	// Arguments carry the task id under the task_argument of the setting.
	r := f.srv.RequestsFor(methodEventsStream)[1]
	var args map[string]string
	if err := r.Param("arguments", &args); err != nil || args["run_id"] != "run-1" {
		t.Errorf("arguments = %v (%v), want run_id=run-1", args, err)
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func TestWatcherTreatsHeartbeatSilenceAsDead(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	f.mgr.timing.heartbeat = 50 * time.Millisecond // silence > 100 ms is dead
	task := f.srv.CreateTask("run-1")
	var firstClosed atomic.Bool
	var calls atomic.Int32
	f.srv.SetStreamScript(func(ctx context.Context, st *mcptest.Stream, _ mcptest.Request) {
		n := calls.Add(1)
		_ = st.Active(new("c1"), false)
		<-ctx.Done() // silent: no heartbeat
		if n == 1 {
			firstClosed.Store(true)
		}
	})
	f.mgr.watch(f.openRow(task.TaskID, true, nil))

	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodEventsStream) >= 2 }, "silent stream was not replaced")
	eventually(t, 2*time.Second, firstClosed.Load, "silent stream was not aborted")
	if cs := streamCursors(f.srv); cs[1] == nil || *cs[1] != "c1" {
		t.Errorf("reconnect cursor = %v, want c1", deref(cs[1]))
	}
}

func TestWatcherFallsBackToEventsPoll(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	task := f.srv.CreateTask("run-1")
	f.srv.Handle(methodEventsStream, func(context.Context, mcptest.Request) mcptest.Reply {
		return mcptest.ErrorReply(CodeMethodNotFound, "events/stream not supported", nil)
	})
	f.srv.QueuePoll(testEvent, mcptest.PollReply{Cursor: new("p1"), NextPollMs: 10})
	f.srv.QueuePoll(testEvent, mcptest.PollReply{
		Events: []mcptest.EventOccurrence{{EventID: "run-1:1", Name: testEvent, Data: map[string]any{"status": "completed"}}},
		Cursor: new("p2"), NextPollMs: 10,
	})
	row := f.openRow(task.TaskID, true, new("c0"))
	f.mgr.watch(row)

	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodEventsPoll) >= 2 }, "no events/poll")
	polls := f.srv.RequestsFor(methodEventsPoll)
	var c1, c2 *string
	_ = polls[0].Param("cursor", &c1)
	_ = polls[1].Param("cursor", &c2)
	if deref(c1) != "c0" || deref(c2) != "p1" {
		t.Fatalf("poll cursors = %s, %s; want c0, p1", deref(c1), deref(c2))
	}
	f.srv.CompleteTask(task.TaskID, map[string]any{"content": []any{map[string]any{"type": "text", "text": "ok"}}})
	f.srv.QueuePoll(testEvent, mcptest.PollReply{
		Events: []mcptest.EventOccurrence{{EventID: "run-1:2", Name: testEvent, Data: map[string]any{}}},
		Cursor: new("p3"), NextPollMs: 10,
	})
	eventually(t, 3*time.Second, func() bool { return f.tasks.snapshot(row.ID).TerminalAt != nil }, "task did not end")
	if got := wakeReasons(f.tasks.wakeList(row.ID)); !slices.Equal(got, []string{store.MCPWakeReasonCompleted}) {
		t.Errorf("wakes = %v", got)
	}
}

func TestWatcherPollsTasksGetWithoutEvents(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	task := f.srv.CreateTask("run-1")
	row := f.openRow(task.TaskID, true, nil)
	f.mgr.watch(row)

	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodTasksGet) >= 3 }, "tasks/get is not polled")
	f.srv.FailTask(task.TaskID, -32000, "boom")
	eventually(t, 3*time.Second, func() bool { return f.tasks.snapshot(row.ID).TerminalAt != nil }, "task did not end")
	for _, m := range f.srv.Methods() {
		if strings.HasPrefix(m, "events/") {
			t.Fatalf("server without the events extension got %s", m)
		}
	}
	if got := wakeReasons(f.tasks.wakeList(row.ID)); !slices.Equal(got, []string{store.MCPWakeReasonFailed}) {
		t.Errorf("wakes = %v", got)
	}
	// Every tasks/get names the task in Mcp-Name and declares the tasks extension.
	for _, r := range f.srv.RequestsFor(methodTasksGet) {
		if r.Header.Get("Mcp-Name") != task.TaskID || !r.HasExtension(mcptest.ExtensionTasks) {
			t.Fatalf("tasks/get headers %v caps %v", r.Header, r.ClientCapabilities())
		}
	}
}

func TestWatcherIgnoresEventDataForState(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	task := f.srv.CreateTask("run-1")
	f.srv.SetStreamScript(func(ctx context.Context, st *mcptest.Stream, _ mcptest.Request) {
		_ = st.Active(new("c1"), false)
		_ = st.Event(mcptest.EventOccurrence{EventID: "run-1:9", Name: testEvent,
			Data: map[string]any{"status": "completed", "task_status": "completed"}, Cursor: new("c2")})
		<-ctx.Done()
	})
	row := f.openRow(task.TaskID, true, nil)
	f.mgr.watch(row)

	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodTasksGet) >= 2 }, "no tasks/get after the event")
	time.Sleep(50 * time.Millisecond)
	snap := f.tasks.snapshot(row.ID)
	if snap.Status != TaskStatusWorking || snap.TerminalAt != nil {
		t.Fatalf("row status %s terminal %v; event data must not set the state", snap.Status, snap.TerminalAt)
	}
	if n := len(f.tasks.wakeList(row.ID)); n != 0 {
		t.Errorf("wakes = %d, want 0", n)
	}
}

func TestWatcherDeduplicatesEventID(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	task := f.srv.CreateTask("run-1")
	sent := make(chan struct{})
	f.srv.SetStreamScript(func(ctx context.Context, st *mcptest.Stream, _ mcptest.Request) {
		_ = st.Active(new("c1"), false)
		ev := mcptest.EventOccurrence{EventID: "run-1:1", Name: testEvent, Data: map[string]any{}}
		_ = st.Event(ev)
		_ = st.Event(ev) // at-least-once delivery repeats it
		_ = st.Event(mcptest.EventOccurrence{EventID: "run-1:2", Name: testEvent, Data: map[string]any{}})
		_ = st.Heartbeat(new("c9"))
		close(sent)
		<-ctx.Done()
	})
	row := f.openRow(task.TaskID, true, nil)
	f.mgr.watch(row)

	<-sent
	eventually(t, 3*time.Second, func() bool { return deref(f.tasks.snapshot(row.ID).EventCursor) == "c9" }, "frames not handled")
	// One tasks/get after the active frame and one per distinct event.
	if n := countFor(f.srv, methodTasksGet); n != 3 {
		t.Errorf("tasks/get count = %d, want 3", n)
	}
}

func TestWakeTriggersInputRequiredPerKeyOnce(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	task := f.srv.CreateTask("run-1")
	form := mcptest.InputRequest{Method: "elicitation/create", Params: map[string]any{
		"mode": "form", "message": "Approve?", "requestedSchema": map[string]any{"type": "object", "properties": map[string]any{}},
	}}
	f.srv.RequireTaskInput(task.TaskID, "approval-gate", form)
	row := f.openRow(task.TaskID, true, nil)
	f.mgr.watch(row)

	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodTasksGet) >= 4 }, "no polls")
	if ws := f.tasks.wakeList(row.ID); len(ws) != 1 || ws[0].WakeID != store.MCPWakeID(row.ID, store.MCPWakeReasonInputRequired, "approval-gate") {
		t.Fatalf("wakes after repeated polls = %+v, want one for approval-gate", ws)
	}
	// An answered key gets no wake; a new key gets its own.
	if err := f.tasks.AddAnsweredKey(f.ctx(false), row.ID, "second"); err != nil {
		t.Fatal(err)
	}
	f.srv.RequireTaskInput(task.TaskID, "second", form)
	f.srv.RequireTaskInput(task.TaskID, "third", form)
	eventually(t, 3*time.Second, func() bool { return len(f.tasks.wakeList(row.ID)) == 2 }, "no wake for the new key")
	time.Sleep(60 * time.Millisecond)
	ws := f.tasks.wakeList(row.ID)
	if len(ws) != 2 || ws[1].WakeID != store.MCPWakeID(row.ID, store.MCPWakeReasonInputRequired, "third") {
		t.Fatalf("wakes = %+v, want approval-gate and third", ws)
	}
	if ws[0].ThreadKey != store.MCPWakeThreadKey(row.ThreadContext, row.ACPSessionID) {
		t.Errorf("thread key = %s", ws[0].ThreadKey)
	}
}

func TestWakeTriggersInputRequiredWithoutInputRequests(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	task := f.srv.CreateTask("run-1")
	f.srv.UpdateTask(task.TaskID, func(tk *mcptest.Task) {
		tk.Status = "input_required"
		tk.StatusMessage = "Approve the merge? Reply approve or reject."
	})
	updated, _ := f.srv.Task(task.TaskID)
	row := f.openRow(task.TaskID, true, nil)
	f.mgr.watch(row)

	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodTasksGet) >= 3 }, "no polls")
	ws := f.tasks.wakeList(row.ID)
	want := store.MCPWakeID(row.ID, store.MCPWakeReasonInputRequired, "status:"+updated.LastUpdatedAt)
	if len(ws) != 1 || ws[0].WakeID != want {
		t.Fatalf("wakes = %+v, want one with discriminator status:<lastUpdatedAt>", ws)
	}
}

func TestWakeTriggersPRReadyOnce(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{})
	task := f.srv.CreateTask("run-1")
	done := make(chan struct{})
	f.srv.SetStreamScript(func(ctx context.Context, st *mcptest.Stream, _ mcptest.Request) {
		_ = st.Active(new("c1"), false)
		_ = st.Event(mcptest.EventOccurrence{EventID: "run-1:1", Name: testEvent, Data: map[string]any{"pr_url": ""}})
		_ = st.Event(mcptest.EventOccurrence{EventID: "run-1:2", Name: testEvent, Data: map[string]any{"status": "running", "pr_url": "https://example.test/pr/1"}})
		_ = st.Event(mcptest.EventOccurrence{EventID: "run-1:3", Name: testEvent, Data: map[string]any{"status": "running", "pr_url": "https://example.test/pr/1"}})
		_ = st.Heartbeat(new("c9"))
		close(done)
		<-ctx.Done()
	})
	row := f.openRow(task.TaskID, true, nil)
	f.mgr.watch(row)

	<-done
	eventually(t, 3*time.Second, func() bool { return deref(f.tasks.snapshot(row.ID).EventCursor) == "c9" }, "frames not handled")
	ws := f.tasks.wakeList(row.ID)
	if len(ws) != 1 || ws[0].WakeID != store.MCPWakeID(row.ID, store.MCPWakeReasonPRReady, "pr") {
		t.Fatalf("wakes = %+v, want one pr_ready", ws)
	}
	if !f.tasks.snapshot(row.ID).PRWoken {
		t.Error("pr_woken is not set")
	}
}

func TestWakeTriggersTerminal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		finish func(srv *mcptest.Server, id string)
		want   string
	}{
		{"completed", func(srv *mcptest.Server, id string) { srv.CompleteTask(id, map[string]any{"content": []any{}}) }, store.MCPWakeReasonCompleted},
		{"failed", func(srv *mcptest.Server, id string) { srv.FailTask(id, -32000, "boom") }, store.MCPWakeReasonFailed},
		{"cancelled", func(srv *mcptest.Server, id string) {
			srv.UpdateTask(id, func(tk *mcptest.Task) { tk.Status = "cancelled" })
		}, store.MCPWakeReasonCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
			task := f.srv.CreateTask("run-1")
			row := f.openRow(task.TaskID, true, nil)
			f.mgr.watch(row)
			eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodTasksGet) >= 1 }, "no poll")
			tc.finish(f.srv, task.TaskID)

			eventually(t, 3*time.Second, func() bool { return !f.mgr.watching(row.ID) }, "watcher did not stop")
			snap := f.tasks.snapshot(row.ID)
			if snap.Status != tc.want || snap.TerminalAt == nil {
				t.Fatalf("row status %s terminal %v", snap.Status, snap.TerminalAt)
			}
			ws := f.tasks.wakeList(row.ID)
			if len(ws) != 1 || ws[0].WakeID != store.MCPWakeID(row.ID, tc.want, tc.want) || ws[0].State != store.MCPWakeStatePending {
				t.Fatalf("wakes = %+v", ws)
			}
		})
	}
}

func TestWakesOnlyForACPOrigin(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	task := f.srv.CreateTask("run-1")
	row := f.openRow(task.TaskID, false, nil)
	f.mgr.watch(row)
	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodTasksGet) >= 1 }, "no poll")
	f.srv.CompleteTask(task.TaskID, map[string]any{"content": []any{}})
	eventually(t, 3*time.Second, func() bool { return f.tasks.snapshot(row.ID).TerminalAt != nil }, "task did not end")
	if n := len(f.tasks.wakeList(row.ID)); n != 0 {
		t.Errorf("a row without an ACP origin got %d wakes", n)
	}
}

func TestWakeIDStableAcrossRestart(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	task := f.srv.CreateTask("run-1")
	f.srv.RequireTaskInput(task.TaskID, "approval-gate", mcptest.InputRequest{Method: "elicitation/create"})
	row := f.openRow(task.TaskID, true, nil)
	f.mgr.watch(row)
	eventually(t, 3*time.Second, func() bool { return len(f.tasks.wakeList(row.ID)) == 1 }, "no wake")
	first := f.tasks.wakeList(row.ID)[0].WakeID
	f.mgr.Stop()

	// A new gateway process resumes the row and sees the same trigger.
	before := countFor(f.srv, methodTasksGet)
	mgr2 := f.newManager()
	if err := mgr2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Second, func() bool { return countFor(f.srv, methodTasksGet) >= before+2 }, "resumed watcher does not poll")
	ws := f.tasks.wakeList(row.ID)
	if len(ws) != 1 || ws[0].WakeID != first {
		t.Fatalf("wakes after restart = %+v, want only %s", ws, first)
	}
}

func TestWatcherHoldsPoolReference(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	task := f.srv.CreateTask("run-1")
	row := f.openRow(task.TaskID, true, nil)
	f.mgr.watch(row)
	key := poolKey(f.tenantID, "archon-mcp")
	refs := func() int {
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		if e, ok := f.pool.servers[key]; ok {
			return e.refCount
		}
		return -1
	}
	eventually(t, 3*time.Second, func() bool { return refs() == 1 }, "watcher holds no pool reference (refs %d)", refs())
	f.mgr.Stop()
	if got := refs(); got != 0 {
		t.Fatalf("refs after stop = %d, want 0", got)
	}
	// Stop leaves the row as it was.
	if snap := f.tasks.snapshot(row.ID); snap.TerminalAt != nil || snap.Status != TaskStatusWorking {
		t.Errorf("stop changed the row: %+v", snap)
	}
}

func TestWatcherExpiryExpiresPendingWakes(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	task := f.srv.CreateTask("run-1")
	f.srv.RequireTaskInput(task.TaskID, "approval-gate", mcptest.InputRequest{Method: "elicitation/create"})
	row := f.openRow(task.TaskID, true, nil)
	row.ExpiresAt = time.Now().Add(300 * time.Millisecond)
	f.tasks.put(row)
	f.mgr.watch(row)

	eventually(t, 3*time.Second, func() bool { return len(f.tasks.wakeList(row.ID)) == 1 }, "no wake")
	eventually(t, 3*time.Second, func() bool { return !f.mgr.watching(row.ID) }, "watcher did not stop at expiry")
	if ws := f.tasks.wakeList(row.ID); ws[0].State != store.MCPWakeStateExpired {
		t.Fatalf("wake state = %s, want expired", ws[0].State)
	}
}

func TestWatcherStopsOnUnknownTask(t *testing.T) {
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	row := f.openRow("gone", true, nil)
	f.mgr.watch(row)
	eventually(t, 3*time.Second, func() bool { return !f.mgr.watching(row.ID) }, "watcher did not stop")
	snap := f.tasks.snapshot(row.ID)
	if snap.TerminalAt == nil || snap.Status != TaskStatusWorking {
		t.Fatalf("row = status %s terminal %v; want terminal_at set and status unchanged", snap.Status, snap.TerminalAt)
	}
}
