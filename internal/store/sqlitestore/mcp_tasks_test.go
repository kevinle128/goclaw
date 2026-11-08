//go:build sqlite || sqliteonly

package sqlitestore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

func newSQLiteMCPTaskFixture(t *testing.T) (*SQLiteMCPTaskStore, context.Context, uuid.UUID, uuid.UUID) {
	t.Helper()
	db := openTestDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	tenantID := store.MasterTenantID
	agentID := uuid.Must(uuid.NewV7())
	serverID := uuid.Must(uuid.NewV7())
	seedSQLiteRunTimelineAgent(t, db, tenantID, agentID)
	if _, err := db.Exec(
		`INSERT INTO mcp_servers (id, name, transport, url, created_by, tenant_id) VALUES (?, ?, 'streamable-http', 'http://x', 'test', ?)`,
		serverID, "srv-"+serverID.String()[:8], tenantID,
	); err != nil {
		t.Fatalf("seed server: %v", err)
	}
	return NewSQLiteMCPTaskStore(db), store.WithTenantID(context.Background(), tenantID), agentID, serverID
}

func TestSQLiteMCPTaskStoreRowLifecycle(t *testing.T) {
	s, ctx, agentID, serverID := newSQLiteMCPTaskFixture(t)
	cursor := "c-0"
	row := &store.MCPTask{
		AgentID: agentID, ServerID: serverID, TaskID: "run-1", SessionKey: "sess",
		ACPSessionID: "acp-1", UserID: "u1", Status: "working", EventCursor: &cursor,
		RequestCaps:   json.RawMessage(`{"tasks":true}`),
		ThreadContext: json.RawMessage(`{"channelId":"c","rootEventId":"r"}`),
		ExpiresAt:     time.Now().Add(time.Hour),
	}
	if err := s.CreateTask(ctx, row); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	dup := *row
	dup.ID = uuid.Nil
	if err := s.CreateTask(ctx, &dup); !errors.Is(err, store.ErrMCPTaskExists) {
		t.Fatalf("duplicate CreateTask err = %v, want ErrMCPTaskExists", err)
	}

	got, err := s.GetTaskForAgent(ctx, agentID, "run-1")
	if err != nil || got == nil {
		t.Fatalf("GetTaskForAgent: %v %v", got, err)
	}
	if got.ID != row.ID || got.EventCursor == nil || *got.EventCursor != "c-0" || !got.HasACPOrigin() {
		t.Fatalf("row = %+v", got)
	}
	if string(got.ThreadContext) != `{"channelId":"c","rootEventId":"r"}` {
		t.Errorf("thread = %s", got.ThreadContext)
	}

	other := store.WithTenantID(context.Background(), uuid.Must(uuid.NewV7()))
	if r, err := s.GetTask(other, row.ID); err != nil || r != nil {
		t.Fatalf("other tenant GetTask = %v, %v; want nil", r, err)
	}

	if err := s.UpdateTaskCursor(ctx, row.ID, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskCursor(ctx, row.ID, "c-7"); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b", "a"} {
		if err := s.AddAnsweredKey(ctx, row.ID, k); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RemoveAnsweredKey(ctx, row.ID, "a"); err != nil {
		t.Fatal(err)
	}
	changed, err := s.MarkPRWoken(ctx, row.ID)
	if err != nil || !changed {
		t.Fatalf("first MarkPRWoken = %v, %v", changed, err)
	}
	if changed, _ := s.MarkPRWoken(ctx, row.ID); changed {
		t.Error("second MarkPRWoken changed the row")
	}
	got, _ = s.GetTask(ctx, row.ID)
	if *got.EventCursor != "c-7" || len(got.AnsweredKeys) != 1 || got.AnsweredKeys[0] != "b" || !got.PRWoken {
		t.Fatalf("after updates: cursor %v keys %v pr %v", *got.EventCursor, got.AnsweredKeys, got.PRWoken)
	}

	if open, _ := s.HasOpenTaskForAgent(ctx, agentID); !open {
		t.Error("HasOpenTaskForAgent = false for an open row")
	}
	if list, err := s.ListOpenTasks(context.Background()); err != nil || len(list) != 1 {
		t.Fatalf("ListOpenTasks = %d rows, %v", len(list), err)
	}
	msg := "done"
	if err := s.UpdateTaskStatus(ctx, row.ID, store.MCPTaskStatusUpdate{Status: "completed", StatusMessage: &msg, Terminal: true}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetTask(ctx, row.ID)
	if got.Status != "completed" || got.TerminalAt == nil || got.StatusMessage == nil || *got.StatusMessage != "done" {
		t.Fatalf("after terminal: %+v", got)
	}
	if list, _ := s.ListOpenTasks(context.Background()); len(list) != 0 {
		t.Errorf("ListOpenTasks after terminal = %d rows", len(list))
	}
}

func TestSQLiteMCPTaskStoreWakes(t *testing.T) {
	s, ctx, agentID, serverID := newSQLiteMCPTaskFixture(t)
	row := &store.MCPTask{AgentID: agentID, ServerID: serverID, TaskID: "run-2", Status: "input_required",
		ACPSessionID: "acp", ExpiresAt: time.Now().Add(time.Hour)}
	if err := s.CreateTask(ctx, row); err != nil {
		t.Fatal(err)
	}
	wake := &store.MCPTaskWake{
		WakeID:    store.MCPWakeID(row.ID, store.MCPWakeReasonInputRequired, "approval-gate"),
		TaskRowID: row.ID, ThreadKey: "tk", Reason: store.MCPWakeReasonInputRequired,
	}
	created, err := s.CreateWake(ctx, wake)
	if err != nil || !created {
		t.Fatalf("CreateWake = %v, %v", created, err)
	}
	again := *wake
	if created, err := s.CreateWake(ctx, &again); err != nil || created {
		t.Fatalf("second CreateWake = %v, %v; want not created", created, err)
	}
	other := store.WithTenantID(context.Background(), uuid.Must(uuid.NewV7()))
	foreign := *wake
	foreign.WakeID = "w_foreign"
	if created, _ := s.CreateWake(other, &foreign); created {
		t.Error("a wake for a task row of another tenant was created")
	}

	due, err := s.ListDueWakes(context.Background(), time.Now().Add(time.Second), 10)
	if err != nil || len(due) != 1 || due[0].Task.ID != row.ID || due[0].Wake.State != store.MCPWakeStatePending {
		t.Fatalf("ListDueWakes = %+v, %v", due, err)
	}
	if changed, err := s.MarkWakeOutcome(ctx, wake.WakeID, store.MCPWakeStateDelivered, time.Now()); err != nil || !changed {
		t.Fatalf("MarkWakeOutcome = %v, %v; want changed", changed, err)
	}
	// A wake that ended stays ended: a late outcome does not change it.
	if changed, err := s.MarkWakeOutcome(ctx, wake.WakeID, store.MCPWakeStatePending, time.Now()); err != nil || changed {
		t.Fatalf("MarkWakeOutcome on a delivered wake = %v, %v; want unchanged", changed, err)
	}
	stats, err := s.WakeThreadStats(ctx, "tk", time.Now().Add(-time.Hour))
	if err != nil || stats.LastDeliveredAt == nil || stats.DeliveredSince != 1 {
		t.Fatalf("WakeThreadStats = %+v, %v", stats, err)
	}
	if due, _ := s.ListDueWakes(context.Background(), time.Now().Add(time.Second), 10); len(due) != 0 {
		t.Errorf("delivered wake still due: %+v", due)
	}

	pending := &store.MCPTaskWake{WakeID: store.MCPWakeID(row.ID, "completed", "completed"), TaskRowID: row.ID,
		ThreadKey: "tk", Reason: store.MCPWakeReasonCompleted}
	if _, err := s.CreateWake(ctx, pending); err != nil {
		t.Fatal(err)
	}
	n, err := s.ExpireWakesForTask(ctx, row.ID)
	if err != nil || n != 1 {
		t.Fatalf("ExpireWakesForTask = %d, %v; want 1", n, err)
	}
	wakes, err := s.ListWakesForTask(ctx, row.ID)
	if err != nil || len(wakes) != 2 {
		t.Fatalf("ListWakesForTask = %d, %v", len(wakes), err)
	}
	states := map[string]string{}
	for _, w := range wakes {
		states[w.WakeID] = w.State
	}
	if states[wake.WakeID] != store.MCPWakeStateDelivered || states[pending.WakeID] != store.MCPWakeStateExpired {
		t.Errorf("states = %v", states)
	}
	if wakes[0].Attempts+wakes[1].Attempts != 1 {
		t.Errorf("attempts = %d + %d, want one attempt in total", wakes[0].Attempts, wakes[1].Attempts)
	}
	if changed, err := s.MarkWakeOutcome(ctx, pending.WakeID, store.MCPWakeStateNoSink, time.Now()); err != nil || changed {
		t.Fatalf("MarkWakeOutcome on an expired wake = %v, %v; want unchanged", changed, err)
	}
	if wakes, _ := s.ListWakesForTask(ctx, row.ID); len(wakes) != 2 || wakes[0].State == store.MCPWakeStateNoSink || wakes[1].State == store.MCPWakeStateNoSink {
		t.Errorf("an expired wake changed state: %+v", wakes)
	}
}
