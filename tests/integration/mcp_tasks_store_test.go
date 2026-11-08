//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/store/pg"
)

func TestPGMCPTaskStoreRowLifecycle(t *testing.T) {
	db := testDB(t)
	tenantID, agentID := seedTenantAgent(t, db)
	serverID := seedMCPServer(t, db, tenantID)
	s := pg.NewPGMCPTaskStore(db)
	ctx := tenantCtx(tenantID)
	t.Cleanup(func() { db.Exec("DELETE FROM mcp_tasks WHERE tenant_id = $1", tenantID) })

	cursor := "c-0"
	row := &store.MCPTask{
		AgentID: agentID, ServerID: serverID, TaskID: "run-1", SessionKey: "sess",
		ACPSessionID: "acp-1", UserID: "u1", Status: "working", EventCursor: &cursor,
		RequestCaps:   json.RawMessage(`{"tasks":true,"events":true}`),
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
	if err != nil || got == nil || got.ID != row.ID || *got.EventCursor != "c-0" || !got.HasACPOrigin() {
		t.Fatalf("GetTaskForAgent = %+v, %v", got, err)
	}
	var thread map[string]string
	if err := json.Unmarshal(got.ThreadContext, &thread); err != nil || thread["channelId"] != "c" {
		t.Errorf("thread = %s", got.ThreadContext)
	}

	// Another tenant sees nothing, and cannot write.
	other := tenantCtx(uuid.New())
	if r, err := s.GetTask(other, row.ID); err != nil || r != nil {
		t.Fatalf("other tenant GetTask = %v, %v", r, err)
	}
	if r, _ := s.GetTaskForAgent(other, agentID, "run-1"); r != nil {
		t.Fatal("other tenant found the row by agent and task id")
	}
	_ = s.UpdateTaskCursor(other, row.ID, "foreign")
	if r, _ := s.GetTask(ctx, row.ID); *r.EventCursor != "c-0" {
		t.Fatal("another tenant changed the cursor")
	}
	if _, err := s.GetTask(context.Background(), row.ID); err == nil {
		t.Error("a query without a tenant must fail")
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
	if changed, err := s.MarkPRWoken(ctx, row.ID); err != nil || !changed {
		t.Fatalf("first MarkPRWoken = %v, %v", changed, err)
	}
	if changed, _ := s.MarkPRWoken(ctx, row.ID); changed {
		t.Error("second MarkPRWoken changed the row")
	}
	got, _ = s.GetTask(ctx, row.ID)
	if *got.EventCursor != "c-7" || len(got.AnsweredKeys) != 1 || got.AnsweredKeys[0] != "b" || !got.PRWoken {
		t.Fatalf("after updates: cursor %s keys %v pr %v", *got.EventCursor, got.AnsweredKeys, got.PRWoken)
	}

	if open, err := s.HasOpenTaskForAgent(ctx, agentID); err != nil || !open {
		t.Fatalf("HasOpenTaskForAgent = %v, %v", open, err)
	}
	if !openTaskListed(t, s, row.ID) {
		t.Fatal("ListOpenTasks misses the open row")
	}
	msg := "done"
	if err := s.UpdateTaskStatus(ctx, row.ID, store.MCPTaskStatusUpdate{Status: "completed", StatusMessage: &msg, Terminal: true}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetTask(ctx, row.ID)
	if got.Status != "completed" || got.TerminalAt == nil || *got.StatusMessage != "done" {
		t.Fatalf("after terminal: %+v", got)
	}
	if openTaskListed(t, s, row.ID) {
		t.Error("ListOpenTasks lists a terminal row")
	}
}

func openTaskListed(t *testing.T, s *pg.PGMCPTaskStore, id uuid.UUID) bool {
	t.Helper()
	rows, err := s.ListOpenTasks(context.Background())
	if err != nil {
		t.Fatalf("ListOpenTasks: %v", err)
	}
	for _, r := range rows {
		if r.ID == id {
			return true
		}
	}
	return false
}

func TestPGMCPTaskStoreWakes(t *testing.T) {
	db := testDB(t)
	tenantID, agentID := seedTenantAgent(t, db)
	serverID := seedMCPServer(t, db, tenantID)
	s := pg.NewPGMCPTaskStore(db)
	ctx := tenantCtx(tenantID)
	t.Cleanup(func() {
		db.Exec("DELETE FROM mcp_task_wakes WHERE tenant_id = $1", tenantID)
		db.Exec("DELETE FROM mcp_tasks WHERE tenant_id = $1", tenantID)
	})

	row := &store.MCPTask{AgentID: agentID, ServerID: serverID, TaskID: "run-2", Status: "input_required",
		ACPSessionID: "acp", ExpiresAt: time.Now().Add(time.Hour)}
	if err := s.CreateTask(ctx, row); err != nil {
		t.Fatal(err)
	}
	threadKey := "tk-" + tenantID.String()
	wake := &store.MCPTaskWake{
		WakeID:    store.MCPWakeID(row.ID, store.MCPWakeReasonInputRequired, "approval-gate"),
		TaskRowID: row.ID, ThreadKey: threadKey, Reason: store.MCPWakeReasonInputRequired,
	}
	if created, err := s.CreateWake(ctx, wake); err != nil || !created {
		t.Fatalf("CreateWake = %v, %v", created, err)
	}
	again := *wake
	if created, err := s.CreateWake(ctx, &again); err != nil || created {
		t.Fatalf("second CreateWake = %v, %v; want not created", created, err)
	}
	foreign := *wake
	foreign.WakeID = "w_foreign_" + tenantID.String()
	if created, _ := s.CreateWake(tenantCtx(uuid.New()), &foreign); created {
		t.Error("a wake for a task row of another tenant was created")
	}

	due, err := s.ListDueWakes(context.Background(), time.Now().Add(time.Second), 500)
	if err != nil {
		t.Fatal(err)
	}
	var mine *store.MCPTaskWakeDue
	for i := range due {
		if due[i].Wake.WakeID == wake.WakeID {
			mine = &due[i]
		}
	}
	if mine == nil || mine.Task.ID != row.ID || mine.Task.TaskID != "run-2" || mine.Wake.State != store.MCPWakeStatePending {
		t.Fatalf("ListDueWakes misses the wake: %+v", mine)
	}

	if changed, err := s.MarkWakeOutcome(ctx, wake.WakeID, store.MCPWakeStateDelivered, time.Now()); err != nil || !changed {
		t.Fatalf("MarkWakeOutcome = %v, %v; want changed", changed, err)
	}
	// A wake that ended stays ended: a late outcome does not change it.
	if changed, err := s.MarkWakeOutcome(ctx, wake.WakeID, store.MCPWakeStatePending, time.Now()); err != nil || changed {
		t.Fatalf("MarkWakeOutcome on a delivered wake = %v, %v; want unchanged", changed, err)
	}
	stats, err := s.WakeThreadStats(ctx, threadKey, time.Now().Add(-time.Hour))
	if err != nil || stats.LastDeliveredAt == nil || stats.DeliveredSince != 1 {
		t.Fatalf("WakeThreadStats = %+v, %v", stats, err)
	}

	pending := &store.MCPTaskWake{WakeID: store.MCPWakeID(row.ID, "completed", "completed"), TaskRowID: row.ID,
		ThreadKey: threadKey, Reason: store.MCPWakeReasonCompleted}
	if _, err := s.CreateWake(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ExpireWakesForTask(ctx, row.ID); err != nil || n != 1 {
		t.Fatalf("ExpireWakesForTask = %d, %v; want 1", n, err)
	}
	wakes, err := s.ListWakesForTask(ctx, row.ID)
	if err != nil || len(wakes) != 2 {
		t.Fatalf("ListWakesForTask = %d, %v", len(wakes), err)
	}
	states := map[string]store.MCPTaskWake{}
	for _, w := range wakes {
		states[w.WakeID] = w
	}
	if d := states[wake.WakeID]; d.State != store.MCPWakeStateDelivered || d.Attempts != 1 || d.DeliveredAt == nil {
		t.Errorf("delivered wake = %+v", d)
	}
	if e := states[pending.WakeID]; e.State != store.MCPWakeStateExpired {
		t.Errorf("pending wake = %+v, want expired", e)
	}
	if changed, err := s.MarkWakeOutcome(ctx, pending.WakeID, store.MCPWakeStateNoSink, time.Now()); err != nil || changed {
		t.Fatalf("MarkWakeOutcome on an expired wake = %v, %v; want unchanged", changed, err)
	}

	// Deleting the task row removes its wakes.
	if _, err := db.Exec("DELETE FROM mcp_tasks WHERE id = $1", row.ID); err != nil {
		t.Fatal(err)
	}
	if wakes, _ := s.ListWakesForTask(ctx, row.ID); len(wakes) != 0 {
		t.Errorf("wakes after task delete = %d", len(wakes))
	}
}
