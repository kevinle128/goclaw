package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// fakeRelay answers AskForm with scripted answers.
type fakeRelay struct {
	canAsk  bool
	mu      sync.Mutex
	answers []relayAnswer
	asked   []relayQuestion
}

type relayAnswer struct {
	action  string
	content string
}

type relayQuestion struct {
	toolCallID, message string
	schema              json.RawMessage
}

func (r *fakeRelay) CanAskForm() bool { return r.canAsk }

func (r *fakeRelay) AskForm(_ context.Context, toolCallID, message string, schema json.RawMessage) (string, json.RawMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.asked = append(r.asked, relayQuestion{toolCallID, message, schema})
	if len(r.answers) == 0 {
		return "cancel", nil, nil
	}
	a := r.answers[0]
	r.answers = r.answers[1:]
	return a.action, json.RawMessage(a.content), nil
}

var approvalForm = mcptest.InputRequest{Method: "elicitation/create", Params: map[string]any{
	"mode":    "form",
	"message": "Approve the merge of PR 7?",
	"requestedSchema": map[string]any{
		"type": "object",
		"properties": map[string]any{
			"decision": map[string]any{"type": "string", "enum": []any{"approve", "reject"}},
			"note":     map[string]any{"type": "string"},
		},
		"required": []any{"decision"},
	},
}}

// taskToolFixture is a fixture with one open task row and the mcp_task tool.
func newTaskToolFixture(t *testing.T) (*taskFixture, *MCPTaskTool, store.MCPTask) {
	t.Helper()
	f := newTaskFixture(t, fixtureOpts{extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}}})
	task := f.srv.CreateTask("run-1")
	row := f.openRow(task.TaskID, true, nil)
	return f, NewMCPTaskTool(f.mgr), row
}

func TestMCPTaskToolRejectsOtherTenantOrAgent(t *testing.T) {
	f, tool, _ := newTaskToolFixture(t)
	otherTenant := store.WithAgentID(store.WithTenantID(context.Background(), uuid.New()), f.agentID)
	otherAgent := store.WithAgentID(store.WithTenantID(context.Background(), f.tenantID), uuid.New())
	for name, ctx := range map[string]context.Context{"tenant": otherTenant, "agent": otherAgent} {
		for _, action := range []string{"status", "input", "cancel"} {
			res := tool.Execute(ctx, map[string]any{"action": action, "task_id": "run-1"})
			if !res.IsError || !strings.Contains(res.ForLLM, "not found") {
				t.Errorf("other %s, %s: %q", name, action, res.ForLLM)
			}
		}
	}
	if n := len(f.srv.Requests()); n != 0 {
		t.Errorf("server got %d requests for a foreign task", n)
	}
}

func TestMCPTaskStatusWrapsResult(t *testing.T) {
	f, tool, _ := newTaskToolFixture(t)
	f.srv.CompleteTask("run-1", map[string]any{
		"content": []any{map[string]any{"type": "text", "text": "pr_url: https://example.test/pr/7 ci: passed"}},
	})
	res := tool.Execute(f.ctx(true), map[string]any{"action": "status", "task_id": "run-1"})
	if res.IsError {
		t.Fatal(res.ForLLM)
	}
	for _, want := range []string{"<<<EXTERNAL_UNTRUSTED_CONTENT>>>", "status completed", "pr_url: https://example.test/pr/7 ci: passed"} {
		if !strings.Contains(res.ForLLM, want) {
			t.Errorf("status result misses %q: %s", want, res.ForLLM)
		}
	}

	f.srv.CreateTask("run-2")
	f.openRow("run-2", true, nil)
	f.srv.FailTask("run-2", -32000, "workflow node failed")
	res = tool.Execute(f.ctx(true), map[string]any{"action": "status", "task_id": "run-2"})
	if res.IsError || !strings.Contains(res.ForLLM, "workflow node failed") {
		t.Fatalf("failed status = %q", res.ForLLM)
	}
}

func TestMCPTaskCancelSendsTasksCancel(t *testing.T) {
	f, tool, _ := newTaskToolFixture(t)
	res := tool.Execute(f.ctx(true), map[string]any{"action": "cancel", "task_id": "run-1"})
	if res.IsError || !strings.Contains(res.ForLLM, "acknowledged") {
		t.Fatalf("cancel = %q", res.ForLLM)
	}
	reqs := f.srv.RequestsFor(methodTasksCancel)
	if len(reqs) != 1 || reqs[0].Header.Get("Mcp-Name") != "run-1" {
		t.Fatalf("tasks/cancel requests = %+v", reqs)
	}
}

func TestMCPTaskInputAcceptSendsTasksUpdateContentUnchanged(t *testing.T) {
	f, tool, row := newTaskToolFixture(t)
	f.srv.RequireTaskInput("run-1", "approval-gate", approvalForm)
	relay := &fakeRelay{canAsk: true, answers: []relayAnswer{{"accept", `{"decision":"approve","note":"ok ✓"}`}}}
	ctx := WithToolCallID(WithInputRelay(f.ctx(true), relay), "call-42")

	res := tool.Execute(ctx, map[string]any{"action": "input", "task_id": "run-1"})
	if res.IsError || res.ForLLM != textAnswerSent {
		t.Fatalf("input = %q", res.ForLLM)
	}
	if len(relay.asked) != 1 || relay.asked[0].toolCallID != "call-42" || relay.asked[0].message != "Approve the merge of PR 7?" {
		t.Fatalf("asked = %+v", relay.asked)
	}
	if strings.Contains(res.ForLLM, "Approve the merge") {
		t.Error("the question text reached the model")
	}
	updates := f.srv.TaskUpdates("run-1")
	if len(updates) != 1 {
		t.Fatalf("tasks/update count = %d", len(updates))
	}
	var resp struct {
		Action  string          `json:"action"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(updates[0]["approval-gate"], &resp); err != nil || resp.Action != "accept" ||
		string(resp.Content) != `{"decision":"approve","note":"ok ✓"}` {
		t.Fatalf("inputResponses = %s (%v)", updates[0]["approval-gate"], err)
	}
	if snap := f.tasks.snapshot(row.ID); len(snap.AnsweredKeys) != 1 || snap.AnsweredKeys[0] != "approval-gate" {
		t.Errorf("answered keys = %v", snap.AnsweredKeys)
	}
}

func TestMCPTaskInputDeclineKeepsRunPaused(t *testing.T) {
	for _, action := range []string{"decline", "cancel"} {
		t.Run(action, func(t *testing.T) {
			f, tool, row := newTaskToolFixture(t)
			f.srv.RequireTaskInput("run-1", "approval-gate", approvalForm)
			relay := &fakeRelay{canAsk: true, answers: []relayAnswer{{action, ""}}}
			res := tool.Execute(WithInputRelay(f.ctx(true), relay), map[string]any{"action": "input", "task_id": "run-1"})
			if res.IsError || res.ForLLM != textNotAnswered {
				t.Fatalf("input = %q", res.ForLLM)
			}
			if n := countFor(f.srv, methodTasksUpdate) + countFor(f.srv, methodTasksCancel); n != 0 {
				t.Fatalf("server got %d tasks/update or tasks/cancel", n)
			}
			if snap := f.tasks.snapshot(row.ID); len(snap.AnsweredKeys) != 0 {
				t.Errorf("answered keys = %v", snap.AnsweredKeys)
			}
		})
	}
}

func TestMCPTaskInputAsksAgainOnSchemaFailure(t *testing.T) {
	f, tool, _ := newTaskToolFixture(t)
	f.srv.RequireTaskInput("run-1", "approval-gate", approvalForm)
	relay := &fakeRelay{canAsk: true, answers: []relayAnswer{{"accept", `{"decision":"maybe"}`}, {"accept", `{"decision":"reject"}`}}}
	res := tool.Execute(WithInputRelay(f.ctx(true), relay), map[string]any{"action": "input", "task_id": "run-1"})
	if res.IsError || res.ForLLM != textAnswerSent {
		t.Fatalf("input = %q", res.ForLLM)
	}
	if len(relay.asked) != 2 || !strings.Contains(relay.asked[1].message, "not valid") {
		t.Fatalf("asked = %+v", relay.asked)
	}

	f.srv.RequireTaskInput("run-1", "gate-2", approvalForm)
	relay = &fakeRelay{canAsk: true, answers: []relayAnswer{{"accept", `{"x":1}`}, {"accept", `{}`}}}
	res = tool.Execute(WithInputRelay(f.ctx(true), relay), map[string]any{"action": "input", "task_id": "run-1"})
	if !res.IsError {
		t.Fatalf("two bad answers must give a tool error, got %q", res.ForLLM)
	}
	if n := len(f.srv.TaskUpdates("run-1")); n != 1 {
		t.Errorf("tasks/update count = %d, want only the first answer", n)
	}
}

func TestMCPTaskInputUpdateFailureRecordsNoAnswer(t *testing.T) {
	f, tool, row := newTaskToolFixture(t)
	f.srv.RequireTaskInput("run-1", "approval-gate", approvalForm)
	var keysAtUpdate atomic.Pointer[[]string]
	f.srv.Handle(methodTasksUpdate, func(context.Context, mcptest.Request) mcptest.Reply {
		keys := f.tasks.snapshot(row.ID).AnsweredKeys
		keysAtUpdate.Store(&keys)
		return mcptest.ErrorReply(-32603, "busy", nil)
	})
	relay := &fakeRelay{canAsk: true, answers: []relayAnswer{{"accept", `{"decision":"approve"}`}}}
	res := tool.Execute(WithInputRelay(f.ctx(true), relay), map[string]any{"action": "input", "task_id": "run-1"})
	if !res.IsError {
		t.Fatalf("input = %q, want an error", res.ForLLM)
	}
	if snap := f.tasks.snapshot(row.ID); len(snap.AnsweredKeys) != 0 {
		t.Errorf("answered keys after a failed update = %v", snap.AnsweredKeys)
	}
	// The answer is recorded only after the server took it.
	if keys := keysAtUpdate.Load(); keys == nil || len(*keys) != 0 {
		t.Errorf("answered keys during tasks/update = %v, want none", keys)
	}
}

func TestMCPTaskInputRecordsAnswerAfterUpdate(t *testing.T) {
	f, tool, row := newTaskToolFixture(t)
	f.srv.RequireTaskInput("run-1", "approval-gate", approvalForm)
	var keysAtUpdate atomic.Pointer[[]string]
	f.srv.OnTaskUpdate(func(string, map[string]json.RawMessage) {
		keys := f.tasks.snapshot(row.ID).AnsweredKeys
		keysAtUpdate.Store(&keys)
	})
	relay := &fakeRelay{canAsk: true, answers: []relayAnswer{{"accept", `{"decision":"approve"}`}}}
	res := tool.Execute(WithInputRelay(f.ctx(true), relay), map[string]any{"action": "input", "task_id": "run-1"})
	if res.IsError || res.ForLLM != textAnswerSent {
		t.Fatalf("input = %q", res.ForLLM)
	}
	if keys := keysAtUpdate.Load(); keys == nil || len(*keys) != 0 {
		t.Errorf("answered keys during tasks/update = %v, want none", keys)
	}
	if snap := f.tasks.snapshot(row.ID); !snap.IsAnswered("approval-gate") {
		t.Errorf("answered keys after the update = %v", snap.AnsweredKeys)
	}
}

// barrierRelay answers accept after n callers asked, so their answers come
// at the same time.
type barrierRelay struct {
	mu      sync.Mutex
	waiting int
	n       int
	release chan struct{}
}

func (r *barrierRelay) CanAskForm() bool { return true }

func (r *barrierRelay) AskForm(ctx context.Context, _, _ string, _ json.RawMessage) (string, json.RawMessage, error) {
	r.mu.Lock()
	r.waiting++
	if r.waiting == r.n {
		close(r.release)
	}
	r.mu.Unlock()
	select {
	case <-r.release:
	case <-ctx.Done():
		return "", nil, ctx.Err()
	}
	return "accept", json.RawMessage(`{"decision":"approve"}`), nil
}

func TestMCPTaskInputConcurrentAnswersSendOnce(t *testing.T) {
	f, tool, row := newTaskToolFixture(t)
	f.srv.RequireTaskInput("run-1", "approval-gate", approvalForm)
	relay := &barrierRelay{n: 2, release: make(chan struct{})}
	results := make(chan string, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			res := tool.Execute(WithInputRelay(f.ctx(true), relay), map[string]any{"action": "input", "task_id": "run-1"})
			if res.IsError {
				t.Errorf("input error: %s", res.ForLLM)
			}
			results <- res.ForLLM
		})
	}
	wg.Wait()
	close(results)
	if n := len(f.srv.TaskUpdates("run-1")); n != 1 {
		t.Fatalf("tasks/update count = %d, want 1", n)
	}
	sent := 0
	for r := range results {
		if r == textAnswerSent {
			sent++
		}
	}
	if sent != 1 {
		t.Errorf("answers sent = %d, want 1", sent)
	}
	if snap := f.tasks.snapshot(row.ID); len(snap.AnsweredKeys) != 1 {
		t.Errorf("answered keys = %v", snap.AnsweredKeys)
	}
}

func TestMCPTaskToolRejectsOtherUser(t *testing.T) {
	f, tool, row := newTaskToolFixture(t)
	row.UserID = "user-1"
	f.tasks.put(row)
	other := store.WithUserID(f.ctx(true), "user-2")
	noUser := store.WithAgentID(store.WithTenantID(context.Background(), f.tenantID), f.agentID)
	for name, ctx := range map[string]context.Context{"other user": other, "no user": noUser} {
		for _, action := range []string{"status", "input", "cancel"} {
			res := tool.Execute(ctx, map[string]any{"action": action, "task_id": "run-1"})
			if !res.IsError || !strings.Contains(res.ForLLM, "not found") {
				t.Errorf("%s, %s: %q", name, action, res.ForLLM)
			}
		}
	}
	if n := len(f.srv.Requests()); n != 0 {
		t.Errorf("server got %d requests for the task of another user", n)
	}
	if res := tool.Execute(f.ctx(true), map[string]any{"action": "status", "task_id": "run-1"}); res.IsError {
		t.Fatalf("owner status = %q", res.ForLLM)
	}
}

func TestMCPTaskToolAcceptsRowID(t *testing.T) {
	f, tool, row := newTaskToolFixture(t)
	res := tool.Execute(f.ctx(true), map[string]any{"action": "status", "task_id": row.ID.String()})
	if res.IsError || !strings.Contains(res.ForLLM, "status working") {
		t.Fatalf("status by row id = %q", res.ForLLM)
	}
	if reqs := f.srv.RequestsFor(methodTasksGet); len(reqs) != 1 || reqs[0].Header.Get("Mcp-Name") != "run-1" {
		t.Fatalf("tasks/get requests = %+v, want the server task id", reqs)
	}
	otherAgent := store.WithAgentID(f.ctx(true), uuid.New())
	if res := tool.Execute(otherAgent, map[string]any{"action": "status", "task_id": row.ID.String()}); !res.IsError || !strings.Contains(res.ForLLM, "not found") {
		t.Fatalf("row id of another agent = %q", res.ForLLM)
	}
}

func TestMCPTaskInputWithoutRelay(t *testing.T) {
	f, tool, _ := newTaskToolFixture(t)
	f.srv.RequireTaskInput("run-1", "approval-gate", approvalForm)
	for name, ctx := range map[string]context.Context{
		"no relay":     f.ctx(true),
		"no form mode": WithInputRelay(f.ctx(true), &fakeRelay{canAsk: false}),
	} {
		res := tool.Execute(ctx, map[string]any{"action": "input", "task_id": "run-1"})
		if res.IsError || res.ForLLM != textCannotAnswerChannel {
			t.Errorf("%s: %q", name, res.ForLLM)
		}
	}
}

func TestMCPTaskInputStatusMessageQuestion(t *testing.T) {
	f, tool, _ := newTaskToolFixture(t)
	f.srv.UpdateTask("run-1", func(tk *mcptest.Task) {
		tk.Status = "input_required"
		tk.StatusMessage = "Approve? Reply approve or reject."
	})
	relay := &fakeRelay{canAsk: true}
	res := tool.Execute(WithInputRelay(f.ctx(true), relay), map[string]any{"action": "input", "task_id": "run-1"})
	if res.IsError || !strings.Contains(res.ForLLM, "<<<EXTERNAL_UNTRUSTED_CONTENT>>>") ||
		!strings.Contains(res.ForLLM, "Approve? Reply approve or reject.") || !strings.Contains(res.ForLLM, textCannotAnswerHere) {
		t.Fatalf("input = %q", res.ForLLM)
	}
	if len(relay.asked) != 0 {
		t.Error("nothing must be asked without input requests")
	}
}

func TestMCPTaskInputURLModeCannotAnswer(t *testing.T) {
	f, tool, _ := newTaskToolFixture(t)
	f.srv.RequireTaskInput("run-1", "login", mcptest.InputRequest{Method: "elicitation/create", Params: map[string]any{
		"mode": "url", "message": "Log in", "url": "https://example.test/login",
	}})
	relay := &fakeRelay{canAsk: true}
	res := tool.Execute(WithInputRelay(f.ctx(true), relay), map[string]any{"action": "input", "task_id": "run-1"})
	if res.IsError || res.ForLLM != textCannotAnswerHere || len(relay.asked) != 0 {
		t.Fatalf("input = %q, asked %d", res.ForLLM, len(relay.asked))
	}
}

func TestMCPTaskToolValidatesArguments(t *testing.T) {
	f, tool, _ := newTaskToolFixture(t)
	for _, args := range []map[string]any{{"action": "delete", "task_id": "run-1"}, {"action": "status"}, {"action": "status", "task_id": " "}} {
		if res := tool.Execute(f.ctx(true), args); !res.IsError {
			t.Errorf("args %v: want an error, got %q", args, res.ForLLM)
		}
	}
}

func TestFormSchemaRejectsExtraKeysAndBadEnum(t *testing.T) {
	raw, _ := json.Marshal(approvalForm.Params.(map[string]any)["requestedSchema"])
	fs, err := parseFormSchema(raw)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]bool{
		`{"decision":"approve"}`:             true,
		`{"decision":"reject","note":"n"}`:   true,
		`{"decision":"maybe"}`:               false,
		`{"decision":"approve","extra":1}`:   false,
		`{"note":"n"}`:                       false,
		`{"decision":1}`:                     false,
		`[]`:                                 false,
		`"approve"`:                          false,
		`{"decision":"approve","note":true}`: false,
	}
	for content, ok := range cases {
		if err := fs.check(json.RawMessage(content)); (err == nil) != ok {
			t.Errorf("check(%s) err = %v, want ok=%v", content, err, ok)
		}
	}
	if err := fs.check(json.RawMessage(`{"note":"` + strings.Repeat("a", maxFormContentBytes) + `","decision":"approve"}`)); err == nil {
		t.Error("an answer over 64 KiB was accepted")
	}

	typed, err := parseFormSchema(json.RawMessage(`{"type":"object","properties":{"n":{"type":"integer"},"b":{"type":"boolean"},"c":{"type":"string","oneOf":[{"const":"x","title":"X"}]}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := typed.check(json.RawMessage(`{"n":2,"b":false,"c":"x"}`)); err != nil {
		t.Errorf("valid typed answer: %v", err)
	}
	if err := typed.check(json.RawMessage(`{"n":2.5}`)); err == nil {
		t.Error("a fraction passed as integer")
	}
	if err := typed.check(json.RawMessage(`{"c":"y"}`)); err == nil {
		t.Error("a value outside oneOf passed")
	}
	for _, bad := range []string{`{"type":"array"}`, `{"type":"object","properties":{"o":{"type":"object"}}}`, `{"type":"object","required":["z"]}`} {
		if _, err := parseFormSchema(json.RawMessage(bad)); err == nil {
			t.Errorf("schema %s must be refused", bad)
		}
	}
}

func TestCleanQuestion(t *testing.T) {
	if got := cleanQuestion("a\x1b[31mb\x00c\nd\te"); got != "a[31mbc\nd\te" {
		t.Errorf("cleanQuestion = %q", got)
	}
	long := strings.Repeat("é", maxQuestionBytes)
	got := cleanQuestion(long)
	if len(got) > maxQuestionBytes || !strings.HasPrefix(long, got) {
		t.Errorf("cut is %d bytes or splits a rune", len(got))
	}
}
