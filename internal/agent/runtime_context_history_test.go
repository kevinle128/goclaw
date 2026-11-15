package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// historyStore keeps session messages in memory so several runs can share one
// session, the way consecutive turns of a chat do.
type historyStore struct {
	nopSessionStore
	mu   sync.Mutex
	msgs []providers.Message
}

func (s *historyStore) AddMessage(_ context.Context, _ string, m providers.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, m)
}

func (s *historyStore) GetHistory(_ context.Context, _ string) []providers.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]providers.Message(nil), s.msgs...)
}

func (s *historyStore) SetHistory(_ context.Context, _ string, msgs []providers.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append([]providers.Message(nil), msgs...)
}

// recordingProvider records every request and calls the alpha tool once on
// the first request of each run when callTool is set.
type recordingProvider struct {
	callTool bool
	requests [][]providers.Message
	n        int
}

func (p *recordingProvider) record(req providers.ChatRequest) *providers.ChatResponse {
	p.requests = append(p.requests, append([]providers.Message(nil), req.Messages...))
	p.n++
	if p.callTool && p.n%2 == 1 {
		return &providers.ChatResponse{
			FinishReason: "tool_calls",
			ToolCalls:    []providers.ToolCall{{ID: "tc-" + strings.Repeat("x", p.n), Name: "alpha", Arguments: map[string]any{}}},
		}
	}
	return &providers.ChatResponse{Content: "answer", FinishReason: "stop"}
}

func (p *recordingProvider) Chat(_ context.Context, req providers.ChatRequest) (*providers.ChatResponse, error) {
	return p.record(req), nil
}

func (p *recordingProvider) ChatStream(_ context.Context, req providers.ChatRequest, _ func(providers.StreamChunk)) (*providers.ChatResponse, error) {
	return p.record(req), nil
}
func (p *recordingProvider) DefaultModel() string { return "stub-model" }
func (p *recordingProvider) Name() string         { return "stub" }

func newHistoryLoop(stable, callTool bool) (*Loop, *recordingProvider, *historyStore) {
	l := newPromptLayoutLoop("alpha")
	prov := &recordingProvider{callTool: callTool}
	st := &historyStore{}
	l.provider = prov
	l.sessions = st
	l.stablePromptPrefix = stable
	l.maxIterations = 5
	// A large window keeps background summarization from calling the provider.
	l.contextWindow = 1_000_000
	return l, prov, st
}

func runTurn(t *testing.T, l *Loop, text, extra string) {
	t.Helper()
	_, err := l.runViaPipeline(context.Background(), RunRequest{
		SessionKey:        "sess-1",
		Message:           text,
		Channel:           "ws",
		ChatID:            "chat-1",
		PeerKind:          "direct",
		UserID:            "user-1",
		RunID:             "run-" + text,
		ExtraSystemPrompt: extra,
	})
	if err != nil {
		t.Fatalf("run %q: %v", text, err)
	}
}

// messageKey is the part of a message the provider sees on the wire.
func messageKey(m providers.Message) string {
	calls, _ := json.Marshal(m.ToolCalls)
	return m.Role + "\x00" + m.Content + "\x00" + string(calls) + "\x00" + m.ToolCallID
}

func countRuntimeBlocks(msgs []providers.Message) int {
	n := 0
	for _, m := range msgs {
		if m.Role == "user" && strings.HasPrefix(m.Content, "<runtime_context>\n") {
			n++
		}
	}
	return n
}

// The provider reuses its cache only when a request contains the whole previous
// request as its prefix, so the user message of turn N must be stored exactly
// as it was sent.
func TestRuntimeContext_NextRequestStartsWithPreviousRequest(t *testing.T) {
	l, prov, _ := newHistoryLoop(true, false)

	runTurn(t, l, "first question", "same extra")
	runTurn(t, l, "second question", "same extra")

	if len(prov.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(prov.requests))
	}
	first, second := prov.requests[0], prov.requests[1]
	if len(second) <= len(first) {
		t.Fatalf("second request (%d) must be longer than first (%d)", len(second), len(first))
	}
	for i := range first {
		if messageKey(first[i]) != messageKey(second[i]) {
			t.Fatalf("message %d differs between turns\nfirst:  %q\nsecond: %q", i, first[i].Content, second[i].Content)
		}
	}
}

// An unchanged block is not repeated, which keeps history small.
func TestRuntimeContext_SentOncePerSessionWhenUnchanged(t *testing.T) {
	l, prov, st := newHistoryLoop(true, false)

	runTurn(t, l, "one", "same extra")
	runTurn(t, l, "two", "same extra")
	runTurn(t, l, "three", "same extra")

	last := prov.requests[2]
	if got := countRuntimeBlocks(last); got != 1 {
		t.Fatalf("runtime blocks in third request = %d, want 1", got)
	}
	if got := countRuntimeBlocks(st.GetHistory(context.Background(), "")); got != 1 {
		t.Fatalf("runtime blocks stored in session = %d, want 1", got)
	}
	if tail := last[len(last)-1]; tail.Content != "three" {
		t.Fatalf("current message = %q, want the raw text", tail.Content)
	}
}

// A block whose content changed is sent again and stored again.
func TestRuntimeContext_ResentWhenContentChanges(t *testing.T) {
	l, prov, st := newHistoryLoop(true, false)

	runTurn(t, l, "one", "extra A")
	runTurn(t, l, "two", "extra A")
	runTurn(t, l, "three", "extra B")
	runTurn(t, l, "four", "extra B")

	third := prov.requests[2]
	tail := third[len(third)-1]
	if !strings.HasPrefix(tail.Content, "<runtime_context>\n") || !strings.Contains(tail.Content, "extra B") {
		t.Fatalf("changed block must be sent again: %q", tail.Content)
	}
	fourth := prov.requests[3]
	if tail := fourth[len(fourth)-1]; tail.Content != "four" {
		t.Fatalf("unchanged block must not repeat: %q", tail.Content)
	}
	if got := countRuntimeBlocks(st.GetHistory(context.Background(), "")); got != 2 {
		t.Fatalf("runtime blocks stored = %d, want 2", got)
	}
}

// When the history no longer holds a block (compaction), the next turn sends it.
func TestRuntimeContext_ResentAfterHistoryDropsBlock(t *testing.T) {
	l, prov, st := newHistoryLoop(true, false)

	runTurn(t, l, "one", "same extra")
	st.SetHistory(context.Background(), "", []providers.Message{
		{Role: "user", Content: "kept without block"},
		{Role: "assistant", Content: "answer"},
	})
	runTurn(t, l, "two", "same extra")

	second := prov.requests[1]
	if tail := second[len(second)-1]; !strings.HasPrefix(tail.Content, "<runtime_context>\n") {
		t.Fatalf("block must return after history lost it: %q", tail.Content)
	}
}

// Tool calls and results keep their order and pairing inside the turn, and the
// next request still starts with the previous one.
func TestRuntimeContext_ToolPairingUnchanged(t *testing.T) {
	l, prov, st := newHistoryLoop(true, true)

	runTurn(t, l, "use the tool", "same extra")
	runTurn(t, l, "again", "same extra")

	stored := st.GetHistory(context.Background(), "")
	// user, assistant(tool_calls), tool, assistant, user, assistant(tool_calls), tool, assistant
	roles := make([]string, len(stored))
	for i, m := range stored {
		roles[i] = m.Role
	}
	want := "user,assistant,tool,assistant,user,assistant,tool,assistant"
	if got := strings.Join(roles, ","); got != want {
		t.Fatalf("stored roles = %s, want %s", got, want)
	}
	if stored[1].ToolCalls[0].ID != stored[2].ToolCallID {
		t.Fatalf("tool result %q does not answer tool call %q", stored[2].ToolCallID, stored[1].ToolCalls[0].ID)
	}
	// Request 3 (turn 2, first call) must start with all of request 2 (turn 1, last call).
	prev, next := prov.requests[1], prov.requests[2]
	for i := range prev {
		if messageKey(prev[i]) != messageKey(next[i]) {
			t.Fatalf("message %d differs: %q vs %q", i, prev[i].Content, next[i].Content)
		}
	}
}

// With the flag off nothing changes: the block never exists and the raw text
// is stored.
func TestRuntimeContext_FlagOffStoresRawText(t *testing.T) {
	l, prov, st := newHistoryLoop(false, false)

	runTurn(t, l, "one", "same extra")
	runTurn(t, l, "two", "same extra")

	for _, req := range prov.requests {
		if countRuntimeBlocks(req) != 0 {
			t.Fatal("flag off must not send a runtime block")
		}
	}
	stored := st.GetHistory(context.Background(), "")
	if stored[0].Content != "one" {
		t.Fatalf("stored user message = %q, want raw text", stored[0].Content)
	}
}
