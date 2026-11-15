package cmd

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
	"github.com/nextlevelbuilder/goclaw/internal/agent"
	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/scheduler"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

type recordingWakeSender struct {
	mu      sync.Mutex
	outcome string
	noSink  bool
	params  []json.RawMessage
	users   []string
}

func (s *recordingWakeSender) DeliverWake(_ context.Context, _, _ uuid.UUID, userID, _ string, params json.RawMessage) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.params = append(s.params, params)
	s.users = append(s.users, userID)
	return s.outcome, !s.noSink
}

func (s *recordingWakeSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.params)
}

const acpTestSession = "agent:fox:ws:direct:acp-1"

// runAnnounceLoop drives processSubagentAnnounceLoop with a real scheduler.
// It returns the response text outbound messages and whether the run saw the
// ACP session marker.
func runAnnounceLoop(t *testing.T, waker *acpbridge.ResultWaker, rememberACP bool, runContent string) (outbound []bus.OutboundMessage, sawACPMarker bool) {
	t.Helper()
	msgBus := bus.New()
	var markerSeen bool
	var mu sync.Mutex
	sched := scheduler.NewScheduler(scheduler.DefaultLanes(),
		scheduler.QueueConfig{Mode: scheduler.QueueModeQueue, Cap: 1, Drop: scheduler.DropOld, MaxConcurrent: 1},
		func(ctx context.Context, req agent.RunRequest) (*agent.RunResult, error) {
			mu.Lock()
			markerSeen = tools.IsACPSessionFromCtx(ctx)
			mu.Unlock()
			return &agent.RunResult{Content: runContent}, nil
		})
	defer sched.Stop()

	if rememberACP {
		waker.Origins.Remember(acpTestSession, acpbridge.SessionOrigin{
			TenantID: uuid.New(), AgentID: uuid.New(), UserID: "user-1", ACPSessionID: "acp-1", Wake: true,
			Thread: json.RawMessage(`{"channelId":"7d6c0f3e-2f0a-4d4e-9f1c-0a5b3a1c2d3e","rootEventId":"` + strings.Repeat("a", 64) + `"}`),
		})
	}
	key := "t:" + acpTestSession
	subagentAnnounceQueue.Enqueue(key, subagentAnnounceEntry{Label: "research", Status: "completed", Content: "raw result"})
	r := subagentAnnounceRouting{QueueKey: key, SessionKey: acpTestSession, OrigChannel: "ws", OrigChatID: "acp-1", ParentAgent: "fox"}
	mgr := tools.NewSubagentManager(nil, nil, "", msgBus, nil, tools.SubagentConfig{})

	processSubagentAnnounceLoop(context.Background(), r, tools.SubagentRoster{}, mgr, sched, msgBus, &config.Config{}, waker)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	for {
		msg, ok := msgBus.SubscribeOutbound(ctx)
		if !ok {
			break
		}
		outbound = append(outbound, msg)
	}
	mu.Lock()
	defer mu.Unlock()
	return outbound, markerSeen
}

func TestSubagentAnnounceOnACPSessionWakesTheThread(t *testing.T) {
	sender := &recordingWakeSender{outcome: protocol.ACPWakeOutcomeAccepted}
	waker := acpbridge.NewResultWaker(acpbridge.NewSessionOrigins(), sender)

	outbound, marker := runAnnounceLoop(t, waker, true, "Sub-agent found 3 issues.")

	if sender.count() != 1 {
		t.Fatalf("wakes = %d, want 1", sender.count())
	}
	var params struct {
		WakeID string `json:"wakeId"`
		Reason string `json:"reason"`
		Prompt string `json:"prompt"`
		Thread struct {
			ChannelID   string `json:"channelId"`
			RootEventID string `json:"rootEventId"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(sender.params[0], &params); err != nil {
		t.Fatal(err)
	}
	if params.Reason != "completed" || params.Thread.ChannelID == "" || len(params.Thread.RootEventID) != 64 || params.WakeID == "" {
		t.Fatalf("params = %+v", params)
	}
	if !strings.Contains(params.Prompt, "Sub-agent found 3 issues.") || !strings.Contains(params.Prompt, "buzz messages send") {
		t.Fatalf("prompt = %q", params.Prompt)
	}
	if sender.users[0] != "user-1" {
		t.Fatalf("wake user = %q", sender.users[0])
	}
	if len(outbound) != 0 {
		t.Fatalf("a delivered wake must not also publish the text, got %d outbound", len(outbound))
	}
	if !marker {
		t.Fatal("announce run of an ACP session lacks the ACP session marker")
	}
}

func TestSubagentAnnounceKeepsOutboundWhenWakeFails(t *testing.T) {
	sender := &recordingWakeSender{noSink: true}
	waker := acpbridge.NewResultWaker(acpbridge.NewSessionOrigins(), sender)

	outbound, _ := runAnnounceLoop(t, waker, true, "summary")

	if len(outbound) != 1 || outbound[0].Content != "summary" {
		t.Fatalf("outbound = %+v, want the summary as fallback", outbound)
	}
}

func TestSubagentAnnounceOutsideACPSessionPublishesAsBefore(t *testing.T) {
	sender := &recordingWakeSender{outcome: protocol.ACPWakeOutcomeAccepted}
	waker := acpbridge.NewResultWaker(acpbridge.NewSessionOrigins(), sender)

	outbound, marker := runAnnounceLoop(t, waker, false, "summary")

	if sender.count() != 0 {
		t.Fatalf("wakes = %d for a session that is not ACP", sender.count())
	}
	if len(outbound) != 1 || outbound[0].Content != "summary" || marker {
		t.Fatalf("outbound = %+v marker=%v", outbound, marker)
	}
}

func TestSubagentWakePromptFitsAndKeepsUTF8(t *testing.T) {
	long := strings.Repeat("kết quả ", 600)
	prompt := buildSubagentWakePrompt([]subagentAnnounceEntry{{Label: "tìm lỗi", Status: "completed", Content: "x"}}, long)
	if len(prompt) > acpbridge.MaxResultWakePromptBytes || !utf8.ValidString(prompt) {
		t.Fatalf("prompt bytes=%d valid=%v", len(prompt), utf8.ValidString(prompt))
	}
	if !strings.HasSuffix(prompt, "[cut]") {
		t.Fatal("a cut summary must be marked")
	}
	raw := buildSubagentWakePrompt([]subagentAnnounceEntry{{Label: "a", Status: "failed", Content: "boom"}, {Label: "b", Status: "completed", Content: "fine"}}, "")
	if !strings.Contains(raw, "boom") || !strings.Contains(raw, "fine") {
		t.Fatalf("raw results missing: %q", raw)
	}
}

func TestSubagentWakeReason(t *testing.T) {
	cases := map[string][]subagentAnnounceEntry{
		"failed":    {{Status: "failed"}, {Status: "failed"}},
		"cancelled": {{Status: "cancelled"}},
		"completed": {{Status: "completed"}, {Status: "failed"}},
	}
	for want, entries := range cases {
		if got := subagentWakeReason(entries); got != want {
			t.Errorf("reason(%v) = %q, want %q", entries, got, want)
		}
	}
}
