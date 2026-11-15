package methods

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/agent"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/gateway"
	"github.com/nextlevelbuilder/goclaw/internal/permissions"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

const steerSessionKey = "agent:a:ws:direct:s1"

type steerHarness struct {
	methods *ChatMethods
	router  *agent.Router
	client  *gateway.Client
	frames  <-chan []byte
}

func newSteerHarness(t *testing.T, role permissions.Role, sessionOwner string) *steerHarness {
	t.Helper()
	sess := newStubSessionStore()
	sess.addSession(steerSessionKey, sessionOwner)
	router := agent.NewRouter()
	client, frames := gateway.NewCapturingTestClient(role, uuid.New(), "user-1", 8)
	return &steerHarness{
		methods: &ChatMethods{agents: router, sessions: sess, cfg: &config.Config{}},
		router:  router,
		client:  client,
		frames:  frames,
	}
}

func (h *steerHarness) steer(t *testing.T, params any) capturedFrame {
	t.Helper()
	h.methods.handleSteer(context.Background(), h.client, request(t, protocol.MethodChatSteer, params))
	return readFrame(t, h.frames)
}

func steerPayload(t *testing.T, frame capturedFrame) (injected bool, runID, reason string) {
	t.Helper()
	var p struct {
		Injected bool   `json:"injected"`
		RunID    string `json:"runId"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal(frame.Payload, &p); err != nil {
		t.Fatalf("payload %s: %v", frame.Payload, err)
	}
	return p.Injected, p.RunID, p.Reason
}

func TestChatSteer_InjectsIntoActiveRunWithoutStartingAnother(t *testing.T) {
	h := newSteerHarness(t, permissions.RoleOperator, "user-1")
	injectCh := h.router.RegisterRun(context.Background(), "run-1", steerSessionKey, "agent-1", func() {})

	frame := h.steer(t, map[string]any{"sessionKey": steerSessionKey, "message": "also check B"})

	if !frame.OK {
		t.Fatalf("frame = %+v, want ok", frame)
	}
	injected, runID, _ := steerPayload(t, frame)
	if !injected || runID != "run-1" {
		t.Fatalf("injected=%v runId=%q, want true and run-1", injected, runID)
	}
	select {
	case msg := <-injectCh:
		if msg.Content != "also check B" || msg.UserID != "user-1" {
			t.Fatalf("injected message = %+v", msg)
		}
	default:
		t.Fatal("the active run did not receive the message")
	}
	if id, ok := h.router.SessionRunID(steerSessionKey); !ok || id != "run-1" {
		t.Fatalf("session run = %q, %v, want the original run only", id, ok)
	}
}

func TestChatSteer_IdleSessionStartsNoRun(t *testing.T) {
	h := newSteerHarness(t, permissions.RoleOperator, "user-1")

	frame := h.steer(t, map[string]any{"sessionKey": steerSessionKey, "message": "hello"})

	if !frame.OK {
		t.Fatalf("frame = %+v, want ok response", frame)
	}
	injected, _, reason := steerPayload(t, frame)
	if injected || reason != agent.InjectReasonNoActiveRun {
		t.Fatalf("injected=%v reason=%q, want false and no_active_run", injected, reason)
	}
	if h.router.IsSessionBusy(steerSessionKey) {
		t.Fatal("chat.steer started a run on an idle session")
	}
}

func TestChatSteer_FullInjectChannelReportsReason(t *testing.T) {
	h := newSteerHarness(t, permissions.RoleOperator, "user-1")
	h.router.RegisterRun(context.Background(), "run-1", steerSessionKey, "agent-1", func() {})
	var last capturedFrame
	for range 6 {
		last = h.steer(t, map[string]any{"sessionKey": steerSessionKey, "message": "m"})
	}
	injected, _, reason := steerPayload(t, last)
	if injected || reason != agent.InjectReasonFull {
		t.Fatalf("6th steer: injected=%v reason=%q, want false and inject_full", injected, reason)
	}
}

func TestChatSteer_AnotherUsersSessionIsRefused(t *testing.T) {
	h := newSteerHarness(t, permissions.RoleOperator, "someone-else")
	injectCh := h.router.RegisterRun(context.Background(), "run-1", steerSessionKey, "agent-1", func() {})

	frame := h.steer(t, map[string]any{"sessionKey": steerSessionKey, "message": "hijack"})

	if frame.OK || frame.Error == nil || frame.Error.Code != protocol.ErrUnauthorized {
		t.Fatalf("frame = %+v, want unauthorized", frame)
	}
	if len(injectCh) != 0 {
		t.Fatal("a refused steer reached the run")
	}
}

func TestChatSteer_RejectsMissingParams(t *testing.T) {
	h := newSteerHarness(t, permissions.RoleOperator, "user-1")
	for name, params := range map[string]any{
		"no session": map[string]any{"message": "x"},
		"no message": map[string]any{"sessionKey": steerSessionKey},
	} {
		frame := h.steer(t, params)
		if frame.OK || frame.Error == nil || frame.Error.Code != protocol.ErrInvalidRequest {
			t.Errorf("%s: frame = %+v, want invalid request", name, frame)
		}
	}
}

func TestChatSteer_IsWriteMethodForReadOnlyRole(t *testing.T) {
	if got := permissions.MethodRole(protocol.MethodChatSteer); got != permissions.RoleOperator {
		t.Fatalf("MethodRole(chat.steer) = %q, want operator", got)
	}
	if permissions.HasMinRole(permissions.RoleViewer, permissions.MethodRole(protocol.MethodChatSteer)) {
		t.Fatal("a viewer may call chat.steer")
	}
}
