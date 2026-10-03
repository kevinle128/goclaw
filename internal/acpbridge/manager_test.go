package acpbridge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

type recordingSender struct {
	events []protocol.EventFrame
	err    error
}

func (s *recordingSender) SendCriticalEvent(event protocol.EventFrame) error {
	if s.err != nil {
		return s.err
	}
	s.events = append(s.events, event)
	return nil
}

func testRegistration(sender CriticalEventSender) Registration {
	return Registration{
		Owner:   Owner{ConnectionID: "conn-1", TenantID: uuid.New(), UserID: "user-1"},
		AgentID: "agent-1", SessionKey: "agent:agent-1:ws:direct:session-1", Generation: 1,
		Sender: sender,
		Tools:  []ToolSchema{{Name: "mcp_buzz__messages_send", ServerName: "buzz", ToolName: "messages_send", InputSchema: map[string]any{"type": "object"}}},
	}
}

func TestCapabilityRegisterBindsConnectionIdentity(t *testing.T) {
	m := NewManager(DefaultLimits())
	reg := testRegistration(&recordingSender{})
	capability, err := m.Register(context.Background(), reg)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := m.Snapshot(capability.ID)
	if !ok {
		t.Fatal("capability missing")
	}
	if snapshot.Owner != reg.Owner || snapshot.AgentID != reg.AgentID || snapshot.SessionKey != reg.SessionKey || snapshot.Generation != reg.Generation {
		t.Fatalf("identity changed: %#v", snapshot)
	}
}

func TestRemoteToolResultRejectsWrongOwnerCapabilityOrGeneration(t *testing.T) {
	m := NewManager(DefaultLimits())
	reg := testRegistration(&recordingSender{})
	capability, err := m.Register(context.Background(), reg)
	if err != nil {
		t.Fatal(err)
	}
	call, err := m.BeginCall(context.Background(), capability.ID, reg.Owner.ConnectionID, reg.SessionKey, reg.Generation, reg.Tools[0].Name, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	wrong := reg.Owner
	wrong.ConnectionID = "conn-2"
	if err := m.CompleteCall(wrong, capability.ID, call.ID, reg.Generation, CallResult{Content: "spoof"}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("wrong owner error = %v", err)
	}
	if err := m.CompleteCall(reg.Owner, capability.ID, call.ID, reg.Generation+1, CallResult{Content: "stale"}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale generation error = %v", err)
	}
	if err := m.CompleteCall(reg.Owner, capability.ID, call.ID, reg.Generation, CallResult{Content: "ok"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-call.Result:
		if got.Content != "ok" {
			t.Fatalf("result = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("result was not delivered")
	}
}

func TestToolCallCriticalSendOverflowFailsImmediately(t *testing.T) {
	m := NewManager(DefaultLimits())
	sender := &recordingSender{err: errors.New("queue full")}
	reg := testRegistration(sender)
	capability, err := m.Register(context.Background(), reg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.BeginCall(context.Background(), capability.ID, reg.Owner.ConnectionID, reg.SessionKey, reg.Generation, reg.Tools[0].Name, nil); !errors.Is(err, sender.err) {
		t.Fatalf("begin call error = %v", err)
	}
	if got := m.PendingCalls(); got != 0 {
		t.Fatalf("pending calls = %d", got)
	}
}

func TestCapabilityLeaseSurvivesActiveRunAndPendingCall(t *testing.T) {
	limits := DefaultLimits()
	limits.OrphanTTL = time.Millisecond
	now := time.Unix(100, 0)
	m := NewManager(limits, WithClock(func() time.Time { return now }))
	reg := testRegistration(&recordingSender{})
	capability, err := m.Register(context.Background(), reg)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := m.Acquire(reg.Owner.ConnectionID, reg.SessionKey, reg.Generation)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	m.Reap(now)
	if _, ok := m.Snapshot(capability.ID); !ok {
		t.Fatal("leased capability was reaped")
	}
	lease.Release()
	m.Reap(now.Add(time.Minute))
	if _, ok := m.Snapshot(capability.ID); ok {
		t.Fatal("unleased capability was not reaped")
	}
}

func TestCapabilityRegistrationRateIsPrincipalScoped(t *testing.T) {
	limits := DefaultLimits()
	limits.RegistrationsPerConnection = 1
	limits.CapabilityPerConnection = 10
	manager := NewManager(limits)
	first := testRegistration(&recordingSender{})
	if _, err := manager.Register(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Register(context.Background(), first); !errors.Is(err, ErrQuota) {
		t.Fatalf("second registration error = %v", err)
	}
	second := testRegistration(&recordingSender{})
	second.Owner.ConnectionID = "conn-2"
	second.Owner.UserID = "user-2"
	if _, err := manager.Register(context.Background(), second); err != nil {
		t.Fatalf("another principal was unfairly limited: %v", err)
	}
}
