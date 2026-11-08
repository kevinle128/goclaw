package acpbridge

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

type deliverResult struct {
	outcome string
	ok      bool
}

func startDeliver(sinks *WakeSinks, tenant, agent uuid.UUID, user, wakeID string) <-chan deliverResult {
	out := make(chan deliverResult, 1)
	go func() {
		outcome, ok := sinks.DeliverWake(context.Background(), tenant, agent, user, wakeID, json.RawMessage(`{"wakeId":"`+wakeID+`"}`))
		out <- deliverResult{outcome, ok}
	}()
	return out
}

func waitDeliver(t *testing.T, results <-chan deliverResult) deliverResult {
	t.Helper()
	select {
	case r := <-results:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("DeliverWake did not return")
		return deliverResult{}
	}
}

func TestWakeSinksNoSinkForOtherTenantAgentOrUser(t *testing.T) {
	sinks := NewWakeSinks(time.Second)
	tenant, agent := uuid.New(), uuid.New()
	if err := sinks.Subscribe(WakeSink{ConnectionID: "c1", TenantID: tenant, AgentID: agent, UserID: "u1", Sender: newChanSender()}); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]struct {
		tenant, agent uuid.UUID
		user          string
	}{
		"other tenant": {uuid.New(), agent, "u1"},
		"other agent":  {tenant, uuid.New(), "u1"},
		"other user":   {tenant, agent, "u2"},
	} {
		if _, ok := sinks.DeliverWake(context.Background(), target.tenant, target.agent, target.user, "w_1", json.RawMessage(`{}`)); ok {
			t.Fatalf("%s: a sink was found", name)
		}
	}
}

func TestWakeSinksDeliverToLastSubscribedAndAck(t *testing.T) {
	sinks := NewWakeSinks(time.Second)
	tenant, agent := uuid.New(), uuid.New()
	first, second := newChanSender(), newChanSender()
	kicks := 0
	sinks.SetOnSubscribe(func() { kicks++ })
	if err := sinks.Subscribe(WakeSink{ConnectionID: "c1", TenantID: tenant, AgentID: agent, UserID: "u1", Sender: first}); err != nil {
		t.Fatal(err)
	}
	if err := sinks.Subscribe(WakeSink{ConnectionID: "c2", TenantID: tenant, AgentID: agent, UserID: "u1", Sender: second}); err != nil {
		t.Fatal(err)
	}
	if kicks != 2 {
		t.Fatalf("subscribe hook ran %d times", kicks)
	}
	results := startDeliver(sinks, tenant, agent, "u1", "w_a")
	payload := second.next(t)
	if jsonString(t, payload["_event"]) != protocol.EventACPWake || jsonString(t, payload["wakeId"]) != "w_a" || string(payload["params"]) != `{"wakeId":"w_a"}` {
		t.Fatalf("wake event = %v", payload)
	}
	if err := sinks.Ack("c1", "w_a", protocol.ACPWakeOutcomeAccepted); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ack from another connection error = %v", err)
	}
	if err := sinks.Ack("c2", "w_a", "maybe"); !errors.Is(err, ErrWakeInvalid) {
		t.Fatalf("bad outcome error = %v", err)
	}
	if err := sinks.Ack("c2", "w_a", protocol.ACPWakeOutcomeAccepted); err != nil {
		t.Fatal(err)
	}
	if got := waitDeliver(t, results); !got.ok || got.outcome != protocol.ACPWakeOutcomeAccepted {
		t.Fatalf("DeliverWake() = %+v", got)
	}
	if len(first.events) != 0 {
		t.Fatal("the older sink also got the wake")
	}

	// A renewed subscription of c1 makes it the last subscribed sink.
	if err := sinks.Subscribe(WakeSink{ConnectionID: "c1", TenantID: tenant, AgentID: agent, UserID: "u1", Sender: first}); err != nil {
		t.Fatal(err)
	}
	results = startDeliver(sinks, tenant, agent, "u1", "w_b")
	first.next(t)
	if err := sinks.Ack("c1", "w_b", protocol.ACPWakeOutcomeBusy); err != nil {
		t.Fatal(err)
	}
	if got := waitDeliver(t, results); got.outcome != protocol.ACPWakeOutcomeBusy {
		t.Fatalf("DeliverWake() = %+v", got)
	}
	if len(second.events) != 0 {
		t.Fatal("busy must not try the next sink")
	}
}

func TestWakeSinksTryNextOnUnsupportedAndRemoveSink(t *testing.T) {
	sinks := NewWakeSinks(time.Second)
	tenant, agent := uuid.New(), uuid.New()
	older, newer := newChanSender(), newChanSender()
	_ = sinks.Subscribe(WakeSink{ConnectionID: "old", TenantID: tenant, AgentID: agent, UserID: "u1", Sender: older})
	_ = sinks.Subscribe(WakeSink{ConnectionID: "new", TenantID: tenant, AgentID: agent, UserID: "u1", Sender: newer})

	results := startDeliver(sinks, tenant, agent, "u1", "w_1")
	newer.next(t)
	if err := sinks.Ack("new", "w_1", protocol.ACPWakeOutcomeUnsupported); err != nil {
		t.Fatal(err)
	}
	older.next(t)
	if err := sinks.Ack("old", "w_1", protocol.ACPWakeOutcomeDuplicate); err != nil {
		t.Fatal(err)
	}
	if got := waitDeliver(t, results); got.outcome != protocol.ACPWakeOutcomeDuplicate {
		t.Fatalf("DeliverWake() = %+v", got)
	}
	if sinks.Len() != 1 {
		t.Fatalf("sinks = %d, want the unsupported sink removed", sinks.Len())
	}
}

func TestWakeSinksFailedOnTimeoutTriesNextOnce(t *testing.T) {
	sinks := NewWakeSinks(50 * time.Millisecond)
	tenant, agent := uuid.New(), uuid.New()
	senders := []*chanSender{newChanSender(), newChanSender(), newChanSender()}
	for i, s := range senders {
		_ = sinks.Subscribe(WakeSink{ConnectionID: string(rune('a' + i)), TenantID: tenant, AgentID: agent, UserID: "u1", Sender: s})
	}
	got := waitDeliver(t, startDeliver(sinks, tenant, agent, "u1", "w_1"))
	if !got.ok || got.outcome != protocol.ACPWakeOutcomeFailed {
		t.Fatalf("DeliverWake() = %+v", got)
	}
	tried := 0
	for _, s := range senders {
		tried += len(s.events)
	}
	if tried != 2 {
		t.Fatalf("tried %d sinks, want 2", tried)
	}
	if sinks.Len() != 3 {
		t.Fatal("a failed sink must stay subscribed")
	}
}

func TestWakeSinksReleaseConnectionFailsWaitingDelivery(t *testing.T) {
	sinks := NewWakeSinks(5 * time.Second)
	tenant, agent := uuid.New(), uuid.New()
	sender := newChanSender()
	_ = sinks.Subscribe(WakeSink{ConnectionID: "c1", TenantID: tenant, AgentID: agent, UserID: "u1", Sender: sender})
	results := startDeliver(sinks, tenant, agent, "u1", "w_1")
	sender.next(t)
	sinks.ReleaseConnection("c1")
	if got := waitDeliver(t, results); got.outcome != protocol.ACPWakeOutcomeFailed {
		t.Fatalf("DeliverWake() = %+v", got)
	}
	if sinks.Len() != 0 {
		t.Fatal("sinks of the closed connection stay")
	}
}

func TestWakeSinksSubscribeValidatesAndLimits(t *testing.T) {
	sinks := NewWakeSinks(time.Second)
	tenant := uuid.New()
	if err := sinks.Subscribe(WakeSink{ConnectionID: "c1", TenantID: tenant, UserID: "u1", Sender: newChanSender()}); !errors.Is(err, ErrWakeInvalid) {
		t.Fatalf("missing agent error = %v", err)
	}
	for range MaxWakeSinksPerConnection {
		if err := sinks.Subscribe(WakeSink{ConnectionID: "c1", TenantID: tenant, AgentID: uuid.New(), UserID: "u1", Sender: newChanSender()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sinks.Subscribe(WakeSink{ConnectionID: "c1", TenantID: tenant, AgentID: uuid.New(), UserID: "u1", Sender: newChanSender()}); !errors.Is(err, ErrWakeSinkLimit) {
		t.Fatalf("limit error = %v", err)
	}
}
