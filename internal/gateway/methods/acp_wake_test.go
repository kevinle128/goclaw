package methods

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
	"github.com/nextlevelbuilder/goclaw/internal/gateway"
	"github.com/nextlevelbuilder/goclaw/internal/permissions"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

type capturedFrame struct {
	Type    string          `json:"type"`
	ID      string          `json:"id"`
	OK      bool            `json:"ok"`
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload"`
	Error   *struct {
		Code string `json:"code"`
	} `json:"error"`
}

func readFrame(t *testing.T, frames <-chan []byte) capturedFrame {
	t.Helper()
	select {
	case data := <-frames:
		var frame capturedFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			t.Fatal(err)
		}
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("no frame was sent")
		return capturedFrame{}
	}
}

func request(t *testing.T, method string, params any) *protocol.RequestFrame {
	t.Helper()
	data, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return &protocol.RequestFrame{Type: protocol.FrameTypeRequest, ID: "req-1", Method: method, Params: data}
}

// tenantAgents resolves agents of one tenant only, like the agent router.
func tenantAgents(tenantID uuid.UUID, agents map[string]uuid.UUID) ACPAgentResolver {
	return func(ctx context.Context, agentID string) (uuid.UUID, error) {
		if store.TenantIDFromContext(ctx) != tenantID {
			return uuid.Nil, errors.New("agent not found")
		}
		id, ok := agents[agentID]
		if !ok {
			return uuid.Nil, errors.New("agent not found")
		}
		return id, nil
	}
}

func TestWakeSubscribeRejectsAgentOutsideKeyScope(t *testing.T) {
	tenantA, tenantB := uuid.New(), uuid.New()
	agentA := uuid.New()
	sinks := acpbridge.NewWakeSinks(time.Second)
	methods := NewACPWakeMethods(sinks, tenantAgents(tenantA, map[string]uuid.UUID{"resolver": agentA}))

	// A key of tenant B names the agent of tenant A.
	client, frames := gateway.NewCapturingTestClient(permissions.RoleOperator, tenantB, "user-b", 4)
	ctx := store.WithTenantID(context.Background(), tenantB)
	methods.handleSubscribe(ctx, client, request(t, protocol.MethodACPWakeSubscribe, map[string]string{"agentId": "resolver"}))
	if frame := readFrame(t, frames); frame.OK || frame.Error == nil || frame.Error.Code != protocol.ErrUnauthorized {
		t.Fatalf("response = %+v", frame)
	}
	if sinks.Len() != 0 {
		t.Fatal("a sink was added for an agent outside the key scope")
	}

	// An unknown agent in the own tenant is also refused.
	clientA, framesA := gateway.NewCapturingTestClient(permissions.RoleOperator, tenantA, "user-a", 4)
	ctxA := store.WithTenantID(context.Background(), tenantA)
	methods.handleSubscribe(ctxA, clientA, request(t, protocol.MethodACPWakeSubscribe, map[string]string{"agentId": "other"}))
	if frame := readFrame(t, framesA); frame.OK {
		t.Fatalf("response = %+v", frame)
	}

	// The own agent is accepted and bound to the connection identity.
	methods.handleSubscribe(ctxA, clientA, request(t, protocol.MethodACPWakeSubscribe, map[string]string{"agentId": "resolver"}))
	if frame := readFrame(t, framesA); !frame.OK {
		t.Fatalf("response = %+v", frame)
	}
	if sinks.Len() != 1 {
		t.Fatalf("sinks = %d", sinks.Len())
	}
	if _, ok := sinks.DeliverWake(context.Background(), tenantA, agentA, "user-other", "w_1", json.RawMessage(`{}`)); ok {
		t.Fatal("the sink accepted a wake of another user")
	}
}

func TestWakeSubscribeAndAckDeliverOutcome(t *testing.T) {
	tenant, agentID := uuid.New(), uuid.New()
	sinks := acpbridge.NewWakeSinks(2 * time.Second)
	subscribed := make(chan struct{}, 1)
	sinks.SetOnSubscribe(func() { subscribed <- struct{}{} })
	methods := NewACPWakeMethods(sinks, tenantAgents(tenant, map[string]uuid.UUID{"resolver": agentID}))
	client, frames := gateway.NewCapturingTestClient(permissions.RoleOperator, tenant, "user-1", 8)
	ctx := store.WithTenantID(context.Background(), tenant)

	methods.handleSubscribe(ctx, client, request(t, protocol.MethodACPWakeSubscribe, map[string]string{"agentId": "resolver"}))
	if frame := readFrame(t, frames); !frame.OK {
		t.Fatalf("subscribe response = %+v", frame)
	}
	select {
	case <-subscribed:
	default:
		t.Fatal("subscribe did not wake the dispatcher")
	}

	outcomes := make(chan string, 1)
	go func() {
		outcome, _ := sinks.DeliverWake(context.Background(), tenant, agentID, "user-1", "w_abc", json.RawMessage(`{"wakeId":"w_abc"}`))
		outcomes <- outcome
	}()
	event := readFrame(t, frames)
	if event.Type != protocol.FrameTypeEvent || event.Event != protocol.EventACPWake {
		t.Fatalf("event = %+v", event)
	}
	var payload struct {
		WakeID string          `json:"wakeId"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.WakeID != "w_abc" || string(payload.Params) != `{"wakeId":"w_abc"}` {
		t.Fatalf("payload = %s", event.Payload)
	}

	methods.handleAck(ctx, client, request(t, protocol.MethodACPWakeAck, map[string]string{"wakeId": "w_abc", "outcome": "nonsense"}))
	if frame := readFrame(t, frames); frame.OK {
		t.Fatal("an unknown outcome was accepted")
	}
	methods.handleAck(ctx, client, request(t, protocol.MethodACPWakeAck, map[string]string{"wakeId": "w_abc", "outcome": protocol.ACPWakeOutcomeAccepted}))
	if frame := readFrame(t, frames); !frame.OK {
		t.Fatalf("ack response = %+v", frame)
	}
	if got := <-outcomes; got != protocol.ACPWakeOutcomeAccepted {
		t.Fatalf("outcome = %q", got)
	}
	methods.handleAck(ctx, client, request(t, protocol.MethodACPWakeAck, map[string]string{"wakeId": "w_abc", "outcome": protocol.ACPWakeOutcomeAccepted}))
	if frame := readFrame(t, frames); frame.OK || frame.Error.Code != protocol.ErrNotFound {
		t.Fatalf("late ack response = %+v", frame)
	}
}

func TestWakeMethodsWithoutRegistryAnswerUnavailable(t *testing.T) {
	methods := NewACPWakeMethods(nil, nil)
	client, frames := gateway.NewCapturingTestClient(permissions.RoleOperator, uuid.New(), "user-1", 4)
	methods.handleSubscribe(context.Background(), client, request(t, protocol.MethodACPWakeSubscribe, map[string]string{"agentId": "a"}))
	if frame := readFrame(t, frames); frame.OK {
		t.Fatal("subscribe without a registry succeeded")
	}
}
