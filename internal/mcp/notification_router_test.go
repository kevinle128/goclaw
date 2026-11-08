package mcp

import (
	"context"
	"errors"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
)

func notificationWithSubscription(method string, id any) mcpgo.JSONRPCNotification {
	n := mcpgo.JSONRPCNotification{JSONRPC: "2.0"}
	n.Method = method
	if id != nil {
		n.Params.Meta = map[string]any{mcpgo.MetaKeySubscriptionID: id}
	}
	return n
}

func TestNotificationRouterBySubscriptionID(t *testing.T) {
	r := newNotificationRouter()
	var gotA, gotB []string
	unregA, err := r.Register("goclaw-1", func(n mcpgo.JSONRPCNotification) { gotA = append(gotA, n.Method) })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register("7", func(n mcpgo.JSONRPCNotification) { gotB = append(gotB, n.Method) }); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register("goclaw-1", func(mcpgo.JSONRPCNotification) {}); !errors.Is(err, errSubscriptionInUse) {
		t.Errorf("duplicate register err = %v", err)
	}

	r.dispatch(notificationWithSubscription("a1", "goclaw-1"))
	r.dispatch(notificationWithSubscription("b1", float64(7))) // numeric id from JSON
	r.dispatch(notificationWithSubscription("unknown", "goclaw-999"))
	r.dispatch(notificationWithSubscription("no-meta", nil))
	unregA()
	unregA() // safe twice
	r.dispatch(notificationWithSubscription("a2", "goclaw-1"))

	if len(gotA) != 1 || gotA[0] != "a1" {
		t.Errorf("subscription A got %v, want [a1]", gotA)
	}
	if len(gotB) != 1 || gotB[0] != "b1" {
		t.Errorf("subscription B got %v, want [b1]", gotB)
	}
}

func TestNotificationRouterReceivesEventsStreamFrames(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
	srv.SetStreamScript(func(_ context.Context, st *mcptest.Stream, _ mcptest.Request) {
		c1, c2 := "c1", "c2"
		_ = st.Active(&c1, false)
		_ = st.Event(mcptest.EventOccurrence{EventID: "run-1:1", Name: "workflow_run.status_changed", Timestamp: "2026-11-09T00:00:00Z", Data: map[string]any{"status": "running"}, Cursor: &c2})
		_ = st.Heartbeat(&c2)
		_ = st.Complete()
	})
	ss := connectStub(t, srv, ProtocolAuto)
	router := ss.currentSession().router

	id := nextRawRequestID()
	frames := make(chan mcpgo.JSONRPCNotification, 8)
	unregister, err := router.Register(id, func(n mcpgo.JSONRPCNotification) { frames <- n })
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()

	params := map[string]any{"name": "workflow_run.status_changed", "arguments": map[string]any{"run_id": "run-1"}, "cursor": nil}
	if _, err := rawCall(context.Background(), ss.client, methodEventsStream, params, RawOpts{ID: id, Events: true}); err != nil {
		t.Fatalf("events/stream: %v", err)
	}

	want := []string{"notifications/events/active", "notifications/events/event", "notifications/events/heartbeat"}
	for i, method := range want {
		select {
		case n := <-frames:
			if n.Method != method {
				t.Errorf("frame %d = %s, want %s", i, n.Method, method)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("frame %d (%s) did not arrive", i, method)
		}
	}
}
