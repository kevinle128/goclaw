package mcp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
)

func notification(method string, params map[string]any) mcpgo.JSONRPCNotification {
	n := mcpgo.JSONRPCNotification{}
	n.Method = method
	n.Params.AdditionalFields = params
	n.Params.Meta = map[string]any{mcpgo.MetaKeySubscriptionID: "goclaw-1"}
	return n
}

func TestDecodeStreamFrame(t *testing.T) {
	f, ok := decodeStreamFrame(notification(notifEventsActive, map[string]any{"cursor": "c1", "truncated": true}))
	if !ok || f.Kind != frameActive || deref(f.Cursor) != "c1" || !f.Truncated {
		t.Fatalf("active = %+v", f)
	}
	f, ok = decodeStreamFrame(notification(notifEventsHeartbeat, map[string]any{"cursor": nil}))
	if !ok || f.Kind != frameHeartbeat || f.Cursor != nil {
		t.Fatalf("heartbeat = %+v", f)
	}
	f, ok = decodeStreamFrame(notification(notifEventsEvent, map[string]any{
		"eventId": "run:1", "name": testEvent, "data": map[string]any{"pr_url": "u"}, "cursor": "c2",
	}))
	if !ok || f.Kind != frameEvent || f.Event.EventID != "run:1" || deref(f.Cursor) != "c2" || !eventPRURL(f.Event) {
		t.Fatalf("event = %+v", f)
	}
	f, ok = decodeStreamFrame(notification(notifEventsTerminated, map[string]any{"error": map[string]any{"code": -32024, "message": "no"}}))
	if !ok || f.Kind != frameTerminated || f.Error == nil || f.Error.Code != -32024 {
		t.Fatalf("terminated = %+v", f)
	}
	if _, ok := decodeStreamFrame(notification("notifications/events/unknown", nil)); ok {
		t.Error("an unknown method must be ignored")
	}
	if _, ok := decodeStreamFrame(notification(notifEventsEvent, map[string]any{"name": "x"})); ok {
		t.Error("an event without eventId must be ignored")
	}
}

func TestEventPRURLReadsOnlyNonEmptyString(t *testing.T) {
	for data, want := range map[string]bool{
		`{"pr_url":"https://x/pr/1"}`: true,
		`{"pr_url":""}`:               false,
		`{"pr_url":1}`:                false,
		`{"pr_url":null}`:             false,
		`{}`:                          false,
		`"text"`:                      false,
	} {
		if got := eventPRURL(&EventOccurrence{EventID: "e", Data: json.RawMessage(data)}); got != want {
			t.Errorf("eventPRURL(%s) = %v, want %v", data, got, want)
		}
	}
}

func TestEventDedupKeepsWindow(t *testing.T) {
	d := newEventDedup(2)
	if !d.firstSeen("a") || d.firstSeen("a") || !d.firstSeen("b") || !d.firstSeen("c") {
		t.Fatal("dedup basics")
	}
	if !d.firstSeen("a") {
		t.Error("an id outside the window must count as new")
	}
}

func TestOpenEventStreamRoutesOnlyItsFrames(t *testing.T) {
	srv, ss := newModernStub(t)
	srv.SetStreamScript(func(ctx context.Context, st *mcptest.Stream, _ mcptest.Request) {
		_ = st.Active(new("c1"), false)
		_ = st.Event(mcptest.EventOccurrence{EventID: "e1", Name: "x", Data: map[string]any{}})
		<-ctx.Done()
	})
	st, err := openEventStream(context.Background(), ss.currentSession(), "x", nil, nil, RawOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, want := range []streamFrameKind{frameActive, frameEvent} {
		select {
		case f := <-st.Frames:
			if f.Kind != want {
				t.Fatalf("frame kind %d, want %d", f.Kind, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no frame")
		}
	}
	req := srv.RequestsFor(methodEventsStream)[0]
	if !req.HasExtension(mcptest.ExtensionEvents) {
		t.Error("events/stream must declare the events extension")
	}
	st.Close()
	select {
	case <-st.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not end the request")
	}
}
