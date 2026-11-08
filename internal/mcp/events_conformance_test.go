package mcp

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
)

// cursorParam returns the cursor of a recorded events/* request; an absent
// or null cursor gives nil.
func cursorParam(t *testing.T, r mcptest.Request) *string {
	t.Helper()
	var c *string
	if err := r.Param("cursor", &c); err != nil && !strings.Contains(err.Error(), "absent") {
		t.Fatalf("decode cursor: %v", err)
	}
	return c
}

func requestsNamed(reqs []mcptest.Request, name string) []mcptest.Request {
	var out []mcptest.Request
	for _, r := range reqs {
		var n string
		if r.Param("name", &n) == nil && n == name {
			out = append(out, r)
		}
	}
	return out
}

func TestRunEventsClient_RejectsBadOptions(t *testing.T) {
	if err := RunEventsClient(t.Context(), "", EventsClientOptions{Duration: time.Second}); err == nil {
		t.Fatal("empty url: want error")
	}
	if err := RunEventsClient(t.Context(), "http://127.0.0.1:1/mcp", EventsClientOptions{}); err == nil {
		t.Fatal("zero duration: want error")
	}
}

func TestRunEventsClient_NoExtensionSendsNoEventsRequest(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{
		Mode:       mcptest.ModeModern,
		Extensions: map[string]any{mcptest.ExtensionTasks: map[string]any{}},
		Events:     []mcptest.EventDescriptor{{Name: "ticks", Delivery: []string{"poll"}}},
	})
	if err := RunEventsClient(t.Context(), srv.URL, EventsClientOptions{Duration: 300 * time.Millisecond}); err != nil {
		t.Fatalf("RunEventsClient: %v", err)
	}
	if len(srv.Requests()) == 0 {
		t.Fatal("client never contacted the server")
	}
	for _, m := range srv.Methods() {
		if strings.HasPrefix(m, "events/") {
			t.Fatalf("client sent %s to a server without the events extension", m)
		}
	}
}

func TestRunEventsClient_PollCursorAndTiming(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{
		Mode: mcptest.ModeModern,
		Events: []mcptest.EventDescriptor{
			{Name: "ticks", Delivery: []string{"poll"}},
			{Name: "cursorless", Delivery: []string{"poll"}},
		},
	})
	c1, c2 := "c1", "c2"
	srv.QueuePoll("ticks", mcptest.PollReply{
		Events:  []mcptest.EventOccurrence{{EventID: "e1", Name: "ticks", Cursor: &c1}},
		Cursor:  &c1,
		HasMore: true, NextPollMs: 5000,
	})
	srv.QueuePoll("ticks", mcptest.PollReply{Cursor: &c2, Truncated: true, NextPollMs: 0})
	for range 5 {
		srv.QueuePoll("cursorless", mcptest.PollReply{NextPollMs: 0})
	}

	if err := RunEventsClient(t.Context(), srv.URL, EventsClientOptions{Duration: 2500 * time.Millisecond}); err != nil {
		t.Fatalf("RunEventsClient: %v", err)
	}

	ticks := requestsNamed(srv.RequestsFor("events/poll"), "ticks")
	if len(ticks) < 3 {
		t.Fatalf("ticks polls = %d, want >= 3", len(ticks))
	}
	if c := cursorParam(t, ticks[0]); c != nil {
		t.Fatalf("first poll cursor = %q, want null", *c)
	}
	if c := cursorParam(t, ticks[1]); c == nil || *c != c1 {
		t.Fatalf("poll after hasMore cursor = %v, want %q", c, c1)
	}
	if gap := ticks[1].At.Sub(ticks[0].At); gap >= time.Second {
		t.Fatalf("poll after hasMore came %v later, want at once", gap)
	}
	if c := cursorParam(t, ticks[2]); c == nil || *c != c2 {
		t.Fatalf("poll after truncated cursor = %v, want the fresh cursor %q", c, c2)
	}
	if gap := ticks[2].At.Sub(ticks[1].At); gap < 900*time.Millisecond {
		t.Fatalf("poll after nextPollMs 0 came %v later, want the 1 s floor", gap)
	}

	cursorless := requestsNamed(srv.RequestsFor("events/poll"), "cursorless")
	if len(cursorless) < 2 {
		t.Fatalf("cursorless polls = %d, want >= 2", len(cursorless))
	}
	for i, r := range cursorless {
		if c := cursorParam(t, r); c != nil {
			t.Fatalf("cursorless poll %d sent cursor %q, want null", i, *c)
		}
	}
}

func TestRunEventsClient_SilentStreamReconnectsWithCursor(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{
		Mode:   mcptest.ModeModern,
		Events: []mcptest.EventDescriptor{{Name: "ticks", Delivery: []string{"push"}}},
	})
	var opened atomic.Int32
	srv.SetStreamScript(func(ctx context.Context, st *mcptest.Stream, _ mcptest.Request) {
		p0, p1 := "p-0", "p-1"
		_ = st.Active(&p0, false)
		if opened.Add(1) == 1 {
			_ = st.Heartbeat(&p1)
		}
		// Silent until the client closes the stream.
		<-ctx.Done()
	})

	err := RunEventsClient(t.Context(), srv.URL, EventsClientOptions{Duration: 2 * time.Second, Heartbeat: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("RunEventsClient: %v", err)
	}

	streams := srv.RequestsFor("events/stream")
	if len(streams) < 2 {
		t.Fatalf("events/stream requests = %d, want a reconnect after the silence", len(streams))
	}
	if c := cursorParam(t, streams[0]); c != nil {
		t.Fatalf("first stream cursor = %q, want null", *c)
	}
	if c := cursorParam(t, streams[1]); c == nil || *c != "p-1" {
		t.Fatalf("reconnect cursor = %v, want the last heartbeat cursor p-1", c)
	}
}
