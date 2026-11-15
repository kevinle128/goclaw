package agent

import (
	"context"
	"testing"
)

func TestRouterInjectDetailed_NoActiveRun(t *testing.T) {
	r := NewRouter()
	got := r.InjectMessageDetailed("s1", InjectedMessage{Content: "x"})
	if got.Injected || got.Reason != InjectReasonNoActiveRun || got.RunID != "" {
		t.Fatalf("outcome = %+v, want no_active_run", got)
	}
	if r.InjectMessage("s1", InjectedMessage{Content: "x"}) {
		t.Fatal("InjectMessage = true for an idle session")
	}
}

func TestRouterInjectDetailed_Accepted(t *testing.T) {
	r := NewRouter()
	ch := r.RegisterRun(context.Background(), "run-1", "s1", "a", func() {})
	got := r.InjectMessageDetailed("s1", InjectedMessage{Content: "x"})
	if !got.Injected || got.RunID != "run-1" || got.Reason != "" {
		t.Fatalf("outcome = %+v, want injected by run-1", got)
	}
	if msg := <-ch; msg.Content != "x" {
		t.Fatalf("message = %+v", msg)
	}
	if !r.InjectMessage("s1", InjectedMessage{Content: "y"}) {
		t.Fatal("InjectMessage = false for an active run with room")
	}
}

func TestRouterInjectDetailed_ChannelFull(t *testing.T) {
	r := NewRouter()
	r.RegisterRun(context.Background(), "run-1", "s1", "a", func() {})
	for i := range injectBufferSize {
		if got := r.InjectMessageDetailed("s1", InjectedMessage{Content: "m"}); !got.Injected {
			t.Fatalf("message %d refused: %+v", i, got)
		}
	}
	got := r.InjectMessageDetailed("s1", InjectedMessage{Content: "overflow"})
	if got.Injected || got.Reason != InjectReasonFull || got.RunID != "run-1" {
		t.Fatalf("outcome = %+v, want inject_full", got)
	}
	if r.InjectMessage("s1", InjectedMessage{Content: "overflow"}) {
		t.Fatal("InjectMessage = true for a full channel")
	}
}
