package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestMCPWakeIDStable(t *testing.T) {
	row := uuid.MustParse("0193a5b0-7000-7000-8000-0000000000aa")
	a := MCPWakeID(row, MCPWakeReasonInputRequired, "approval-gate")
	b := MCPWakeID(row, MCPWakeReasonInputRequired, "approval-gate")
	if a != b || !strings.HasPrefix(a, "w_") || len(a) != 2+64 {
		t.Fatalf("wake ids %q %q", a, b)
	}
	if MCPWakeID(row, MCPWakeReasonInputRequired, "other") == a {
		t.Error("a different discriminator gave the same id")
	}
	// Fixed vector: w_ + sha256("<row id>:completed:completed").
	const want = "w_11798126763f023917ba107feefca36ac8a2244c264004f392769a0774d5e88e"
	if got := MCPWakeID(row, MCPWakeReasonCompleted, MCPWakeReasonCompleted); got != want {
		t.Fatalf("wake id = %s, want %s", got, want)
	}
}

func TestMCPWakeThreadKeyCanonical(t *testing.T) {
	a := MCPWakeThreadKey(json.RawMessage(`{"rootEventId":"r","channelId":"c"}`), "s1")
	b := MCPWakeThreadKey(json.RawMessage(`{ "channelId": "c", "rootEventId": "r" }`), "s2")
	if a != b {
		t.Errorf("key order or white space changed the thread key: %s %s", a, b)
	}
	if MCPWakeThreadKey(nil, "s1") == MCPWakeThreadKey(nil, "s2") {
		t.Error("tasks without a thread in two sessions share a key")
	}
	if MCPWakeThreadKey(json.RawMessage(`null`), "s1") != MCPWakeThreadKey(nil, "s1") {
		t.Error("a null thread must use the session fallback")
	}
}

func TestMCPTaskHasACPOrigin(t *testing.T) {
	cases := []struct {
		row  MCPTask
		want bool
	}{
		{MCPTask{}, false},
		{MCPTask{ThreadContext: json.RawMessage(`null`)}, false},
		{MCPTask{ThreadContext: json.RawMessage(`{}`)}, false},
		{MCPTask{ThreadContext: json.RawMessage(`{"channelId":"c"}`)}, true},
		{MCPTask{ACPSessionID: "s"}, true},
	}
	for i, c := range cases {
		if got := c.row.HasACPOrigin(); got != c.want {
			t.Errorf("case %d: HasACPOrigin = %v, want %v", i, got, c.want)
		}
	}
}
