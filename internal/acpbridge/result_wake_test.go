package acpbridge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

type fakeWakeSender struct {
	outcomes []string
	noSink   bool
	calls    []json.RawMessage
	user     string
	agent    uuid.UUID
}

func (f *fakeWakeSender) DeliverWake(_ context.Context, _, agentID uuid.UUID, userID, _ string, params json.RawMessage) (string, bool) {
	f.calls = append(f.calls, params)
	f.user, f.agent = userID, agentID
	if f.noSink {
		return "", false
	}
	out := f.outcomes[min(len(f.calls)-1, len(f.outcomes)-1)]
	return out, true
}

func testOrigin() SessionOrigin {
	return SessionOrigin{
		TenantID: uuid.New(), AgentID: uuid.New(), UserID: "u1", ACPSessionID: "s1", Wake: true,
		Thread: json.RawMessage(`{"channelId":"c","rootEventId":"r"}`),
	}
}

func TestResultWakerSendsThreadAndPrompt(t *testing.T) {
	sender := &fakeWakeSender{outcomes: []string{protocol.ACPWakeOutcomeAccepted}}
	w := NewResultWaker(NewSessionOrigins(), sender)
	origin := testOrigin()
	if !w.Wake(context.Background(), origin, "w_1", "completed", "post it") {
		t.Fatal("accepted wake must report delivered")
	}
	var got map[string]any
	if err := json.Unmarshal(sender.calls[0], &got); err != nil {
		t.Fatal(err)
	}
	thread, _ := got["thread"].(map[string]any)
	if thread["channelId"] != "c" || got["reason"] != "completed" || got["prompt"] != "post it" || got["wakeId"] != "w_1" {
		t.Fatalf("params = %v", got)
	}
	if sender.user != "u1" || sender.agent != origin.AgentID {
		t.Fatalf("identity user=%q agent=%v", sender.user, sender.agent)
	}
}

func TestResultWakerRetriesBusyThenDelivers(t *testing.T) {
	sender := &fakeWakeSender{outcomes: []string{protocol.ACPWakeOutcomeBusy, protocol.ACPWakeOutcomeAccepted}}
	w := NewResultWaker(NewSessionOrigins(), sender)
	w.BusyRetry = time.Millisecond
	if !w.Wake(context.Background(), testOrigin(), "w_1", "completed", "p") || len(sender.calls) != 2 {
		t.Fatalf("calls = %d", len(sender.calls))
	}
}

func TestResultWakerFailsClosed(t *testing.T) {
	ctx := context.Background()
	noWake := testOrigin()
	noWake.Wake = false
	noThread := testOrigin()
	noThread.Thread = nil
	tests := map[string]struct {
		origin SessionOrigin
		sender *fakeWakeSender
		prompt string
	}{
		"client without wake":  {noWake, &fakeWakeSender{outcomes: []string{"accepted"}}, "p"},
		"no thread":            {noThread, &fakeWakeSender{outcomes: []string{"accepted"}}, "p"},
		"no live sink":         {testOrigin(), &fakeWakeSender{noSink: true}, "p"},
		"unsupported":          {testOrigin(), &fakeWakeSender{outcomes: []string{"unsupported"}}, "p"},
		"prompt over limit":    {testOrigin(), &fakeWakeSender{outcomes: []string{"accepted"}}, string(make([]byte, MaxResultWakePromptBytes+1))},
		"busy until the limit": {testOrigin(), &fakeWakeSender{outcomes: []string{"busy"}}, "p"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			w := NewResultWaker(NewSessionOrigins(), tc.sender)
			w.BusyRetry, w.BusyTries = time.Millisecond, 2
			if w.Wake(ctx, tc.origin, "w_1", "completed", tc.prompt) {
				t.Fatal("wake must not report delivered")
			}
		})
	}
}

func TestSessionOriginsLookupAndBounds(t *testing.T) {
	s := NewSessionOrigins()
	now := time.Now()
	s.now = func() time.Time { return now }
	s.Remember("k1", testOrigin())
	if o, ok := s.Lookup("k1"); !ok || o.UserID != "u1" {
		t.Fatalf("lookup = %v %v", o, ok)
	}
	if _, ok := s.Lookup("other"); ok {
		t.Fatal("unknown key must not match")
	}
	now = now.Add(sessionOriginTTL + time.Second)
	if _, ok := s.Lookup("k1"); ok {
		t.Fatal("expired entry must not match")
	}
	for i := range maxSessionOrigins + 10 {
		now = now.Add(time.Millisecond)
		s.Remember(uuid.NewString()+string(rune('a'+i%26)), testOrigin())
	}
	if len(s.entries) > maxSessionOrigins {
		t.Fatalf("entries = %d", len(s.entries))
	}
	var nilOrigins *SessionOrigins
	nilOrigins.Remember("k", testOrigin())
	if _, ok := nilOrigins.Lookup("k"); ok {
		t.Fatal("nil registry matches nothing")
	}
}
