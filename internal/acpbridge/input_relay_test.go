package acpbridge

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// chanSender gives each sent event to a channel, so a test can answer it.
type chanSender struct {
	events chan protocol.EventFrame
	err    error
}

func newChanSender() *chanSender { return &chanSender{events: make(chan protocol.EventFrame, 16)} }

func (s *chanSender) SendCriticalEvent(event protocol.EventFrame) error {
	if s.err != nil {
		return s.err
	}
	s.events <- event
	return nil
}

func (s *chanSender) next(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	select {
	case event := <-s.events:
		data, err := json.Marshal(event.Payload)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Fatal(err)
		}
		payload["_event"], _ = json.Marshal(event.Event)
		return payload
	case <-time.After(2 * time.Second):
		t.Fatal("no event was sent")
		return nil
	}
}

func jsonString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("not a JSON string: %s", raw)
	}
	return s
}

type askResult struct {
	action  string
	content json.RawMessage
	err     error
}

func startAsk(ctx context.Context, relay *InputRelay, schema string) <-chan askResult {
	results := make(chan askResult, 1)
	go func() {
		action, content, err := relay.AskForm(ctx, "call-7", "Approve the merge?", json.RawMessage(schema))
		results <- askResult{action, content, err}
	}()
	return results
}

func waitAsk(t *testing.T, results <-chan askResult) askResult {
	t.Helper()
	select {
	case r := <-results:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("AskForm did not return")
		return askResult{}
	}
}

const testSchema = `{"type":"object","properties":{"decision":{"type":"string","enum":["approve","reject"]}},"required":["decision"]}`

func TestInputRelayDeliversAnswerToWaiter(t *testing.T) {
	relays := NewInputRelays()
	sender := newChanSender()
	relay := relays.Bind(InputBinding{ConnectionID: "conn-1", SessionKey: "agent:a:ws:direct:s1", Generation: 3, Sender: sender, CanAskForm: true})
	results := startAsk(context.Background(), relay, testSchema)

	payload := sender.next(t)
	if got := jsonString(t, payload["_event"]); got != protocol.EventACPInputRequest {
		t.Fatalf("event = %q", got)
	}
	for field, want := range map[string]string{"sessionKey": "agent:a:ws:direct:s1", "toolCallId": "call-7", "mode": "form", "message": "Approve the merge?"} {
		if got := jsonString(t, payload[field]); got != want {
			t.Fatalf("%s = %q, want %q", field, got, want)
		}
	}
	if string(payload["acpGeneration"]) != "3" || string(payload["requestedSchema"]) != testSchema {
		t.Fatalf("payload = %v", payload)
	}
	requestID := jsonString(t, payload["requestId"])

	content := json.RawMessage(`{"decision":"approve","comment":"ok"}`)
	if err := relays.Respond("conn-1", "agent:a:ws:direct:s1", 3, requestID, InputActionAccept, content); err != nil {
		t.Fatal(err)
	}
	got := waitAsk(t, results)
	if got.err != nil || got.action != InputActionAccept || string(got.content) != string(content) {
		t.Fatalf("AskForm() = %q, %s, %v", got.action, got.content, got.err)
	}
	if err := relays.Respond("conn-1", "agent:a:ws:direct:s1", 3, requestID, InputActionDecline, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second answer error = %v, want %v", err, ErrNotFound)
	}
	if relays.Pending() != 0 {
		t.Fatalf("pending = %d", relays.Pending())
	}
}

func TestInputRelayRejectsAnswerFromOtherConnectionOrGeneration(t *testing.T) {
	relays := NewInputRelays()
	sender := newChanSender()
	relay := relays.Bind(InputBinding{ConnectionID: "conn-1", SessionKey: "key-1", Generation: 2, Sender: sender, CanAskForm: true})
	results := startAsk(context.Background(), relay, testSchema)
	requestID := jsonString(t, sender.next(t)["requestId"])

	for name, answer := range map[string]struct {
		conn, key  string
		generation uint64
	}{
		"other connection": {"conn-2", "key-1", 2},
		"other session":    {"conn-1", "key-2", 2},
		"other generation": {"conn-1", "key-1", 3},
	} {
		if err := relays.Respond(answer.conn, answer.key, answer.generation, requestID, InputActionAccept, json.RawMessage(`{"decision":"approve"}`)); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("%s: error = %v, want %v", name, err, ErrNotOwner)
		}
	}
	if err := relays.Respond("conn-1", "key-1", 2, "unknown", InputActionCancel, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown request error = %v", err)
	}
	// A bad answer from another connection does not end the request.
	if err := relays.Respond("conn-2", "key-1", 2, requestID, "maybe", nil); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("bad answer of another connection: error = %v, want %v", err, ErrNotOwner)
	}
	if err := relays.Respond("conn-1", "key-1", 2, requestID, InputActionDecline, json.RawMessage(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	got := waitAsk(t, results)
	if got.err != nil || got.action != InputActionDecline || got.content != nil {
		t.Fatalf("AskForm() = %q, %s, %v", got.action, got.content, got.err)
	}
}

func TestInputRelayInvalidAnswerCancelsWaiter(t *testing.T) {
	cases := map[string]struct {
		action  string
		content json.RawMessage
	}{
		"bad action":         {"maybe", nil},
		"non-object content": {InputActionAccept, json.RawMessage(`[1]`)},
		"content too large":  {InputActionAccept, json.RawMessage(`{"note":"` + strings.Repeat("a", MaxInputContentBytes) + `"}`)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			relays := NewInputRelays()
			sender := newChanSender()
			relay := relays.Bind(InputBinding{ConnectionID: "conn-1", SessionKey: "key-1", Generation: 2, Sender: sender, CanAskForm: true})
			results := startAsk(context.Background(), relay, testSchema)
			requestID := jsonString(t, sender.next(t)["requestId"])

			if err := relays.Respond("conn-1", "key-1", 2, requestID, c.action, c.content); !errors.Is(err, ErrInputInvalid) {
				t.Fatalf("error = %v, want %v", err, ErrInputInvalid)
			}
			got := waitAsk(t, results)
			if got.err != nil || got.action != InputActionCancel || got.content != nil {
				t.Fatalf("AskForm() = %q, %s, %v; want cancel", got.action, got.content, got.err)
			}
			if relays.Pending() != 0 {
				t.Fatalf("pending = %d", relays.Pending())
			}
		})
	}
}

func TestInputRelayEndsAtContextCancel(t *testing.T) {
	relays := NewInputRelays()
	sender := newChanSender()
	relay := relays.Bind(InputBinding{ConnectionID: "conn-1", SessionKey: "key-1", Generation: 1, Sender: sender, CanAskForm: true})
	ctx, cancel := context.WithCancel(context.Background())
	results := startAsk(ctx, relay, testSchema)
	sender.next(t)
	cancel()
	if got := waitAsk(t, results); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("AskForm() error = %v", got.err)
	}
	if relays.Pending() != 0 {
		t.Fatalf("pending = %d after cancel", relays.Pending())
	}
}

func TestInputRelayEndsWhenConnectionCloses(t *testing.T) {
	relays := NewInputRelays()
	sender := newChanSender()
	relay := relays.Bind(InputBinding{ConnectionID: "conn-1", SessionKey: "key-1", Generation: 1, Sender: sender, CanAskForm: true})
	results := startAsk(context.Background(), relay, testSchema)
	sender.next(t)
	relays.ReleaseConnection("conn-2")
	relays.ReleaseConnection("conn-1")
	if got := waitAsk(t, results); !errors.Is(got.err, ErrInputClosed) {
		t.Fatalf("AskForm() error = %v", got.err)
	}
}

func TestInputRelayRefusesWithoutFormSupportOrBadSchema(t *testing.T) {
	relays := NewInputRelays()
	sender := newChanSender()
	noForm := relays.Bind(InputBinding{ConnectionID: "conn-1", SessionKey: "key-1", Generation: 1, Sender: sender})
	if noForm.CanAskForm() {
		t.Fatal("CanAskForm() = true without form support")
	}
	if _, _, err := noForm.AskForm(context.Background(), "c", "m", json.RawMessage(testSchema)); !errors.Is(err, ErrInputUnsupported) {
		t.Fatalf("error = %v", err)
	}
	relay := relays.Bind(InputBinding{ConnectionID: "conn-1", SessionKey: "key-1", Generation: 1, Sender: sender, CanAskForm: true})
	if _, _, err := relay.AskForm(context.Background(), "c", "m", json.RawMessage(`"x"`)); !errors.Is(err, ErrInputInvalid) {
		t.Fatalf("bad schema error = %v", err)
	}
	if len(sender.events) != 0 || relays.Pending() != 0 {
		t.Fatal("a refused request was sent or kept")
	}
}

func TestInputRelayLimitsPendingPerConnection(t *testing.T) {
	relays := NewInputRelays()
	sender := &chanSender{events: make(chan protocol.EventFrame, MaxPendingInputsPerConnection+1)}
	relay := relays.Bind(InputBinding{ConnectionID: "conn-1", SessionKey: "key-1", Generation: 1, Sender: sender, CanAskForm: true})
	ctx := t.Context()
	for range MaxPendingInputsPerConnection {
		startAsk(ctx, relay, testSchema)
	}
	deadline := time.Now().Add(2 * time.Second)
	for relays.Pending() < MaxPendingInputsPerConnection {
		if time.Now().After(deadline) {
			t.Fatal("requests did not register")
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, err := relay.AskForm(ctx, "c", "m", json.RawMessage(testSchema)); !errors.Is(err, ErrQuota) {
		t.Fatalf("error = %v, want %v", err, ErrQuota)
	}
}
