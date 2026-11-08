package methods

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
	"github.com/nextlevelbuilder/goclaw/internal/gateway"
	mcpbridge "github.com/nextlevelbuilder/goclaw/internal/mcp"
	"github.com/nextlevelbuilder/goclaw/internal/permissions"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

func TestACPInputRespondDeliversAnswerOfOwnerOnly(t *testing.T) {
	tenant := uuid.New()
	relays := acpbridge.NewInputRelays()
	methods := NewACPInputMethods(relays)
	owner, ownerFrames := gateway.NewCapturingTestClient(permissions.RoleOperator, tenant, "user-1", 8)
	other, otherFrames := gateway.NewCapturingTestClient(permissions.RoleOperator, tenant, "user-1", 8)
	relay := relays.Bind(acpbridge.InputBinding{ConnectionID: owner.ID(), SessionKey: "key-1", Generation: 4, Sender: owner, CanAskForm: true})

	type answer struct {
		action  string
		content json.RawMessage
		err     error
	}
	answers := make(chan answer, 1)
	go func() {
		action, content, err := relay.AskForm(context.Background(), "call-1", "Approve?", json.RawMessage(`{"type":"object"}`))
		answers <- answer{action, content, err}
	}()
	event := readFrame(t, ownerFrames)
	if event.Event != protocol.EventACPInputRequest {
		t.Fatalf("event = %+v", event)
	}
	var payload struct {
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.RequestID == "" {
		t.Fatalf("payload = %s", event.Payload)
	}
	respond := func(client *gateway.Client, generation uint64, action string, content string) {
		params := map[string]any{"sessionKey": "key-1", "acpGeneration": generation, "requestId": payload.RequestID, "action": action}
		if content != "" {
			params["content"] = json.RawMessage(content)
		}
		methods.handleRespond(context.Background(), client, request(t, protocol.MethodACPInputRespond, params))
	}

	respond(other, 4, "accept", `{"decision":"approve"}`)
	if frame := readFrame(t, otherFrames); frame.OK || frame.Error.Code != protocol.ErrUnauthorized {
		t.Fatalf("answer from another connection = %+v", frame)
	}
	respond(owner, 5, "accept", `{"decision":"approve"}`)
	if frame := readFrame(t, ownerFrames); frame.OK || frame.Error.Code != protocol.ErrUnauthorized {
		t.Fatalf("answer of another generation = %+v", frame)
	}
	respond(owner, 4, "accept", `{"decision":"approve","comment":"ship it"}`)
	if frame := readFrame(t, ownerFrames); !frame.OK {
		t.Fatalf("owner answer = %+v", frame)
	}
	got := <-answers
	if got.err != nil || got.action != "accept" || string(got.content) != `{"decision":"approve","comment":"ship it"}` {
		t.Fatalf("AskForm() = %+v", got)
	}
	respond(owner, 4, "decline", "")
	if frame := readFrame(t, ownerFrames); frame.OK || frame.Error.Code != protocol.ErrNotFound {
		t.Fatalf("second answer = %+v", frame)
	}
}

func TestChatACPRunContextNeedsACPClient(t *testing.T) {
	relays := acpbridge.NewInputRelays()
	m := &ChatMethods{inputRelays: relays}
	client := gateway.NewTestClient(permissions.RoleOperator, uuid.New(), "user-1")
	primary := chatSendRequest{client: client, ownerConnectionID: client.ID(), sessionKey: "agent:a:ws:direct:s"}

	// An older adapter sends no acpClient: the run gets no ACP values.
	ctx := m.acpRunContext(context.Background(), primary, chatSendParams{ACPGeneration: 2})
	if _, ok := mcpbridge.ACPOriginFromContext(ctx); ok || mcpbridge.InputRelayFromContext(ctx) != nil || mcpbridge.ElicitationFormFromContext(ctx) {
		t.Fatal("ACP values set without acpClient")
	}

	thread := json.RawMessage(`{"channelId":"c1","rootEventId":"e1"}`)
	params := chatSendParams{ACPGeneration: 2, ACPClient: &chatACPClient{ElicitationForm: true, Wake: true, Thread: thread, ACPSessionID: "acp-1"}}
	ctx = m.acpRunContext(context.Background(), primary, params)
	origin, ok := mcpbridge.ACPOriginFromContext(ctx)
	if !ok || origin.SessionID != "acp-1" || string(origin.Thread) != string(thread) {
		t.Fatalf("origin = %+v, %v", origin, ok)
	}
	relay := mcpbridge.InputRelayFromContext(ctx)
	if relay == nil || !relay.CanAskForm() || !mcpbridge.ElicitationFormFromContext(ctx) {
		t.Fatal("form relay missing")
	}

	params.ACPClient.ElicitationForm = false
	ctx = m.acpRunContext(context.Background(), primary, params)
	if relay := mcpbridge.InputRelayFromContext(ctx); relay == nil || relay.CanAskForm() || mcpbridge.ElicitationFormFromContext(ctx) {
		t.Fatal("form elicitation declared without client support")
	}

	// Not an ACP prompt of an owner connection.
	ctx = m.acpRunContext(context.Background(), chatSendRequest{client: client}, params)
	if _, ok := mcpbridge.ACPOriginFromContext(ctx); ok {
		t.Fatal("ACP origin set for a run without an owner connection")
	}
}

func TestChatSendParamsAcceptACPClientAndOlderShapes(t *testing.T) {
	var newer chatSendParams
	if err := json.Unmarshal([]byte(`{"message":"hi","acpGeneration":3,"acpClient":{"elicitationForm":true,"wake":true,"thread":{"channelId":"c"},"acpSessionId":"s","future":1}}`), &newer); err != nil {
		t.Fatal(err)
	}
	if newer.ACPClient == nil || !newer.ACPClient.ElicitationForm || !newer.ACPClient.Wake || newer.ACPClient.ACPSessionID != "s" || string(newer.ACPClient.Thread) != `{"channelId":"c"}` {
		t.Fatalf("acpClient = %+v", newer.ACPClient)
	}
	var older chatSendParams
	if err := json.Unmarshal([]byte(`{"message":"hi","acpGeneration":3,"terminalMode":"metadata"}`), &older); err != nil || older.ACPClient != nil {
		t.Fatalf("older params = %+v, %v", older, err)
	}
}
