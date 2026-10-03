package acpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

func TestPromptPreservesBlockOrder(t *testing.T) {
	got := joinPrompt([]TextContent{{Type: "text", Text: "one"}, {Type: "text", Text: "two"}, {Type: "text", Text: "three"}})
	if got != "one\ntwo\nthree" {
		t.Fatalf("prompt = %q", got)
	}
}

func TestMapGatewayToolLifecycle(t *testing.T) {
	transportOutput := &bytes.Buffer{}
	transport := NewTransport(nil, transportOutput, Handler{}, "test")
	transport.outputQ = make(chan []byte, 2)
	transport.terminal = make(chan error, 1)
	transport.writeDone = make(chan error, 1)
	go transport.writeLoop()
	payload, err := json.Marshal(map[string]any{"payload": map[string]any{"name": "mcp_buzz__send", "id": "call-1", "arguments": map[string]any{"text": "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := emitGatewayTool(context.Background(), transport, "session-1", GatewayEvent{Type: protocol.AgentEventToolCall, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	transport.stopOutput()
	if err := <-transport.writeDone; err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(transportOutput.Bytes(), []byte(`"sessionUpdate":"tool_call"`)) || !bytes.Contains(transportOutput.Bytes(), []byte(`"toolCallId":"call-1"`)) {
		t.Fatalf("output = %s", transportOutput.Bytes())
	}
}

func TestMapGatewayChunk(t *testing.T) {
	payload, err := json.Marshal(map[string]any{"payload": map[string]string{"content": "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	kind, text, ok := gatewayEventText(GatewayEvent{Type: protocol.ChatEventChunk, Payload: payload})
	if !ok || kind != "agent_message_chunk" || text != "hello" {
		t.Fatalf("mapped = %q %q %v", kind, text, ok)
	}
}
