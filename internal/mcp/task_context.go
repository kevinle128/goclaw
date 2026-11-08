package mcp

import (
	"bytes"
	"context"
	"encoding/json"
)

const (
	ctxACPOrigin  mcpContextKey = "mcp_acp_origin"
	ctxInputRelay mcpContextKey = "mcp_input_relay"
	ctxToolCallID mcpContextKey = "mcp_tool_call_id"
)

// maxThreadContextBytes bounds the opaque ACP thread object that goclaw
// stores with a task and echoes in its wakes.
const maxThreadContextBytes = 4 << 10

// ACPOrigin tells which ACP turn runs the agent: the ACP session id and the
// opaque thread object of the client (for example
// {"channelId": "...", "rootEventId": "..."}). A task started in such a turn
// gets wakes; a task without an origin only updates its status.
type ACPOrigin struct {
	SessionID string
	Thread    json.RawMessage
}

// WithACPOrigin marks the run as an ACP turn.
func WithACPOrigin(ctx context.Context, origin ACPOrigin) context.Context {
	return context.WithValue(ctx, ctxACPOrigin, origin)
}

// ACPOriginFromContext returns the value set by WithACPOrigin.
func ACPOriginFromContext(ctx context.Context) (ACPOrigin, bool) {
	o, ok := ctx.Value(ctxACPOrigin).(ACPOrigin)
	return o, ok
}

// sanitizeThread returns the thread object when it is a JSON object of at
// most 4 KiB, compacted, and nil otherwise.
func sanitizeThread(raw json.RawMessage) (json.RawMessage, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return nil, true
	}
	if len(trimmed) > maxThreadContextBytes || trimmed[0] != '{' {
		return nil, false
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, trimmed); err != nil {
		return nil, false
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(buf.Bytes(), &obj); err != nil {
		return nil, false
	}
	return json.RawMessage(buf.Bytes()), true
}

// InputRelay asks the people of the current ACP turn to answer a form
// request of a task (ACP elicitation/create in form mode). The gateway puts
// one in the tool context of an ACP run.
type InputRelay interface {
	// CanAskForm reports whether the ACP client declared form elicitation.
	CanAskForm() bool
	// AskForm shows message and the flat requested schema to the people and
	// waits for the answer or the end of ctx. action is "accept", "decline"
	// or "cancel"; content is the answer object for "accept".
	AskForm(ctx context.Context, toolCallID, message string, schema json.RawMessage) (action string, content json.RawMessage, err error)
}

// WithInputRelay adds an InputRelay to the tool context.
func WithInputRelay(ctx context.Context, relay InputRelay) context.Context {
	return context.WithValue(ctx, ctxInputRelay, relay)
}

// InputRelayFromContext returns the InputRelay of the run, or nil.
func InputRelayFromContext(ctx context.Context) InputRelay {
	r, _ := ctx.Value(ctxInputRelay).(InputRelay)
	return r
}

// WithToolCallID adds the LLM tool call id of the current tool call. ACP
// uses it as toolCallId to scope an elicitation to that call.
func WithToolCallID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxToolCallID, id)
}

// ToolCallIDFromContext returns the value set by WithToolCallID.
func ToolCallIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxToolCallID).(string)
	return v
}
