package acpserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
)

const ProtocolVersion = 1

const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
	CodeGatewayError   = -32000
	CodeUnknownSession = -32002
	// CodeSteerNotApplied answers _session/steering when the message did not
	// join a running turn. It is an application error on purpose: Buzz treats
	// -32601 and a result without an outcome as "cancel the turn and merge",
	// and every other code as "keep the turn and run the message after it".
	CodeSteerNotApplied = -32003
)

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("ACP error %d: %s", e.Code, e.Message)
}

type AgentInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title"`
	Version string `json:"version"`
}

type InitializeParams struct {
	ProtocolVersion    int             `json:"protocolVersion"`
	ClientCapabilities json.RawMessage `json:"clientCapabilities,omitempty"`
	ClientInfo         *ClientInfo     `json:"clientInfo,omitempty"`
}

type ClientInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

type InitializeResult struct {
	ProtocolVersion   int               `json:"protocolVersion"`
	AgentCapabilities AgentCapabilities `json:"agentCapabilities"`
	AgentInfo         AgentInfo         `json:"agentInfo"`
	AuthMethods       []any             `json:"authMethods"`
	// Meta is the ROOT _meta of the initialize result. Buzz reads
	// _meta.steering.supported here, not under agentCapabilities._meta.
	Meta map[string]any `json:"_meta,omitempty"`
}

type AgentCapabilities struct {
	LoadSession        bool               `json:"loadSession"`
	PromptCapabilities PromptCapabilities `json:"promptCapabilities"`
	MCPCapabilities    MCPCapabilities    `json:"mcpCapabilities"`
	Meta               map[string]any     `json:"_meta,omitempty"`
}

// WakeCapabilityKey is the _meta key for the agent-initiated wake request.
// Both sides advertise it with exactly {"version": 1}.
const (
	WakeCapabilityKey     = "buzz.wake"
	WakeCapabilityVersion = 1
	ThreadContextKey      = "buzz.thread"
)

// SteeringCapabilityKey is the root _meta key of the initialize result that
// tells the client the agent accepts _session/steering mid-turn.
const (
	SteeringCapabilityKey = "steering"
	SteeringMethod        = "_session/steering"
	// SteerOutcomeInjected is the only outcome that keeps the turn alive.
	SteerOutcomeInjected = "injected"
)

// TurnAbortKey is the _meta key of a prompt result whose run stopped without a
// normal answer, and of the matching agent capability (exactly {"version": 1}).
// Clients that do not know the key see a plain end_turn result.
const (
	TurnAbortKey     = "buzz.turnAbort"
	TurnAbortVersion = 1

	// TurnAbortReasonContextOverBudget asks the client to drop the ACP session,
	// because the session cannot continue.
	TurnAbortReasonContextOverBudget = "context_over_budget"
)

// ClientCapabilities holds the client features that change agent behaviour.
type ClientCapabilities struct {
	ElicitationForm bool
	ElicitationURL  bool
	Wake            bool
}

// ParseClientCapabilities reads the features from clientCapabilities. Each
// part is decoded alone, so a bad shape in one part does not hide another.
// An absent or malformed value gives false.
func ParseClientCapabilities(raw json.RawMessage) ClientCapabilities {
	var caps ClientCapabilities
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return caps
	}
	var elicitation map[string]json.RawMessage
	if json.Unmarshal(fields["elicitation"], &elicitation) == nil {
		caps.ElicitationForm = presentNotNull(elicitation["form"])
		caps.ElicitationURL = presentNotNull(elicitation["url"])
	}
	var meta map[string]json.RawMessage
	if json.Unmarshal(fields["_meta"], &meta) == nil {
		var wake map[string]json.RawMessage
		var version int64
		caps.Wake = json.Unmarshal(meta[WakeCapabilityKey], &wake) == nil && wake != nil &&
			json.Unmarshal(wake["version"], &version) == nil && version == WakeCapabilityVersion
	}
	return caps
}

func presentNotNull(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

type PromptCapabilities struct {
	Image           bool `json:"image"`
	Audio           bool `json:"audio"`
	EmbeddedContext bool `json:"embeddedContext"`
}

type MCPCapabilities struct {
	HTTP bool `json:"http"`
	SSE  bool `json:"sse"`
}

type NewSessionParams struct {
	CWD        string      `json:"cwd"`
	MCPServers []MCPServer `json:"mcpServers"`
	// Thread is the compact JSON object from _meta["buzz.thread"], or nil.
	Thread json.RawMessage `json:"-"`
}

type MCPServer struct {
	Type    string          `json:"type,omitempty"`
	Name    string          `json:"name"`
	Command string          `json:"command"`
	Args    []string        `json:"args"`
	Env     []EnvVariable   `json:"env"`
	Meta    json.RawMessage `json:"_meta,omitempty"`
}

func (s MCPServer) Environment() []EnvVariable {
	return s.Env
}

type EnvVariable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type NewSessionResult struct {
	SessionID string `json:"sessionId"`
}

type TextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type PromptParams struct {
	SessionID string        `json:"sessionId"`
	Prompt    []TextContent `json:"prompt"`
	// Thread is the compact JSON object from _meta["buzz.thread"], or nil.
	// It is not part of the serialized prompt budget.
	Thread json.RawMessage `json:"-"`
}

type PromptResult struct {
	StopReason string         `json:"stopReason"`
	Meta       map[string]any `json:"_meta,omitempty"`
}

// SteerParams are the params of _session/steering: the session and the text
// blocks to add to the running turn.
type SteerParams struct {
	SessionID string        `json:"sessionId"`
	Prompt    []TextContent `json:"prompt"`
}

// SteerResult is the result of _session/steering. Outcome is always
// SteerOutcomeInjected; every other case is an RPC error.
type SteerResult struct {
	Outcome string `json:"outcome"`
}

type CancelParams struct {
	SessionID string `json:"sessionId"`
}

type SessionUpdateParams struct {
	SessionID string        `json:"sessionId"`
	Update    SessionUpdate `json:"update"`
}

type SessionUpdate struct {
	SessionUpdate string      `json:"sessionUpdate"`
	Content       TextContent `json:"content"`
}

func DecodeInitializeParams(raw json.RawMessage) (InitializeParams, *RPCError) {
	var params struct {
		ProtocolVersion    *int            `json:"protocolVersion"`
		ClientCapabilities json.RawMessage `json:"clientCapabilities,omitempty"`
		ClientInfo         *ClientInfo     `json:"clientInfo,omitempty"`
		Meta               json.RawMessage `json:"_meta,omitempty"`
	}
	if err := decodeStrict(raw, &params); err != nil || params.ProtocolVersion == nil {
		return InitializeParams{}, invalidParams()
	}
	if *params.ProtocolVersion != ProtocolVersion && *params.ProtocolVersion != 2 {
		return InitializeParams{}, invalidParams()
	}
	if params.ClientInfo != nil && (params.ClientInfo.Name == "" || params.ClientInfo.Version == "") {
		return InitializeParams{}, invalidParams()
	}
	return InitializeParams{
		ProtocolVersion:    *params.ProtocolVersion,
		ClientCapabilities: params.ClientCapabilities,
		ClientInfo:         params.ClientInfo,
	}, nil
}

func DecodeNewSessionParams(raw json.RawMessage) (NewSessionParams, *RPCError) {
	var wire struct {
		CWD        *string         `json:"cwd"`
		MCPServers json.RawMessage `json:"mcpServers"`
		Meta       json.RawMessage `json:"_meta,omitempty"`
	}
	if err := decodeStrict(raw, &wire); err != nil || wire.CWD == nil || *wire.CWD == "" || !filepath.IsAbs(*wire.CWD) || wire.MCPServers == nil {
		return NewSessionParams{}, invalidParams()
	}

	thread, rpcErr := decodeThreadContext(wire.Meta)
	if rpcErr != nil {
		return NewSessionParams{}, rpcErr
	}

	var servers []MCPServer
	if err := decodeStrict(wire.MCPServers, &servers); err != nil || servers == nil || len(servers) > MaxMCPServersPerSession {
		return NewSessionParams{}, invalidParams()
	}
	for i := range servers {
		server := &servers[i]
		if server.Type != "" && server.Type != "stdio" {
			return NewSessionParams{}, invalidParams()
		}
		if server.Name == "" || server.Command == "" || !filepath.IsAbs(server.Command) || server.Args == nil || server.Env == nil || len(server.Args) > MaxMCPEntries || len(server.Env) > MaxMCPEntries {
			return NewSessionParams{}, invalidParams()
		}
		for _, arg := range server.Args {
			if arg == "" {
				return NewSessionParams{}, invalidParams()
			}
		}
		for _, env := range server.Env {
			if env.Name == "" {
				return NewSessionParams{}, invalidParams()
			}
		}
	}
	return NewSessionParams{CWD: *wire.CWD, MCPServers: servers, Thread: thread}, nil
}

func DecodePromptParams(raw json.RawMessage) (PromptParams, *RPCError) {
	var wire struct {
		SessionID *string           `json:"sessionId"`
		Prompt    []json.RawMessage `json:"prompt"`
		Meta      json.RawMessage   `json:"_meta,omitempty"`
	}
	if err := decodeStrict(raw, &wire); err != nil || wire.SessionID == nil || *wire.SessionID == "" || len(wire.Prompt) == 0 {
		return PromptParams{}, invalidParams()
	}

	thread, rpcErr := decodeThreadContext(wire.Meta)
	if rpcErr != nil {
		return PromptParams{}, rpcErr
	}

	params := PromptParams{SessionID: *wire.SessionID, Prompt: make([]TextContent, 0, len(wire.Prompt)), Thread: thread}
	textBytes := 0
	for _, block := range wire.Prompt {
		var kind struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(block, &kind); err != nil || kind.Type != "text" {
			return PromptParams{}, invalidParams()
		}
		var text TextContent
		if err := decodeStrict(block, &text); err != nil || text.Type != "text" || text.Text == "" {
			return PromptParams{}, invalidParams()
		}
		textBytes += len(text.Text)
		if textBytes > MaxPromptTextBytes {
			return PromptParams{}, invalidParams()
		}
		params.Prompt = append(params.Prompt, text)
	}
	data, err := MarshalGatewayRequest(params)
	if err != nil || len(data)+len(thread)+ChatSendFrameOverhead > MaxGatewayFrameBytes {
		// chat.send also carries the thread and the other fields; it must fit
		// in one Gateway frame.
		return PromptParams{}, invalidParams()
	}
	return params, nil
}

// decodeThreadContext reads _meta["buzz.thread"]. Other _meta keys and a
// _meta value that is not an object are ignored. When the key is present, its
// value must be a JSON object of at most MaxThreadContextBytes in compact form.
func decodeThreadContext(meta json.RawMessage) (json.RawMessage, *RPCError) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(meta, &fields) != nil {
		return nil, nil
	}
	raw, ok := fields[ThreadContextKey]
	if !ok {
		return nil, nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, invalidParams()
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil || compact.Len() > MaxThreadContextBytes {
		return nil, invalidParams()
	}
	return json.RawMessage(compact.Bytes()), nil
}

// DecodeSteerParams reads _session/steering params with the same rules as a
// prompt: a session id and text blocks with non-empty text, under the prompt
// size limit. Unknown _meta content is ignored.
func DecodeSteerParams(raw json.RawMessage) (SteerParams, *RPCError) {
	var wire struct {
		SessionID *string           `json:"sessionId"`
		Prompt    []json.RawMessage `json:"prompt"`
		Meta      json.RawMessage   `json:"_meta,omitempty"`
	}
	if err := decodeStrict(raw, &wire); err != nil || wire.SessionID == nil || *wire.SessionID == "" || len(wire.Prompt) == 0 {
		return SteerParams{}, invalidParams()
	}
	params := SteerParams{SessionID: *wire.SessionID, Prompt: make([]TextContent, 0, len(wire.Prompt))}
	textBytes := 0
	for _, block := range wire.Prompt {
		var text TextContent
		if err := decodeStrict(block, &text); err != nil || text.Type != "text" || text.Text == "" {
			return SteerParams{}, invalidParams()
		}
		textBytes += len(text.Text)
		if textBytes > MaxPromptTextBytes {
			return SteerParams{}, invalidParams()
		}
		params.Prompt = append(params.Prompt, text)
	}
	return params, nil
}

func DecodeCancelParams(raw json.RawMessage) (CancelParams, *RPCError) {
	var wire struct {
		SessionID *string         `json:"sessionId"`
		Meta      json.RawMessage `json:"_meta,omitempty"`
	}
	if err := decodeStrict(raw, &wire); err != nil || wire.SessionID == nil || *wire.SessionID == "" {
		return CancelParams{}, invalidParams()
	}
	return CancelParams{SessionID: *wire.SessionID}, nil
}

func MarshalGatewayRequest(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal Gateway request: %w", err)
	}
	if len(data) > MaxGatewayFrameBytes {
		return nil, fmt.Errorf("Gateway request exceeds %d bytes", MaxGatewayFrameBytes)
	}
	return data, nil
}

func decodeStrict(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func invalidParams() *RPCError {
	return &RPCError{Code: CodeInvalidParams, Message: "Invalid params"}
}
