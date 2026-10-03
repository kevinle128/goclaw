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
}

type AgentCapabilities struct {
	LoadSession        bool               `json:"loadSession"`
	PromptCapabilities PromptCapabilities `json:"promptCapabilities"`
	MCPCapabilities    MCPCapabilities    `json:"mcpCapabilities"`
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
}

type PromptResult struct {
	StopReason string `json:"stopReason"`
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
	return NewSessionParams{CWD: *wire.CWD, MCPServers: servers}, nil
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

	params := PromptParams{SessionID: *wire.SessionID, Prompt: make([]TextContent, 0, len(wire.Prompt))}
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
	if _, err := MarshalGatewayRequest(params); err != nil {
		return PromptParams{}, invalidParams()
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
