package mcp

import (
	"encoding/json"
	"errors"
	"fmt"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// Extension identifiers of protocol revision 2026-07-28. A server advertises
// them in the server/discover capabilities; a client declares them per
// request in _meta clientCapabilities.
const (
	ExtensionTasks  = "io.modelcontextprotocol/tasks"
	ExtensionEvents = "io.modelcontextprotocol/events"
)

// Result discriminator values of protocol revision 2026-07-28. A missing
// resultType from an older server means "complete".
const (
	ResultTypeComplete      = "complete"
	ResultTypeTask          = "task"
	ResultTypeInputRequired = "input_required"
)

// Task status values (tasks extension).
const (
	TaskStatusWorking       = "working"
	TaskStatusInputRequired = "input_required"
	TaskStatusCompleted     = "completed"
	TaskStatusCancelled     = "cancelled"
	TaskStatusFailed        = "failed"
)

// JSON-RPC error codes that the modern request layer reports as typed errors.
const (
	CodeHeaderMismatch                  = -32020
	CodeMissingRequiredClientCapability = -32021
	CodeUnsupportedProtocolVersion      = -32022
	CodeMethodNotFound                  = -32601
	CodeInvalidParams                   = -32602
	// Events extension error codes.
	CodeEventsNotFound            = -32023
	CodeEventsForbidden           = -32024
	CodeEventsResourceExhausted   = -32025
	CodeEventsUnsupported         = -32026
	CodeEventsCallbackEndpointErr = -32027
)

// Sentinel errors for errors.Is checks. An *RPCError matches the sentinel
// with the same code.
var (
	ErrHeaderMismatch                  = &RPCError{Code: CodeHeaderMismatch, Message: "header mismatch"}
	ErrMissingRequiredClientCapability = &RPCError{Code: CodeMissingRequiredClientCapability, Message: "missing required client capability"}
	ErrUnsupportedProtocolVersion      = &RPCError{Code: CodeUnsupportedProtocolVersion, Message: "unsupported protocol version"}
	ErrMethodNotFound                  = &RPCError{Code: CodeMethodNotFound, Message: "method not found"}
	ErrInvalidParams                   = &RPCError{Code: CodeInvalidParams, Message: "invalid params"}
	ErrEventsNotFound                  = &RPCError{Code: CodeEventsNotFound, Message: "event not found"}
	ErrEventsForbidden                 = &RPCError{Code: CodeEventsForbidden, Message: "event subscription forbidden"}
	ErrEventsResourceExhausted         = &RPCError{Code: CodeEventsResourceExhausted, Message: "event resource exhausted"}
	ErrEventsUnsupported               = &RPCError{Code: CodeEventsUnsupported, Message: "event feature unsupported"}
	ErrEventsCallbackEndpoint          = &RPCError{Code: CodeEventsCallbackEndpointErr, Message: "event callback endpoint error"}
)

// RPCError is a JSON-RPC error returned by a modern MCP server.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
	// Method is the request method that got the error (not on the wire).
	Method string `json:"-"`
}

func (e *RPCError) Error() string {
	if e.Method != "" {
		return fmt.Sprintf("mcp %s: %s (code %d)", e.Method, e.Message, e.Code)
	}
	return fmt.Sprintf("mcp: %s (code %d)", e.Message, e.Code)
}

// Is matches another *RPCError with the same code, so that
// errors.Is(err, ErrMethodNotFound) works for any message text.
func (e *RPCError) Is(target error) bool {
	var t *RPCError
	if !errors.As(target, &t) {
		return false
	}
	return t.Code == e.Code
}

// TaskError is the JSON-RPC error object of a failed task.
type TaskError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// CreateTaskResult is the flat result of a task-augmented tools/call
// (resultType "task"). The wire shape has no "task" wrapper.
type CreateTaskResult struct {
	ResultType     string `json:"resultType"`
	TaskID         string `json:"taskId"`
	Status         string `json:"status"`
	StatusMessage  string `json:"statusMessage,omitempty"`
	CreatedAt      string `json:"createdAt,omitempty"`
	LastUpdatedAt  string `json:"lastUpdatedAt,omitempty"`
	TTLMs          *int64 `json:"ttlMs"`
	PollIntervalMs *int64 `json:"pollIntervalMs,omitempty"`
}

// InputRequest is one entry of DetailedTask.InputRequests: a server request
// (for example elicitation/create) that the client answers with tasks/update.
type InputRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

// DetailedTask is the result of tasks/get and the params of
// notifications/tasks. Fields that apply only to some statuses are optional:
// InputRequests (input_required), Result (completed), Error (failed).
type DetailedTask struct {
	ResultType     string                  `json:"resultType,omitempty"`
	TaskID         string                  `json:"taskId"`
	Status         string                  `json:"status"`
	StatusMessage  string                  `json:"statusMessage,omitempty"`
	CreatedAt      string                  `json:"createdAt,omitempty"`
	LastUpdatedAt  string                  `json:"lastUpdatedAt,omitempty"`
	TTLMs          *int64                  `json:"ttlMs"`
	PollIntervalMs *int64                  `json:"pollIntervalMs,omitempty"`
	InputRequests  map[string]InputRequest `json:"inputRequests,omitempty"`
	Result         json.RawMessage         `json:"result,omitempty"`
	Error          *TaskError              `json:"error,omitempty"`
}

// DiscoverInfo keeps the essentials of a server/discover result for a
// modern connection.
type DiscoverInfo struct {
	ProtocolVersion string
	Capabilities    mcpgo.ServerCapabilities
	ServerInfo      mcpgo.Implementation
	Instructions    string
}

// HasExtension reports whether the server advertised the extension id.
func (d *DiscoverInfo) HasExtension(id string) bool {
	if d == nil {
		return false
	}
	return serverHasExtension(d.Capabilities, id)
}

// serverHasExtension reports whether caps.extensions contains id.
func serverHasExtension(caps mcpgo.ServerCapabilities, id string) bool {
	if caps.Extensions == nil {
		return false
	}
	_, ok := caps.Extensions[id]
	return ok
}

// ProtocolEra tells which protocol core a connection speaks.
type ProtocolEra int

const (
	// EraLegacy is the initialize handshake with sessions (2025-11-25 and earlier).
	EraLegacy ProtocolEra = iota
	// EraModern is the stateless core of 2026-07-28 and later.
	EraModern
)

func (e ProtocolEra) String() string {
	if e == EraModern {
		return "modern"
	}
	return "legacy"
}

// eraFromVersion maps a negotiated protocol version to its era.
func eraFromVersion(version string) ProtocolEra {
	if version != "" && mcpgo.IsModernProtocol(version) {
		return EraModern
	}
	return EraLegacy
}
