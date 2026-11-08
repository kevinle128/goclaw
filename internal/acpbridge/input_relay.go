package acpbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// Limits of the in-turn input relay.
const (
	// MaxInputMessageBytes bounds the question text of one input request.
	MaxInputMessageBytes = 8 << 10
	// MaxInputSchemaBytes bounds the requested schema of one input request.
	MaxInputSchemaBytes = 64 << 10
	// MaxInputContentBytes bounds the answer content of one input request.
	MaxInputContentBytes = 64 << 10
	// MaxPendingInputsPerConnection bounds the open input requests of one
	// adapter connection.
	MaxPendingInputsPerConnection = 16
	// MaxPendingInputs bounds the open input requests of the gateway.
	MaxPendingInputs = 1024
)

// Answer actions of an input request (the ACP elicitation result).
const (
	InputActionAccept  = "accept"
	InputActionDecline = "decline"
	InputActionCancel  = "cancel"
)

var (
	// ErrInputUnsupported means the ACP client did not declare form
	// elicitation.
	ErrInputUnsupported = errors.New("ACP client cannot show forms")
	// ErrInputInvalid means an input request or answer has a bad shape.
	ErrInputInvalid = errors.New("invalid ACP input request or answer")
	// ErrInputClosed means the adapter connection closed before the answer.
	ErrInputClosed = errors.New("ACP connection closed before the answer")
)

// InputBinding binds an input relay to the ACP run that owns it.
type InputBinding struct {
	ConnectionID string
	SessionKey   string
	Generation   uint64
	Sender       CriticalEventSender
	// CanAskForm is true only when the ACP client declared form elicitation.
	CanAskForm bool
}

// InputRelays holds the open input requests of all ACP runs. An answer is
// accepted only from the connection, session key and generation of the run
// that asked.
type InputRelays struct {
	mu      sync.Mutex
	pending map[string]*pendingInput
}

type pendingInput struct {
	connectionID string
	sessionKey   string
	generation   uint64
	// result has one slot; exactly one value is sent before the entry is
	// removed from the map.
	result chan inputResult
}

type inputResult struct {
	action  string
	content json.RawMessage
	err     error
}

// NewInputRelays creates an empty registry.
func NewInputRelays() *InputRelays {
	return &InputRelays{pending: make(map[string]*pendingInput)}
}

// Bind returns the relay of one ACP run.
func (r *InputRelays) Bind(binding InputBinding) *InputRelay {
	return &InputRelay{relays: r, binding: binding}
}

// Respond gives the answer to the waiting request. It returns ErrNotFound
// for an unknown or already answered request, ErrNotOwner when the answer
// comes from another connection, session key or generation, and
// ErrInputInvalid for a bad action or content. A bad answer from the owner
// ends the request with "cancel", so the waiter does not wait for an answer
// that does not come.
func (r *InputRelays) Respond(connectionID, sessionKey string, generation uint64, requestID, action string, content json.RawMessage) error {
	r.mu.Lock()
	p, ok := r.pending[requestID]
	if !ok {
		r.mu.Unlock()
		return ErrNotFound
	}
	if p.connectionID != connectionID || p.sessionKey != sessionKey || p.generation != generation {
		r.mu.Unlock()
		return ErrNotOwner
	}
	delete(r.pending, requestID)
	r.mu.Unlock()
	answer, err := normalizeInputAnswer(action, content)
	if err != nil {
		p.result <- inputResult{action: InputActionCancel}
		return err
	}
	p.result <- answer
	return nil
}

// ReleaseConnection ends the open requests of a closed connection.
func (r *InputRelays) ReleaseConnection(connectionID string) {
	r.mu.Lock()
	var ended []*pendingInput
	for id, p := range r.pending {
		if p.connectionID == connectionID {
			ended = append(ended, p)
			delete(r.pending, id)
		}
	}
	r.mu.Unlock()
	for _, p := range ended {
		p.result <- inputResult{err: ErrInputClosed}
	}
}

// Pending returns the number of open requests.
func (r *InputRelays) Pending() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

func (r *InputRelays) register(id string, p *pendingInput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) >= MaxPendingInputs {
		return ErrQuota
	}
	perConnection := 0
	for _, other := range r.pending {
		if other.connectionID == p.connectionID {
			perConnection++
		}
	}
	if perConnection >= MaxPendingInputsPerConnection {
		return ErrQuota
	}
	r.pending[id] = p
	return nil
}

func (r *InputRelays) remove(id string, p *pendingInput) {
	r.mu.Lock()
	if r.pending[id] == p {
		delete(r.pending, id)
	}
	r.mu.Unlock()
}

// InputRelay asks the people of one ACP run to answer a form. It implements
// the InputRelay interface of the MCP task tool.
type InputRelay struct {
	relays  *InputRelays
	binding InputBinding
}

// CanAskForm reports whether the ACP client declared form elicitation.
func (r *InputRelay) CanAskForm() bool {
	return r != nil && r.binding.CanAskForm && r.binding.Sender != nil
}

// AskForm sends acp.input.request to the owner connection and waits for
// acp.input.respond or the end of ctx.
func (r *InputRelay) AskForm(ctx context.Context, toolCallID, message string, schema json.RawMessage) (string, json.RawMessage, error) {
	if !r.CanAskForm() {
		return "", nil, ErrInputUnsupported
	}
	if len(message) > MaxInputMessageBytes || !isJSONObject(schema) || len(schema) > MaxInputSchemaBytes {
		return "", nil, ErrInputInvalid
	}
	id := uuid.NewString()
	p := &pendingInput{
		connectionID: r.binding.ConnectionID,
		sessionKey:   r.binding.SessionKey,
		generation:   r.binding.Generation,
		result:       make(chan inputResult, 1),
	}
	if err := r.relays.register(id, p); err != nil {
		return "", nil, err
	}
	defer r.relays.remove(id, p)

	payload := map[string]any{
		"sessionKey":      r.binding.SessionKey,
		"acpGeneration":   r.binding.Generation,
		"requestId":       id,
		"toolCallId":      toolCallID,
		"mode":            "form",
		"message":         message,
		"requestedSchema": schema,
	}
	if err := r.binding.Sender.SendCriticalEvent(*protocol.NewEvent(protocol.EventACPInputRequest, payload)); err != nil {
		return "", nil, fmt.Errorf("send ACP input request: %w", err)
	}
	select {
	case result := <-p.result:
		return result.action, result.content, result.err
	case <-ctx.Done():
		return "", nil, ctx.Err()
	}
}

// normalizeInputAnswer checks an answer. Content is kept only for accept,
// and then it must be a JSON object.
func normalizeInputAnswer(action string, content json.RawMessage) (inputResult, error) {
	switch action {
	case InputActionAccept:
		if !isJSONObject(content) || len(content) > MaxInputContentBytes {
			return inputResult{}, ErrInputInvalid
		}
		return inputResult{action: action, content: bytes.Clone(content)}, nil
	case InputActionDecline, InputActionCancel:
		return inputResult{action: action}, nil
	default:
		return inputResult{}, ErrInputInvalid
	}
}

func isJSONObject(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(trimmed, &object) == nil
}
