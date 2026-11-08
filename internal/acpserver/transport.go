package acpserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	ErrInputTooLarge       = errors.New("ACP input line exceeds limit")
	ErrOutputBackpressure  = errors.New("ACP output queue is full")
	ErrOutputFrameTooLarge = errors.New("ACP output frame exceeds limit")
	ErrTransportWrite      = errors.New("ACP output write failed")
	ErrTransportClosed     = errors.New("ACP transport is closed")
	ErrRequestBackpressure = errors.New("ACP request limit reached")
	// ErrOutboundRequestBackpressure means too many requests to the client
	// wait for a response. The transport stays open.
	ErrOutboundRequestBackpressure = errors.New("ACP outbound request limit reached")
)

type Handler struct {
	NewSession func(context.Context, NewSessionParams) (NewSessionResult, error)
	Prompt     func(context.Context, PromptParams) (PromptResult, error)
	Cancel     func(context.Context, CancelParams)
	// Initialized runs once after the initialize result is queued, with the
	// client capabilities. It runs on the read loop and must not block.
	Initialized func(context.Context, ClientCapabilities)
}

type Transport struct {
	input        io.Reader
	output       io.Writer
	handler      Handler
	agentVersion string

	started atomic.Bool
	stateMu sync.Mutex
	state   connectionState

	requestSlots chan struct{}
	requests     sync.WaitGroup

	// outbound holds requests sent to the client that wait for a response,
	// keyed by their "goclaw-<n>" id.
	outboundMu     sync.Mutex
	outboundNext   uint64
	outbound       map[string]outboundRequest
	outboundClosed bool

	sendMu    sync.RWMutex
	outputQ   chan []byte
	stopped   bool
	cancel    context.CancelFunc
	terminal  chan error
	writeDone chan error
}

type connectionState struct {
	initialized        bool
	clientCapabilities ClientCapabilities
	sessions           map[string]struct{}
	pendingSessions    int
}

type outboundResponse struct {
	result json.RawMessage
	rpcErr *RPCError
}

// outboundRequest is one request to the client that waits for a response.
// wake marks a _buzz/wake request, which has its own limit.
type outboundRequest struct {
	responses chan outboundResponse
	wake      bool
}

type outboundRequestEnvelope struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type requestEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

type responseEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

type notificationEnvelope struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

func NewTransport(input io.Reader, output io.Writer, handler Handler, agentVersion string) *Transport {
	return &Transport{
		input:        input,
		output:       output,
		handler:      handler,
		agentVersion: agentVersion,
		state: connectionState{
			sessions: make(map[string]struct{}),
		},
		requestSlots: make(chan struct{}, MaxPendingRequests),
		outbound:     make(map[string]outboundRequest),
	}
}

func (t *Transport) Serve(ctx context.Context) error {
	if !t.started.CompareAndSwap(false, true) {
		return errors.New("ACP transport already started")
	}
	if t.input == nil || t.output == nil {
		return errors.New("ACP transport requires input and output")
	}

	serveCtx, cancel := context.WithCancel(ctx)
	t.sendMu.Lock()
	t.cancel = cancel
	t.outputQ = make(chan []byte, MaxOutputQueue)
	t.terminal = make(chan error, 1)
	t.writeDone = make(chan error, 1)
	t.sendMu.Unlock()
	go t.writeLoop()
	readFinished := make(chan struct{})
	defer close(readFinished)
	if closer, ok := t.input.(io.Closer); ok {
		go func() {
			select {
			case <-serveCtx.Done():
				_ = closer.Close()
			case <-readFinished:
			}
		}()
	}

	reader := bufio.NewReaderSize(t.input, MaxInputLineBytes+1)
	var serveErr error
	for {
		if err := t.terminalError(); err != nil {
			serveErr = err
			break
		}
		if err := serveCtx.Err(); err != nil {
			serveErr = err
			break
		}

		line, readErr := reader.ReadSlice('\n')
		if serveCtx.Err() != nil {
			serveErr = serveCtx.Err()
			break
		}
		if errors.Is(readErr, bufio.ErrBufferFull) || len(line) > MaxInputLineBytes+1 || (len(line) == MaxInputLineBytes+1 && line[len(line)-1] != '\n') {
			serveErr = ErrInputTooLarge
			break
		}
		line = bytes.TrimSuffix(line, []byte{'\n'})
		if len(line) > MaxInputLineBytes {
			serveErr = ErrInputTooLarge
			break
		}
		if len(line) > 0 || readErr == nil {
			if err := t.dispatch(serveCtx, line); err != nil {
				serveErr = err
				break
			}
		}

		switch {
		case readErr == nil:
			continue
		case errors.Is(readErr, io.EOF):
			goto finished
		default:
			serveErr = errors.New("ACP input read failed")
			goto finished
		}
	}

finished:
	// No response can arrive after the read loop ends. Fail the waiting
	// requests first, so handlers that wait on them can return.
	t.closeOutbound()
	if serveErr != nil {
		cancel()
	}
	t.requests.Wait()
	cancel()
	t.stopOutput()
	if serveErr == nil {
		serveErr = t.terminalError()
	}
	if serveErr != nil {
		return serveErr
	}
	if err := <-t.writeDone; err != nil {
		return err
	}
	return nil
}

func (t *Transport) SessionUpdate(ctx context.Context, params SessionUpdateParams) error {
	if params.SessionID == "" || params.Update.SessionUpdate == "" || params.Update.Content.Type != "text" || len(params.Update.Content.Text) > MaxOutputTextChunkBytes {
		return invalidParams()
	}
	return t.sendFrame(ctx, notificationEnvelope{
		JSONRPC: "2.0",
		Method:  "session/update",
		Params:  params,
	})
}

func (t *Transport) SessionRawUpdate(ctx context.Context, sessionID string, update map[string]any) error {
	if sessionID == "" || update["sessionUpdate"] == nil {
		return invalidParams()
	}
	return t.sendFrame(ctx, notificationEnvelope{
		JSONRPC: "2.0",
		Method:  "session/update",
		Params:  map[string]any{"sessionId": sessionID, "update": update},
	})
}

// Request sends a JSON-RPC request to the client and waits for its response
// or for the end of ctx. It works inside and outside a prompt. A JSON-RPC
// error from the client is returned as *RPCError with a nil error. A nil
// params value is not sent.
func (t *Transport) Request(ctx context.Context, method string, params any) (json.RawMessage, *RPCError, error) {
	if method == "" {
		return nil, nil, errors.New("ACP request method is required")
	}
	id, responses, err := t.registerOutbound(method == MethodBuzzWake)
	if err != nil {
		return nil, nil, err
	}
	defer t.unregisterOutbound(id)
	if err := t.sendFrame(ctx, outboundRequestEnvelope{JSONRPC: "2.0", ID: id, Method: method, Params: params}); err != nil {
		return nil, nil, err
	}
	select {
	case response, ok := <-responses:
		if !ok {
			return nil, nil, ErrTransportClosed
		}
		return response.result, response.rpcErr, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// ClientCapabilities returns the capabilities from the client initialize
// request, or the zero value before initialize.
func (t *Transport) ClientCapabilities() ClientCapabilities {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	return t.state.clientCapabilities
}

// registerOutbound adds a waiting request. Wakes and other requests are
// counted against separate limits.
func (t *Transport) registerOutbound(wake bool) (string, chan outboundResponse, error) {
	t.outboundMu.Lock()
	defer t.outboundMu.Unlock()
	if t.outboundClosed {
		return "", nil, ErrTransportClosed
	}
	limit := MaxPendingOutboundRequests
	if wake {
		limit = MaxPendingOutboundWakes
	}
	same := 0
	for _, pending := range t.outbound {
		if pending.wake == wake {
			same++
		}
	}
	if same >= limit {
		return "", nil, ErrOutboundRequestBackpressure
	}
	t.outboundNext++
	id := "goclaw-" + strconv.FormatUint(t.outboundNext, 10)
	// One buffered slot: the read loop never blocks on delivery.
	responses := make(chan outboundResponse, 1)
	t.outbound[id] = outboundRequest{responses: responses, wake: wake}
	return id, responses, nil
}

func (t *Transport) unregisterOutbound(id string) {
	t.outboundMu.Lock()
	delete(t.outbound, id)
	t.outboundMu.Unlock()
}

func (t *Transport) closeOutbound() {
	t.outboundMu.Lock()
	defer t.outboundMu.Unlock()
	t.outboundClosed = true
	for id, pending := range t.outbound {
		close(pending.responses)
		delete(t.outbound, id)
	}
}

// isResponseFrame reports whether a frame is a JSON-RPC response: version
// 2.0, a valid id, no method, and exactly one of result and error.
func isResponseFrame(fields map[string]json.RawMessage, msg requestEnvelope, hasID, validID bool) bool {
	_, hasMethod := fields["method"]
	_, hasResult := fields["result"]
	_, hasError := fields["error"]
	return msg.JSONRPC == "2.0" && hasID && validID && !hasMethod && hasResult != hasError
}

// deliverResponse gives a client response to the waiting request with the
// same id. A response with an unknown id, for example one that arrives after
// its request was cancelled, is dropped without an error frame.
func (t *Transport) deliverResponse(fields map[string]json.RawMessage) {
	var id string
	if json.Unmarshal(fields["id"], &id) != nil {
		return
	}
	t.outboundMu.Lock()
	pending, ok := t.outbound[id]
	delete(t.outbound, id)
	t.outboundMu.Unlock()
	if !ok {
		return
	}
	var response outboundResponse
	if raw, hasResult := fields["result"]; hasResult {
		response.result = raw
	} else {
		var wire struct {
			Code    *int   `json:"code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(fields["error"], &wire) != nil || wire.Code == nil {
			// The caller gets an error instead of a wait until its deadline.
			response.rpcErr = &RPCError{Code: CodeInternalError, Message: "Malformed error response"}
		} else {
			response.rpcErr = &RPCError{Code: *wire.Code, Message: wire.Message}
		}
	}
	pending.responses <- response
}

func (t *Transport) dispatch(ctx context.Context, line []byte) error {
	var msg requestEnvelope
	var fields map[string]json.RawMessage
	if !json.Valid(line) {
		return t.sendError(ctx, json.RawMessage("null"), &RPCError{Code: CodeParseError, Message: "Parse error"})
	}
	if err := json.Unmarshal(line, &fields); err != nil || fields == nil {
		return t.sendError(ctx, json.RawMessage("null"), &RPCError{Code: CodeInvalidRequest, Message: "Invalid Request"})
	}
	if err := json.Unmarshal(line, &msg); err != nil {
		if _, notification := fields["id"]; !notification {
			return nil
		}
		return t.sendError(ctx, json.RawMessage("null"), &RPCError{Code: CodeInvalidRequest, Message: "Invalid Request"})
	}

	id, hasID, validID := parseRequestID(fields)
	if isResponseFrame(fields, msg, hasID, validID) {
		t.deliverResponse(fields)
		return nil
	}
	if msg.JSONRPC != "2.0" || msg.Method == "" || len(msg.Result) != 0 || len(msg.Error) != 0 || !validID {
		if !hasID {
			return nil
		}
		return t.sendError(ctx, json.RawMessage("null"), &RPCError{Code: CodeInvalidRequest, Message: "Invalid Request"})
	}

	if !hasID {
		return t.dispatchNotification(ctx, msg)
	}

	if msg.Method == "initialize" {
		return t.initialize(ctx, id, msg.Params)
	}
	if !t.isInitialized() {
		return t.sendError(ctx, id, &RPCError{Code: CodeInvalidRequest, Message: "Connection not initialized"})
	}

	switch msg.Method {
	case "session/new":
		params, rpcErr := DecodeNewSessionParams(msg.Params)
		if rpcErr != nil {
			return t.sendError(ctx, id, rpcErr)
		}
		if !t.reserveSession() {
			return t.sendError(ctx, id, &RPCError{Code: CodeInvalidRequest, Message: "Session limit reached"})
		}
		return t.startRequest(ctx, id, func(requestCtx context.Context) (any, error) {
			defer t.releaseSessionReservation()
			if t.handler.NewSession == nil {
				return nil, errors.New("session handler unavailable")
			}
			result, err := t.handler.NewSession(requestCtx, params)
			if err != nil {
				return nil, err
			}
			if result.SessionID == "" || !t.addSession(result.SessionID) {
				return nil, errors.New("invalid session result")
			}
			return result, nil
		})
	case "session/prompt":
		params, rpcErr := DecodePromptParams(msg.Params)
		if rpcErr != nil {
			return t.sendError(ctx, id, rpcErr)
		}
		if !t.hasSession(params.SessionID) {
			return t.sendError(ctx, id, &RPCError{Code: CodeUnknownSession, Message: "Unknown session"})
		}
		return t.startRequest(ctx, id, func(requestCtx context.Context) (any, error) {
			if t.handler.Prompt == nil {
				return nil, errors.New("prompt handler unavailable")
			}
			return t.handler.Prompt(requestCtx, params)
		})
	default:
		return t.sendError(ctx, id, &RPCError{Code: CodeMethodNotFound, Message: "Method not found"})
	}
}

func (t *Transport) dispatchNotification(ctx context.Context, msg requestEnvelope) error {
	if msg.Method != "session/cancel" || !t.isInitialized() {
		return nil
	}
	params, rpcErr := DecodeCancelParams(msg.Params)
	if rpcErr != nil || !t.hasSession(params.SessionID) || t.handler.Cancel == nil {
		return nil
	}
	select {
	case t.requestSlots <- struct{}{}:
	default:
		t.failTransport(ErrRequestBackpressure)
		return ErrRequestBackpressure
	}
	t.requests.Add(1)
	go func() {
		defer t.requests.Done()
		defer func() { <-t.requestSlots }()
		t.handler.Cancel(ctx, params)
	}()
	return nil
}

func (t *Transport) initialize(ctx context.Context, id, raw json.RawMessage) error {
	params, rpcErr := DecodeInitializeParams(raw)
	if rpcErr != nil {
		return t.sendError(ctx, id, rpcErr)
	}

	t.stateMu.Lock()
	if t.state.initialized {
		t.stateMu.Unlock()
		return t.sendError(ctx, id, &RPCError{Code: CodeInvalidRequest, Message: "Connection already initialized"})
	}
	t.state.initialized = true
	caps := ParseClientCapabilities(params.ClientCapabilities)
	t.state.clientCapabilities = caps
	t.stateMu.Unlock()

	err := t.sendResult(ctx, id, InitializeResult{
		ProtocolVersion: ProtocolVersion,
		AgentCapabilities: AgentCapabilities{
			LoadSession:        false,
			PromptCapabilities: PromptCapabilities{},
			MCPCapabilities:    MCPCapabilities{},
			Meta:               map[string]any{WakeCapabilityKey: map[string]int{"version": WakeCapabilityVersion}},
		},
		AgentInfo:   AgentInfo{Name: "goclaw", Title: "GoClaw", Version: t.agentVersion},
		AuthMethods: []any{},
	})
	if err == nil && t.handler.Initialized != nil {
		t.handler.Initialized(ctx, caps)
	}
	return err
}

func (t *Transport) startRequest(ctx context.Context, id json.RawMessage, fn func(context.Context) (any, error)) error {
	select {
	case t.requestSlots <- struct{}{}:
	default:
		return t.sendError(ctx, id, &RPCError{Code: CodeInternalError, Message: "Server busy"})
	}

	t.requests.Add(1)
	go func() {
		defer t.requests.Done()
		defer func() { <-t.requestSlots }()
		result, err := fn(ctx)
		if err != nil {
			_ = t.sendError(ctx, id, publicError(err))
			return
		}
		_ = t.sendResult(ctx, id, result)
	}()
	return nil
}

func (t *Transport) sendResult(ctx context.Context, id json.RawMessage, result any) error {
	return t.sendFrame(ctx, responseEnvelope{JSONRPC: "2.0", ID: id, Result: result})
}

func (t *Transport) sendError(ctx context.Context, id json.RawMessage, rpcErr *RPCError) error {
	return t.sendFrame(ctx, responseEnvelope{JSONRPC: "2.0", ID: id, Error: rpcErr})
}

func (t *Transport) sendFrame(ctx context.Context, frame any) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return errors.New("ACP frame serialization failed")
	}
	if len(data) > MaxOutputFrameBytes {
		return ErrOutputFrameTooLarge
	}
	data = append(data, '\n')

	t.sendMu.RLock()
	defer t.sendMu.RUnlock()
	if t.stopped || t.outputQ == nil {
		return ErrTransportClosed
	}
	select {
	case t.outputQ <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		t.failTransport(ErrOutputBackpressure)
		return ErrOutputBackpressure
	}
}

func (t *Transport) writeLoop() {
	for frame := range t.outputQ {
		if err := writeFull(t.output, frame); err != nil {
			terminal := fmt.Errorf("%w: %v", ErrTransportWrite, err)
			t.failTransport(terminal)
			t.writeDone <- terminal
			return
		}
	}
	t.writeDone <- nil
}

func (t *Transport) failTransport(err error) {
	select {
	case t.terminal <- err:
		if t.cancel != nil {
			t.cancel()
		}
	default:
	}
}

func (t *Transport) terminalError() error {
	if t.terminal == nil {
		return nil
	}
	select {
	case err := <-t.terminal:
		return err
	default:
		return nil
	}
}

func (t *Transport) stopOutput() {
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	if !t.stopped {
		t.stopped = true
		close(t.outputQ)
	}
}

func (t *Transport) isInitialized() bool {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	return t.state.initialized
}

func (t *Transport) reserveSession() bool {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	if len(t.state.sessions)+t.state.pendingSessions >= MaxActiveSessions {
		return false
	}
	t.state.pendingSessions++
	return true
}

func (t *Transport) releaseSessionReservation() {
	t.stateMu.Lock()
	t.state.pendingSessions--
	t.stateMu.Unlock()
}

func (t *Transport) addSession(sessionID string) bool {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	if _, exists := t.state.sessions[sessionID]; exists {
		return false
	}
	t.state.sessions[sessionID] = struct{}{}
	return true
}

func (t *Transport) hasSession(sessionID string) bool {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	_, ok := t.state.sessions[sessionID]
	return ok
}

func parseRequestID(fields map[string]json.RawMessage) (json.RawMessage, bool, bool) {
	raw, ok := fields["id"]
	if !ok {
		return nil, false, true
	}
	if bytes.Equal(raw, []byte("null")) {
		return json.RawMessage("null"), true, true
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return bytes.Clone(raw), true, true
	}
	var integer int64
	if json.Unmarshal(raw, &integer) == nil {
		return bytes.Clone(raw), true, true
	}
	return nil, true, false
}

func publicError(err error) *RPCError {
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr
	}
	return &RPCError{Code: CodeInternalError, Message: "Internal error"}
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
