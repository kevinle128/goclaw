// Package mcptest provides a stub MCP server over Streamable HTTP for tests.
//
// The stub speaks protocol revision 2026-07-28 (server/discover, per-request
// _meta, Mcp-Method and Mcp-Name headers, the tasks extension, and the events
// extension), the legacy revision 2025-11-25 (initialize handshake with
// Mcp-Session-Id), or both. It records every request with its headers and
// _meta so that a test can check what the client sent.
//
// In modern mode the stub is strict: it answers 400 with -32020 when the
// MCP-Protocol-Version header, the Mcp-Method header, or the Mcp-Name header
// does not agree with the body, -32022 for an unsupported version, -32021
// for tasks/* without the tasks extension, and 404 with -32601 for an
// unknown method.
//
// Typical use:
//
//	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
//	srv.AddTool("echo", "Echo text", func(ctx context.Context, c mcptest.ToolCall) mcptest.Reply {
//		return mcptest.TextReply("hello")
//	})
//	srv.AddTaskTool("run_workflow", "Start a run")
//	// connect a client to srv.URL, then check srv.RequestsFor("tools/call")
package mcptest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// Protocol versions and extension identifiers.
const (
	ProtocolModern  = "2026-07-28"
	ProtocolLegacy  = "2025-11-25"
	ExtensionTasks  = "io.modelcontextprotocol/tasks"
	ExtensionEvents = "io.modelcontextprotocol/events"

	metaSubscriptionID = "io.modelcontextprotocol/subscriptionId"
)

// Mode selects which protocol era the stub speaks.
type Mode int

const (
	// ModeModern speaks only 2026-07-28. initialize gets 404 + -32601.
	ModeModern Mode = iota
	// ModeLegacy speaks only 2025-11-25 and rejects server/discover.
	ModeLegacy
	// ModeDual speaks both: server/discover and initialize.
	ModeDual
)

// DiscoverRejection selects how a legacy-mode stub answers server/discover.
type DiscoverRejection int

const (
	// RejectMethodNotFound answers HTTP 200 with JSON-RPC -32601, as a
	// legacy mcp-go server does.
	RejectMethodNotFound DiscoverRejection = iota
	// RejectHTTP404 answers HTTP 404 with an empty body.
	RejectHTTP404
	// RejectHTTP400Empty answers HTTP 400 with an empty body.
	RejectHTTP400Empty
)

// EventDescriptor is one entry of the events/list result.
type EventDescriptor struct {
	Name          string         `json:"name"`
	Description   string         `json:"description,omitempty"`
	Delivery      []string       `json:"delivery"`
	InputSchema   map[string]any `json:"inputSchema,omitempty"`
	PayloadSchema map[string]any `json:"payloadSchema,omitempty"`
}

// Options configures a stub server.
type Options struct {
	Mode Mode
	// Extensions are advertised in server/discover capabilities.extensions.
	// Nil advertises tasks and events. A non-nil empty map advertises none.
	Extensions map[string]any
	// LegacyDiscover selects the server/discover answer in ModeLegacy.
	LegacyDiscover DiscoverRejection
	// RequireBearer, when set, answers 401 to a request without
	// "Authorization: Bearer <RequireBearer>".
	RequireBearer string
	// Events is the events/list result.
	Events []EventDescriptor
}

// Request is one recorded HTTP request.
type Request struct {
	Method    string
	ID        json.RawMessage // nil for notifications
	Header    http.Header
	Params    json.RawMessage
	Meta      map[string]json.RawMessage
	SessionID string
	At        time.Time
}

// IDString returns the JSON-RPC id as text: a string id without quotes, a
// number in decimal.
func (r Request) IDString() string {
	var s string
	if json.Unmarshal(r.ID, &s) == nil {
		return s
	}
	return string(r.ID)
}

// ProtocolVersion returns _meta["io.modelcontextprotocol/protocolVersion"].
func (r Request) ProtocolVersion() string {
	var v string
	_ = json.Unmarshal(r.Meta[mcpgo.MetaKeyProtocolVersion], &v)
	return v
}

// ClientCapabilities returns _meta["io.modelcontextprotocol/clientCapabilities"].
func (r Request) ClientCapabilities() map[string]any {
	var caps map[string]any
	_ = json.Unmarshal(r.Meta[mcpgo.MetaKeyClientCapabilities], &caps)
	return caps
}

// HasExtension reports whether the request declared the extension id in
// _meta clientCapabilities.extensions.
func (r Request) HasExtension(id string) bool {
	ext, _ := r.ClientCapabilities()["extensions"].(map[string]any)
	_, ok := ext[id]
	return ok
}

// Param decodes one top-level field of params into dst.
func (r Request) Param(name string, dst any) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(r.Params, &fields); err != nil {
		return err
	}
	raw, ok := fields[name]
	if !ok {
		return fmt.Errorf("param %q is absent", name)
	}
	return json.Unmarshal(raw, dst)
}

// Reply is the answer of a handler: a result, a JSON-RPC error, or a raw
// HTTP response. Build it with TextReply, ResultReply, ErrorReply or
// HTTPReply.
type Reply struct {
	result any
	err    *rpcError
	status int
	body   string
	raw    bool
}

// TextReply is a CallToolResult with one text block and no resultType, as an
// older server sends it.
func TextReply(text string) Reply {
	return ResultReply(map[string]any{
		"content": []any{map[string]any{"type": "text", "text": text}},
	})
}

// ResultReply answers with result as the JSON-RPC result.
func ResultReply(result any) Reply { return Reply{result: result} }

// ErrorReply answers with a JSON-RPC error at HTTP 200.
func ErrorReply(code int, message string, data any) Reply {
	return Reply{err: &rpcError{Code: code, Message: message, Data: data}, status: http.StatusOK}
}

// HTTPReply answers with a raw HTTP status and body (the body may be empty).
func HTTPReply(status int, body string) Reply {
	return Reply{status: status, body: body, raw: true}
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// ToolCall is the decoded tools/call request.
type ToolCall struct {
	Name          string
	Arguments     map[string]any
	Request       Request
	TasksDeclared bool
}

// ToolHandler answers one tools/call.
type ToolHandler func(ctx context.Context, call ToolCall) Reply

// MethodHandler answers one request. It overrides the built-in handler of
// the method; use it to script hangs, HTTP errors, or custom results.
type MethodHandler func(ctx context.Context, req Request) Reply

// Task is the stub's record of one task. Its fields follow the DetailedTask
// wire shape of the tasks extension.
type Task struct {
	TaskID         string                  `json:"taskId"`
	Status         string                  `json:"status"`
	StatusMessage  string                  `json:"statusMessage,omitempty"`
	CreatedAt      string                  `json:"createdAt"`
	LastUpdatedAt  string                  `json:"lastUpdatedAt"`
	TTLMs          *int64                  `json:"ttlMs"`
	PollIntervalMs *int64                  `json:"pollIntervalMs,omitempty"`
	InputRequests  map[string]InputRequest `json:"inputRequests,omitempty"`
	Result         any                     `json:"result,omitempty"`
	Error          *TaskError              `json:"error,omitempty"`
}

// InputRequest is one server request inside a task.
type InputRequest struct {
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

// TaskError is the JSON-RPC error object of a failed task.
type TaskError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// EventOccurrence is one event in events/poll and notifications/events/event.
type EventOccurrence struct {
	EventID   string  `json:"eventId"`
	Name      string  `json:"name"`
	Timestamp string  `json:"timestamp"`
	Data      any     `json:"data"`
	Cursor    *string `json:"cursor,omitempty"`
}

// PollReply is one scripted events/poll result.
type PollReply struct {
	Events     []EventOccurrence
	Cursor     *string
	Truncated  bool
	HasMore    bool
	NextPollMs int
}

// StreamScript writes the frames of one events/stream request. It runs in the
// HTTP handler; when it returns, the stub ends the response. ctx ends when the
// client closes the stream or the stub closes.
type StreamScript func(ctx context.Context, s *Stream, req Request)

// Stream writes notifications/events/* frames on one SSE response. Every frame
// carries _meta subscriptionId = the request id.
type Stream struct {
	w        http.ResponseWriter
	flusher  http.Flusher
	id       json.RawMessage
	mu       sync.Mutex
	finished bool
}

// Server is a running stub MCP server.
type Server struct {
	// URL is the MCP endpoint (Streamable HTTP).
	URL string

	opts    Options
	httpSrv *httptest.Server
	stop    chan struct{}
	once    sync.Once

	mu           sync.Mutex
	requests     []Request
	tools        []toolEntry
	handlers     map[string]MethodHandler
	sessions     map[string]bool
	sessionSeq   int
	tasks        map[string]*Task
	taskSeq      int
	taskUpdates  map[string][]map[string]json.RawMessage
	onTaskUpdate func(id string, responses map[string]json.RawMessage)
	polls        map[string][]PollReply
	streamScript StreamScript
}

type toolEntry struct {
	name        string
	description string
	handler     ToolHandler
}

// New starts a stub server and closes it when the test ends.
func New(t testing.TB, opts Options) *Server {
	t.Helper()
	s := &Server{
		opts:        opts,
		stop:        make(chan struct{}),
		handlers:    make(map[string]MethodHandler),
		sessions:    make(map[string]bool),
		tasks:       make(map[string]*Task),
		taskUpdates: make(map[string][]map[string]json.RawMessage),
		polls:       make(map[string][]PollReply),
	}
	s.httpSrv = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	s.URL = s.httpSrv.URL + "/mcp"
	t.Cleanup(s.Close)
	return s
}

// Close stops the server. Open streams end. It is safe to call more than once.
func (s *Server) Close() {
	s.once.Do(func() {
		close(s.stop)
		s.httpSrv.CloseClientConnections()
		s.httpSrv.Close()
	})
}

// AddTool registers a tool for tools/list and tools/call.
func (s *Server) AddTool(name, description string, handler ToolHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tools = append(s.tools, toolEntry{name: name, description: description, handler: handler})
}

// AddTaskTool registers a tool that starts a task when the request declared
// the tasks extension, and returns a plain text result with the new id when
// it did not (a server must not return a task to such a request).
func (s *Server) AddTaskTool(name, description string) {
	s.AddTool(name, description, func(_ context.Context, call ToolCall) Reply {
		task := s.CreateTask("")
		if !call.TasksDeclared {
			return TextReply("run_id: " + task.TaskID)
		}
		return s.CreateTaskReply(task.TaskID)
	})
}

// Handle overrides the built-in handler of method.
func (s *Server) Handle(method string, h MethodHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = h
}

// ForgetSessions drops all legacy sessions, as a server restart does. The next
// request with an old Mcp-Session-Id gets HTTP 404.
func (s *Server) ForgetSessions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = make(map[string]bool)
}

// Requests returns a copy of all recorded requests in arrival order.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests)
}

// RequestsFor returns the recorded requests of one method.
func (s *Server) RequestsFor(method string) []Request {
	var out []Request
	for _, r := range s.Requests() {
		if r.Method == method {
			out = append(out, r)
		}
	}
	return out
}

// Methods returns the method of every recorded request in arrival order.
func (s *Server) Methods() []string {
	reqs := s.Requests()
	out := make([]string, len(reqs))
	for i, r := range reqs {
		out[i] = r.Method
	}
	return out
}

// CreateTask creates a task in status "working" with a TTL of 30 days and a
// poll interval of 5 s. An empty id gets the id "task-<n>".
func (s *Server) CreateTask(id string) Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.taskSeq++
	if id == "" {
		id = "task-" + strconv.Itoa(s.taskSeq)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	ttl := int64(30 * 24 * time.Hour / time.Millisecond)
	poll := int64(5000)
	task := &Task{TaskID: id, Status: "working", CreatedAt: now, LastUpdatedAt: now, TTLMs: &ttl, PollIntervalMs: &poll}
	s.tasks[id] = task
	return *task
}

// CreateTaskReply answers a tools/call with the flat CreateTaskResult of an
// existing task.
func (s *Server) CreateTaskReply(taskID string) Reply {
	task, ok := s.Task(taskID)
	if !ok {
		return ErrorReply(-32603, "unknown task "+taskID, nil)
	}
	return ResultReply(map[string]any{
		"resultType":     "task",
		"taskId":         task.TaskID,
		"status":         task.Status,
		"statusMessage":  task.StatusMessage,
		"createdAt":      task.CreatedAt,
		"lastUpdatedAt":  task.LastUpdatedAt,
		"ttlMs":          task.TTLMs,
		"pollIntervalMs": task.PollIntervalMs,
	})
}

// Task returns a copy of a task.
func (s *Server) Task(id string) (Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, false
	}
	return *t, true
}

// UpdateTask changes a task under the stub lock and sets lastUpdatedAt. It
// returns false for an unknown id. Use it to script the task state machine.
func (s *Server) UpdateTask(id string, fn func(*Task)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return false
	}
	fn(t)
	t.LastUpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return true
}

// RequireTaskInput moves a task to input_required with one input request.
func (s *Server) RequireTaskInput(id, key string, req InputRequest) bool {
	return s.UpdateTask(id, func(t *Task) {
		t.Status = "input_required"
		if t.InputRequests == nil {
			t.InputRequests = map[string]InputRequest{}
		}
		t.InputRequests[key] = req
	})
}

// CompleteTask moves a task to completed with result (a CallToolResult).
func (s *Server) CompleteTask(id string, result any) bool {
	return s.UpdateTask(id, func(t *Task) {
		t.Status = "completed"
		t.InputRequests = nil
		t.Result = result
	})
}

// FailTask moves a task to failed with a JSON-RPC error.
func (s *Server) FailTask(id string, code int, message string) bool {
	return s.UpdateTask(id, func(t *Task) {
		t.Status = "failed"
		t.InputRequests = nil
		t.Error = &TaskError{Code: code, Message: message}
	})
}

// TaskUpdates returns the inputResponses of every tasks/update for a task.
func (s *Server) TaskUpdates(id string) []map[string]json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.taskUpdates[id])
}

// OnTaskUpdate sets a hook that runs after each accepted tasks/update.
func (s *Server) OnTaskUpdate(fn func(id string, responses map[string]json.RawMessage)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onTaskUpdate = fn
}

// QueuePoll adds a scripted events/poll result for an event name. Results are
// used in order; when the queue is empty, poll returns no events and the
// cursor "cursor-0".
func (s *Server) QueuePoll(name string, reply PollReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.polls[name] = append(s.polls[name], reply)
}

// SetStreamScript sets the script of events/stream. The default script sends
// one notifications/events/active with cursor "cursor-0" and then waits until
// the client closes the stream.
func (s *Server) SetStreamScript(script StreamScript) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streamScript = script
}

// Active sends notifications/events/active.
func (st *Stream) Active(cursor *string, truncated bool) error {
	return st.notify("notifications/events/active", map[string]any{"cursor": cursor, "truncated": truncated})
}

// Event sends notifications/events/event.
func (st *Stream) Event(ev EventOccurrence) error {
	return st.notify("notifications/events/event", map[string]any{
		"eventId": ev.EventID, "name": ev.Name, "timestamp": ev.Timestamp, "data": ev.Data, "cursor": ev.Cursor,
	})
}

// Heartbeat sends notifications/events/heartbeat.
func (st *Stream) Heartbeat(cursor *string) error {
	return st.notify("notifications/events/heartbeat", map[string]any{"cursor": cursor})
}

// Error sends notifications/events/error (transient; the stream stays open).
func (st *Stream) Error(code int, message string, data any) error {
	return st.notify("notifications/events/error", map[string]any{"error": rpcError{Code: code, Message: message, Data: data}})
}

// Terminated sends notifications/events/terminated (the subscription ended).
func (st *Stream) Terminated(code int, message string, data any) error {
	return st.notify("notifications/events/terminated", map[string]any{"error": rpcError{Code: code, Message: message, Data: data}})
}

// Complete sends the final JSON-RPC response of the stream request. A client
// must not depend on it; a script can also return without it.
func (st *Stream) Complete() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.finished {
		return nil
	}
	st.finished = true
	return st.write(map[string]any{
		"jsonrpc": "2.0", "id": st.id,
		"result": map[string]any{"resultType": "complete", "_meta": map[string]any{metaSubscriptionID: st.id}},
	})
}

func (st *Stream) notify(method string, params map[string]any) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	params["_meta"] = map[string]any{metaSubscriptionID: st.id}
	return st.write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func (st *Stream) write(msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(st.w, "event: message\ndata: %s\n\n", data); err != nil {
		return err
	}
	st.flusher.Flush()
	return nil
}

// ---- HTTP handling ----

type wireRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.opts.RequireBearer != "" && r.Header.Get("Authorization") != "Bearer "+s.opts.RequireBearer {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var wr wireRequest
	if err := json.Unmarshal(body, &wr); err != nil || wr.Method == "" {
		writeJSON(w, http.StatusBadRequest, errorBody(nil, -32700, "parse error", nil))
		return
	}

	req := Request{
		Method:    wr.Method,
		ID:        wr.ID,
		Header:    r.Header.Clone(),
		Params:    wr.Params,
		SessionID: r.Header.Get(mcpgo.HeaderSessionID),
		At:        time.Now(),
	}
	if len(wr.Params) > 0 {
		var fields map[string]json.RawMessage
		if json.Unmarshal(wr.Params, &fields) == nil {
			_ = json.Unmarshal(fields["_meta"], &req.Meta)
		}
	}
	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.mu.Unlock()

	isNotification := len(wr.ID) == 0 || string(wr.ID) == "null"
	modernRequest := req.ProtocolVersion() != ""

	switch {
	case wr.Method == "initialize":
		if s.opts.Mode == ModeModern {
			writeJSON(w, http.StatusNotFound, errorBody(wr.ID, -32601, "method not found: initialize", nil))
			return
		}
		s.handleInitialize(w, wr)
		return
	case s.opts.Mode == ModeLegacy || (s.opts.Mode == ModeDual && !modernRequest):
		s.serveLegacy(w, r, wr, req, isNotification)
		return
	}

	if isNotification {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if ok := s.validateModern(w, wr, req); !ok {
		return
	}
	s.dispatch(w, r, wr, req)
}

func (s *Server) handleInitialize(w http.ResponseWriter, wr wireRequest) {
	s.mu.Lock()
	s.sessionSeq++
	sid := "sess-" + strconv.Itoa(s.sessionSeq)
	s.sessions[sid] = true
	s.mu.Unlock()
	w.Header().Set(mcpgo.HeaderSessionID, sid)
	writeJSON(w, http.StatusOK, resultBody(wr.ID, map[string]any{
		"protocolVersion": ProtocolLegacy,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": "mcptest-stub", "version": "1.0.0"},
	}))
}

func (s *Server) serveLegacy(w http.ResponseWriter, r *http.Request, wr wireRequest, req Request, isNotification bool) {
	// A dual-era client probes server/discover before it has a session.
	if wr.Method == "server/discover" {
		switch s.opts.LegacyDiscover {
		case RejectHTTP404:
			w.WriteHeader(http.StatusNotFound)
		case RejectHTTP400Empty:
			w.WriteHeader(http.StatusBadRequest)
		default:
			writeJSON(w, http.StatusOK, errorBody(wr.ID, -32601, "method not found", nil))
		}
		return
	}
	if req.SessionID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	known := s.sessions[req.SessionID]
	s.mu.Unlock()
	if !known {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if isNotification {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	switch wr.Method {
	case "ping":
		writeJSON(w, http.StatusOK, resultBody(wr.ID, map[string]any{}))
		return
	case "tools/list", "tools/call":
		s.dispatch(w, r, wr, req)
		return
	}
	if h := s.handler(wr.Method); h != nil {
		s.writeReply(w, wr.ID, h(r.Context(), req))
		return
	}
	writeJSON(w, http.StatusOK, errorBody(wr.ID, -32601, "method not found", nil))
}

func (s *Server) validateModern(w http.ResponseWriter, wr wireRequest, req Request) bool {
	version := req.ProtocolVersion()
	if version != ProtocolModern {
		writeJSON(w, http.StatusBadRequest, errorBody(wr.ID, -32022, "unsupported protocol version",
			map[string]any{"supported": []string{ProtocolModern}, "requested": version}))
		return false
	}
	if got := req.Header.Get(mcpgo.HeaderProtocolVersion); got != version {
		writeJSON(w, http.StatusBadRequest, errorBody(wr.ID, -32020, "MCP-Protocol-Version header does not match _meta", nil))
		return false
	}
	if got := req.Header.Get(mcpgo.HeaderMethod); got != wr.Method {
		writeJSON(w, http.StatusBadRequest, errorBody(wr.ID, -32020, "Mcp-Method header does not match method", nil))
		return false
	}
	if want, ok := expectedName(wr.Method, req); ok {
		got, decodedOK := mcpgo.DecodeHeaderValue(req.Header.Get(mcpgo.HeaderName))
		if !decodedOK || got != want {
			writeJSON(w, http.StatusBadRequest, errorBody(wr.ID, -32020, "Mcp-Name header does not match params", nil))
			return false
		}
	}
	return true
}

func expectedName(method string, req Request) (string, bool) {
	var key string
	switch method {
	case "tools/call", "events/poll", "events/stream":
		key = "name"
	case "tasks/get", "tasks/update", "tasks/cancel":
		key = "taskId"
	default:
		return "", false
	}
	var v string
	if err := req.Param(key, &v); err != nil {
		return "", false
	}
	return v, true
}

func (s *Server) handler(method string) MethodHandler {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handlers[method]
}

func (s *Server) dispatch(w http.ResponseWriter, r *http.Request, wr wireRequest, req Request) {
	ctx := r.Context()
	if h := s.handler(wr.Method); h != nil {
		s.writeReply(w, wr.ID, h(ctx, req))
		return
	}
	switch wr.Method {
	case "server/discover":
		s.writeReply(w, wr.ID, s.discoverReply())
	case "tools/list":
		s.writeReply(w, wr.ID, s.toolsListReply())
	case "tools/call":
		s.writeReply(w, wr.ID, s.toolsCallReply(ctx, req))
	case "tasks/get", "tasks/update", "tasks/cancel":
		s.writeReply(w, wr.ID, s.tasksReply(wr.Method, req))
	case "events/list":
		s.writeReply(w, wr.ID, ResultReply(map[string]any{"resultType": "complete", "events": s.eventList()}))
	case "events/poll":
		s.writeReply(w, wr.ID, s.pollReply(req))
	case "events/stream":
		s.serveStream(w, r, wr, req)
	default:
		writeJSON(w, http.StatusNotFound, errorBody(wr.ID, -32601, "method not found: "+wr.Method, nil))
	}
}

func (s *Server) discoverReply() Reply {
	ext := s.opts.Extensions
	if ext == nil {
		ext = map[string]any{ExtensionTasks: map[string]any{}, ExtensionEvents: map[string]any{}}
	}
	versions := []string{ProtocolModern}
	if s.opts.Mode == ModeDual {
		versions = append(versions, ProtocolLegacy)
	}
	return ResultReply(map[string]any{
		"resultType":        "complete",
		"supportedVersions": versions,
		"capabilities":      map[string]any{"tools": map[string]any{}, "extensions": ext},
		"_meta":             map[string]any{mcpgo.MetaKeyServerInfo: map[string]any{"name": "mcptest-stub", "version": "1.0.0"}},
	})
}

func (s *Server) toolsListReply() Reply {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := make([]any, 0, len(s.tools))
	for _, t := range s.tools {
		list = append(list, map[string]any{
			"name":        t.name,
			"description": t.description,
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
		})
	}
	return ResultReply(map[string]any{"resultType": "complete", "tools": list})
}

func (s *Server) toolsCallReply(ctx context.Context, req Request) Reply {
	var name string
	if err := req.Param("name", &name); err != nil {
		return ErrorReply(-32602, "missing tool name", nil)
	}
	var args map[string]any
	_ = req.Param("arguments", &args)
	s.mu.Lock()
	var handler ToolHandler
	for _, t := range s.tools {
		if t.name == name {
			handler = t.handler
		}
	}
	s.mu.Unlock()
	if handler == nil {
		return ErrorReply(-32602, "unknown tool "+name, nil)
	}
	return handler(ctx, ToolCall{Name: name, Arguments: args, Request: req, TasksDeclared: req.HasExtension(ExtensionTasks)})
}

func (s *Server) tasksReply(method string, req Request) Reply {
	if !req.HasExtension(ExtensionTasks) {
		return ErrorReply(-32021, "tasks extension not declared", nil)
	}
	var id string
	if err := req.Param("taskId", &id); err != nil || id == "" {
		return ErrorReply(-32602, "missing taskId", nil)
	}
	switch method {
	case "tasks/get":
		task, ok := s.Task(id)
		if !ok {
			return ErrorReply(-32602, "unknown task", nil)
		}
		out, _ := json.Marshal(task)
		var m map[string]any
		_ = json.Unmarshal(out, &m)
		m["resultType"] = "complete"
		return ResultReply(m)
	case "tasks/update":
		var responses map[string]json.RawMessage
		if err := req.Param("inputResponses", &responses); err != nil {
			return ErrorReply(-32602, "missing inputResponses", nil)
		}
		s.mu.Lock()
		t, ok := s.tasks[id]
		if !ok {
			s.mu.Unlock()
			return ErrorReply(-32602, "unknown task", nil)
		}
		s.taskUpdates[id] = append(s.taskUpdates[id], responses)
		for key := range responses {
			delete(t.InputRequests, key)
		}
		if t.Status == "input_required" && len(t.InputRequests) == 0 {
			t.Status = "working"
			t.InputRequests = nil
		}
		t.LastUpdatedAt = time.Now().UTC().Format(time.RFC3339)
		hook := s.onTaskUpdate
		s.mu.Unlock()
		if hook != nil {
			hook(id, responses)
		}
		return ResultReply(map[string]any{"resultType": "complete"})
	default: // tasks/cancel
		ok := s.UpdateTask(id, func(t *Task) {
			switch t.Status {
			case "completed", "failed", "cancelled":
			default:
				t.Status = "cancelled"
				t.InputRequests = nil
			}
		})
		if !ok {
			return ErrorReply(-32602, "unknown task", nil)
		}
		return ResultReply(map[string]any{"resultType": "complete"})
	}
}

func (s *Server) eventList() []EventDescriptor {
	if s.opts.Events == nil {
		return []EventDescriptor{}
	}
	return s.opts.Events
}

func (s *Server) pollReply(req Request) Reply {
	var name string
	_ = req.Param("name", &name)
	s.mu.Lock()
	var next PollReply
	queue := s.polls[name]
	if len(queue) > 0 {
		next = queue[0]
		s.polls[name] = queue[1:]
	} else {
		c := "cursor-0"
		next = PollReply{Cursor: &c, NextPollMs: 1000}
	}
	s.mu.Unlock()
	events := next.Events
	if events == nil {
		events = []EventOccurrence{}
	}
	return ResultReply(map[string]any{
		"resultType": "complete", "events": events, "cursor": next.Cursor,
		"truncated": next.Truncated, "hasMore": next.HasMore, "nextPollMs": next.NextPollMs,
	})
}

func (s *Server) serveStream(w http.ResponseWriter, r *http.Request, wr wireRequest, req Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, errorBody(wr.ID, -32603, "streaming unsupported", nil))
		return
	}
	s.mu.Lock()
	script := s.streamScript
	s.mu.Unlock()
	if script == nil {
		script = func(ctx context.Context, st *Stream, _ Request) {
			c := "cursor-0"
			_ = st.Active(&c, false)
			<-ctx.Done()
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go func() {
		select {
		case <-s.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	script(ctx, &Stream{w: w, flusher: flusher, id: wr.ID}, req)
}

func (s *Server) writeReply(w http.ResponseWriter, id json.RawMessage, reply Reply) {
	switch {
	case reply.raw:
		w.WriteHeader(reply.status)
		if reply.body != "" {
			_, _ = io.WriteString(w, reply.body)
		}
	case reply.err != nil:
		status := reply.status
		if status == 0 {
			status = http.StatusOK
		}
		writeJSON(w, status, errorBody(id, reply.err.Code, reply.err.Message, reply.err.Data))
	default:
		writeJSON(w, http.StatusOK, resultBody(id, reply.result))
	}
}

func resultBody(id json.RawMessage, result any) map[string]any {
	if result == nil {
		result = map[string]any{}
	}
	return map[string]any{"jsonrpc": "2.0", "id": id, "result": result}
}

func errorBody(id json.RawMessage, code int, message string, data any) map[string]any {
	e := map[string]any{"code": code, "message": message}
	if data != nil {
		e["data"] = data
	}
	var idValue any = id
	if len(id) == 0 {
		idValue = nil
	}
	return map[string]any{"jsonrpc": "2.0", "id": idValue, "error": e}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// String returns a short description for test logs.
func (m Mode) String() string {
	switch m {
	case ModeLegacy:
		return "legacy"
	case ModeDual:
		return "dual"
	default:
		return "modern"
	}
}
