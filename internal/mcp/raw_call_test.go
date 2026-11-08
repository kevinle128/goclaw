package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
)

func newModernStub(t *testing.T) (*mcptest.Server, *serverState) {
	t.Helper()
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
	srv.AddTool("echo", "Echo", func(context.Context, mcptest.ToolCall) mcptest.Reply { return mcptest.TextReply("hi") })
	srv.AddTool("héllo", "Non-ASCII name", func(context.Context, mcptest.ToolCall) mcptest.Reply { return mcptest.TextReply("hé") })
	return srv, connectStub(t, srv, ProtocolAuto)
}

func lastRequest(t *testing.T, srv *mcptest.Server, method string) mcptest.Request {
	t.Helper()
	reqs := srv.RequestsFor(method)
	if len(reqs) == 0 {
		t.Fatalf("no %s request recorded (methods %v)", method, srv.Methods())
	}
	return reqs[len(reqs)-1]
}

func TestRawCallCarriesMetaAndHeaders(t *testing.T) {
	srv, ss := newModernStub(t)
	ctx := context.Background()
	task := srv.CreateTask("task-abc")
	srv.SetStreamScript(func(_ context.Context, st *mcptest.Stream, _ mcptest.Request) {
		c := "c1"
		_ = st.Active(&c, false)
		_ = st.Complete()
	})

	calls := []struct {
		method   string
		params   map[string]any
		opts     RawOpts
		wantName string // "" = header absent
	}{
		{methodToolsCall, map[string]any{"name": "echo"}, RawOpts{}, "echo"},
		{methodTasksGet, map[string]any{"taskId": task.TaskID}, RawOpts{Tasks: true}, task.TaskID},
		{methodEventsStream, map[string]any{"name": "workflow_run.status_changed", "arguments": map[string]any{}, "cursor": nil}, RawOpts{Events: true}, "workflow_run.status_changed"},
		{methodEventsList, nil, RawOpts{Events: true}, ""},
	}
	for _, c := range calls {
		if _, err := rawCall(ctx, ss.client, c.method, c.params, c.opts); err != nil {
			t.Fatalf("%s: %v", c.method, err)
		}
		r := lastRequest(t, srv, c.method)
		if pv := r.ProtocolVersion(); pv != mcptest.ProtocolModern || r.Header.Get(mcpgo.HeaderProtocolVersion) != pv {
			t.Errorf("%s: _meta version %q, header %q; want equal 2026-07-28", c.method, pv, r.Header.Get(mcpgo.HeaderProtocolVersion))
		}
		if got := r.Header.Get(mcpgo.HeaderMethod); got != c.method {
			t.Errorf("%s: Mcp-Method = %q", c.method, got)
		}
		gotName, present := r.Header[mcpgo.HeaderName]
		switch {
		case c.wantName == "" && present:
			t.Errorf("%s: Mcp-Name must be absent, got %v", c.method, gotName)
		case c.wantName != "" && r.Header.Get(mcpgo.HeaderName) != c.wantName:
			t.Errorf("%s: Mcp-Name = %q, want %q", c.method, r.Header.Get(mcpgo.HeaderName), c.wantName)
		}
		if !strings.HasPrefix(r.IDString(), "goclaw-") {
			t.Errorf("%s: request id %q, want prefix goclaw-", c.method, r.IDString())
		}
		var info map[string]string
		_ = json.Unmarshal(r.Meta[mcpgo.MetaKeyClientInfo], &info)
		if info["name"] != "goclaw" || info["version"] == "" {
			t.Errorf("%s: clientInfo = %v", c.method, info)
		}
		if r.HasExtension(mcptest.ExtensionTasks) != c.opts.Tasks || r.HasExtension(mcptest.ExtensionEvents) != c.opts.Events {
			t.Errorf("%s: extensions %v do not match opts %+v", c.method, r.ClientCapabilities(), c.opts)
		}
	}
}

func TestRawCallEncodesNonASCIINameAndElicitation(t *testing.T) {
	srv, ss := newModernStub(t)
	_, err := rawCall(context.Background(), ss.client, methodToolsCall, map[string]any{"name": "héllo"}, RawOpts{ElicitationForm: true})
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	r := lastRequest(t, srv, methodToolsCall)
	if h := r.Header.Get(mcpgo.HeaderName); !strings.HasPrefix(h, "=?base64?") || !strings.HasSuffix(h, "?=") {
		t.Errorf("Mcp-Name = %q, want the Base64 sentinel form", h)
	}
	elic, _ := r.ClientCapabilities()["elicitation"].(map[string]any)
	if _, ok := elic["form"]; !ok {
		t.Errorf("elicitation form not declared: %v", r.ClientCapabilities())
	}
}

func TestRawCallDeadlineExceptForEventsStream(t *testing.T) {
	if d, ok := rawDeadline(methodEventsPoll, RawOpts{}); !ok || d != 30*time.Second {
		t.Errorf("events/poll default deadline = %v %v, want 30s", d, ok)
	}
	if _, ok := rawDeadline(methodEventsStream, RawOpts{Timeout: time.Second}); ok {
		t.Error("events/stream must not get a deadline")
	}

	srv, ss := newModernStub(t)
	srv.Handle(methodEventsList, func(ctx context.Context, _ mcptest.Request) mcptest.Reply {
		<-ctx.Done() // never answers, like a server that ignores the method
		return mcptest.HTTPReply(500, "")
	})
	start := time.Now()
	_, err := rawCall(context.Background(), ss.client, methodEventsList, nil, RawOpts{Timeout: 150 * time.Millisecond})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("events/list on a silent server: err = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("deadline took %v", elapsed)
	}

	const hold = 500 * time.Millisecond
	srv.SetStreamScript(func(ctx context.Context, st *mcptest.Stream, _ mcptest.Request) {
		c := "c1"
		_ = st.Active(&c, false)
		select {
		case <-time.After(hold):
		case <-ctx.Done():
			return
		}
		_ = st.Complete()
	})
	start = time.Now()
	_, err = rawCall(context.Background(), ss.client, methodEventsStream,
		map[string]any{"name": "e", "arguments": map[string]any{}}, RawOpts{Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("events/stream ended early: %v", err)
	}
	if elapsed := time.Since(start); elapsed < hold {
		t.Errorf("events/stream returned after %v, before the server closed it", elapsed)
	}
}

func TestRawCallTypedErrors(t *testing.T) {
	srv, ss := newModernStub(t)
	cases := []struct {
		code int
		want error
	}{
		{CodeHeaderMismatch, ErrHeaderMismatch},
		{CodeMissingRequiredClientCapability, ErrMissingRequiredClientCapability},
		{CodeUnsupportedProtocolVersion, ErrUnsupportedProtocolVersion},
		{CodeMethodNotFound, ErrMethodNotFound},
		{CodeInvalidParams, ErrInvalidParams},
		{CodeEventsNotFound, ErrEventsNotFound},
		{CodeEventsForbidden, ErrEventsForbidden},
		{CodeEventsResourceExhausted, ErrEventsResourceExhausted},
		{CodeEventsUnsupported, ErrEventsUnsupported},
		{CodeEventsCallbackEndpointErr, ErrEventsCallbackEndpoint},
	}
	for _, c := range cases {
		code := c.code
		srv.Handle(methodEventsPoll, func(context.Context, mcptest.Request) mcptest.Reply {
			return mcptest.ErrorReply(code, "scripted", map[string]any{"kind": "event"})
		})
		_, err := rawCall(context.Background(), ss.client, methodEventsPoll, map[string]any{"name": "e"}, RawOpts{Events: true})
		if !errors.Is(err, c.want) {
			t.Errorf("code %d: err = %v, want %v", c.code, err, c.want)
			continue
		}
		var rpcErr *RPCError
		if !errors.As(err, &rpcErr) || !strings.Contains(string(rpcErr.Data), "event") {
			t.Errorf("code %d: error data lost: %+v", c.code, rpcErr)
		}
	}
}

func TestRawCallModern404IsMethodNotFound(t *testing.T) {
	_, ss := newModernStub(t)
	_, err := rawCall(context.Background(), ss.client, "events/unknown", nil, RawOpts{})
	if !errors.Is(err, ErrMethodNotFound) {
		t.Fatalf("404 err = %v, want ErrMethodNotFound", err)
	}
	if isSessionUninitializedErr(err) {
		t.Errorf("a modern 404 must not read as a session reset: %v", err)
	}
	// The connection has no session, so later requests still go out.
	if _, err := rawCall(context.Background(), ss.client, methodToolsCall, map[string]any{"name": "echo"}, RawOpts{}); err != nil {
		t.Fatalf("request after a 404: %v", err)
	}
}

func TestRawCallRefusesLegacyConnection(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeLegacy})
	ss := connectStub(t, srv, ProtocolAuto)
	if _, err := rawCall(context.Background(), ss.client, methodTasksGet, map[string]any{"taskId": "x"}, RawOpts{Tasks: true}); err == nil {
		t.Fatal("rawCall must refuse a legacy connection")
	}
	if n := len(srv.RequestsFor(methodTasksGet)); n != 0 {
		t.Errorf("legacy server got %d tasks/get requests", n)
	}
}

func TestNextRawRequestIDIsUnique(t *testing.T) {
	a, b := nextRawRequestID(), nextRawRequestID()
	if a == b || !strings.HasPrefix(a, "goclaw-") {
		t.Errorf("ids %q %q", a, b)
	}
}
