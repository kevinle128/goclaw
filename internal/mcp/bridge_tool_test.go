package mcp

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

func TestInputSchemaToMap(t *testing.T) {
	schema := mcpgo.ToolInputSchema{
		Type: "object",
		Properties: map[string]any{
			"query": map[string]any{
				"type":        "string",
				"description": "Search query",
			},
		},
		Required: []string{"query"},
	}

	m := inputSchemaToMap(schema)

	if m["type"] != "object" {
		t.Errorf("expected type=object, got %v", m["type"])
	}

	props, ok := m["properties"].(map[string]any)
	if !ok || props == nil {
		t.Fatal("expected properties map")
	}
	if _, ok := props["query"]; !ok {
		t.Error("expected 'query' in properties")
	}

	req, ok := m["required"].([]string)
	if !ok || len(req) != 1 || req[0] != "query" {
		t.Errorf("expected required=[query], got %v", m["required"])
	}
}

func TestInputSchemaToMap_EmptyType(t *testing.T) {
	schema := mcpgo.ToolInputSchema{}
	m := inputSchemaToMap(schema)

	if m["type"] != "object" {
		t.Errorf("expected default type=object, got %v", m["type"])
	}
}

func TestInputSchemaToMap_ObjectNoProperties(t *testing.T) {
	schema := mcpgo.ToolInputSchema{Type: "object"}
	m := inputSchemaToMap(schema)

	props, ok := m["properties"].(map[string]any)
	if !ok || props == nil {
		t.Fatal("expected empty properties map for object schema, got nil — OpenAI rejects object schemas without properties")
	}
	if len(props) != 0 {
		t.Errorf("expected empty properties, got %v", props)
	}
}

func TestExtractTextContent(t *testing.T) {
	result := &mcpgo.CallToolResult{
		Content: []mcpgo.Content{
			mcpgo.TextContent{Type: "text", Text: "hello"},
			mcpgo.TextContent{Type: "text", Text: "world"},
		},
	}

	got := extractTextContent(result)
	if got != "hello\nworld" {
		t.Errorf("expected 'hello\\nworld', got %q", got)
	}
}

func TestExtractTextContent_Nil(t *testing.T) {
	if got := extractTextContent(nil); got != "" {
		t.Errorf("expected empty for nil, got %q", got)
	}

	result := &mcpgo.CallToolResult{}
	if got := extractTextContent(result); got != "" {
		t.Errorf("expected empty for no content, got %q", got)
	}
}

func TestBridgeToolNaming(t *testing.T) {
	mcpTool := mcpgo.Tool{
		Name:        "query",
		Description: "Run a query",
		InputSchema: mcpgo.ToolInputSchema{Type: "object"},
	}

	// Without prefix → auto-derived from server name
	bt := NewBridgeTool("myserver", mcpTool, nil, "", 30, nil, uuid.Nil, nil)
	if bt.Name() != "mcp_myserver__query" {
		t.Errorf("expected name=mcp_myserver__query, got %s", bt.Name())
	}
	if bt.ServerName() != "myserver" {
		t.Errorf("expected serverName=myserver, got %s", bt.ServerName())
	}
	if bt.OriginalName() != "query" {
		t.Errorf("expected originalName=query, got %s", bt.OriginalName())
	}

	// With non-mcp_ prefix → gets mcp_ prepended
	bt2 := NewBridgeTool("myserver", mcpTool, nil, "pg", 0, nil, uuid.Nil, nil)
	if bt2.Name() != "mcp_pg__query" {
		t.Errorf("expected name=mcp_pg__query, got %s", bt2.Name())
	}
	if bt2.OriginalName() != "query" {
		t.Errorf("expected originalName=query, got %s", bt2.OriginalName())
	}

	// With mcp_ prefix → unchanged
	bt3 := NewBridgeTool("myserver", mcpTool, nil, "mcp_pg", 0, nil, uuid.Nil, nil)
	if bt3.Name() != "mcp_pg__query" {
		t.Errorf("expected name=mcp_pg__query, got %s", bt3.Name())
	}

	// Server name with hyphens → sanitized to underscores
	bt4 := NewBridgeTool("my-server", mcpTool, nil, "", 0, nil, uuid.Nil, nil)
	if bt4.Name() != "mcp_my_server__query" {
		t.Errorf("expected name=mcp_my_server__query, got %s", bt4.Name())
	}

	// Default timeout
	if bt2.timeoutSec != 60 {
		t.Errorf("expected default timeout=60, got %d", bt2.timeoutSec)
	}
}

func TestBridgeToolWithHints(t *testing.T) {
	mcpTool := mcpgo.Tool{
		Name:        "search",
		Description: "Run a search",
		InputSchema: mcpgo.ToolInputSchema{Type: "object"},
	}

	// No hints → original description unchanged
	bt := NewBridgeTool("srv", mcpTool, nil, "", 30, nil, uuid.Nil, nil)
	if bt.Description() != "Run a search" {
		t.Errorf("expected unchanged description, got %q", bt.Description())
	}

	// Global hint only
	bt2 := NewBridgeTool("srv", mcpTool, nil, "", 30, nil, uuid.Nil, nil).
		WithHints("No trailing semicolons.", "")
	got := bt2.Description()
	if got != "Run a search\n\n[Server hint] No trailing semicolons." {
		t.Errorf("global-only mismatch:\n%q", got)
	}

	// Per-tool hint only
	bt3 := NewBridgeTool("srv", mcpTool, nil, "", 30, nil, uuid.Nil, nil).
		WithHints("", "Use arrow func.")
	if bt3.Description() != "Run a search\n\n[Tool hint] Use arrow func." {
		t.Errorf("tool-only mismatch: %q", bt3.Description())
	}

	// Both hints — order: global then tool
	bt4 := NewBridgeTool("srv", mcpTool, nil, "", 30, nil, uuid.Nil, nil).
		WithHints("G.", "T.")
	if bt4.Description() != "Run a search\n\n[Server hint] G.\n\n[Tool hint] T." {
		t.Errorf("combined mismatch: %q", bt4.Description())
	}

	// Whitespace-only hints → treated as empty (no suffix)
	bt5 := NewBridgeTool("srv", mcpTool, nil, "", 30, nil, uuid.Nil, nil).
		WithHints("  \n ", "\t")
	if bt5.Description() != "Run a search" {
		t.Errorf("whitespace-only hints should render no suffix, got %q", bt5.Description())
	}

	// WithHints can be chained and reset by re-calling
	bt6 := NewBridgeTool("srv", mcpTool, nil, "", 30, nil, uuid.Nil, nil).
		WithHints("first", "hint")
	bt6.WithHints("", "")
	if bt6.Description() != "Run a search" {
		t.Errorf("calling WithHints with empty should clear suffix, got %q", bt6.Description())
	}
}

func TestIsPlaceholderValue(t *testing.T) {
	// Should be detected as placeholder.
	placeholders := []string{
		"null", "None", "nil", "UNDEFINED", "n/a",
		"optional", "Optional", "OPTIONAL",
		"skip", "Skip",
		"__OMIT__", "__skip__", "__EMPTY__",
		"http://example.com", "https://example.com",
		"http://localhost", "https://localhost",
		"PLACEHOLDER", "NOT_SET", "DO_NOT_SEND",
	}
	for _, s := range placeholders {
		if !isPlaceholderValue(s) {
			t.Errorf("expected isPlaceholderValue(%q) = true", s)
		}
	}

	// Should NOT be detected as placeholder (real values).
	realValues := []string{
		"", // empty string handled separately by type-aware check
		"sk-abc123",
		"my-proxy.example.com",
		"https://api.reviewweb.site/v1",
		"gpt-4o-mini",
		"bullet",
		"hello world",
		"ab", // too short for all-caps check
	}
	for _, s := range realValues {
		if isPlaceholderValue(s) {
			t.Errorf("expected isPlaceholderValue(%q) = false", s)
		}
	}
}

func TestStripEmptyOptionalArgs(t *testing.T) {
	bt := &BridgeTool{
		requiredSet: map[string]bool{"url": true},
		inputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"url":      map[string]any{"type": "string"},
				"api_key":  map[string]any{"type": "string"},
				"timeout":  map[string]any{"type": "number"},
				"debug":    map[string]any{"type": "boolean"},
				"keywords": map[string]any{"type": "string"},
			},
		},
	}

	args := map[string]any{
		"url":      "https://example.com",
		"api_key":  "optional",    // placeholder → strip
		"timeout":  nil,           // nil → strip
		"debug":    true,          // real boolean → keep
		"keywords": "",            // empty string for string-typed → keep
	}

	cleaned := bt.stripEmptyOptionalArgs(args)

	if cleaned["url"] != "https://example.com" {
		t.Error("required param 'url' should be preserved")
	}
	if _, ok := cleaned["api_key"]; ok {
		t.Error("placeholder 'optional' should be stripped for api_key")
	}
	if _, ok := cleaned["timeout"]; ok {
		t.Error("nil should be stripped for timeout")
	}
	if cleaned["debug"] != true {
		t.Error("real boolean value should be preserved")
	}
	if v, ok := cleaned["keywords"]; !ok || v != "" {
		t.Error("empty string should be kept for string-typed optional param 'keywords'")
	}
}

func TestStripEmptyOptionalArgs_EmptyStringNonString(t *testing.T) {
	bt := &BridgeTool{
		requiredSet: map[string]bool{},
		inputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"timeout": map[string]any{"type": "number"},
				"count":   map[string]any{"type": "integer"},
			},
		},
	}

	args := map[string]any{
		"timeout": "",
		"count":   "",
	}

	cleaned := bt.stripEmptyOptionalArgs(args)

	if _, ok := cleaned["timeout"]; ok {
		t.Error("empty string should be stripped for number-typed param")
	}
	if _, ok := cleaned["count"]; ok {
		t.Error("empty string should be stripped for integer-typed param")
	}
}

func TestEnsureMCPPrefix(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		serverName string
		want       string
	}{
		{"empty prefix", "", "vnstock", "mcp_vnstock"},
		{"empty prefix hyphenated server", "", "my-server", "mcp_my_server"},
		{"non-mcp prefix", "pg", "postgres", "mcp_pg"},
		{"already mcp_ prefix", "mcp_pg", "postgres", "mcp_pg"},
		{"mcp prefix without underscore", "mcp", "x", "mcp_mcp"},
		{"custom prefix with underscores", "vnstock", "vnstock", "mcp_vnstock"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureMCPPrefix(tt.prefix, tt.serverName)
			if got != tt.want {
				t.Errorf("ensureMCPPrefix(%q, %q) = %q, want %q", tt.prefix, tt.serverName, got, tt.want)
			}
		})
	}
}

// bridgeToolFor connects to srv and returns the BridgeTool of toolName.
func bridgeToolFor(t *testing.T, srv *mcptest.Server, toolName string) (*BridgeTool, *serverState) {
	t.Helper()
	ctx := context.Background()
	ss, mcpTools, err := connectAndDiscover(ctx, "stub", "streamable-http", "", nil, nil, srv.URL, nil, 5, ProtocolAuto)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.client.Close() })
	for _, mt := range mcpTools {
		if mt.Name == toolName {
			return NewBridgeTool("stub", mt, &ss.clientPtr, "", 5, &ss.connected, uuid.Nil, nil), ss
		}
	}
	t.Fatalf("tool %q not listed", toolName)
	return nil, nil
}

type recordingTaskStarter struct {
	got []TaskStart
}

func (r *recordingTaskStarter) StartTask(_ context.Context, start TaskStart) *tools.Result {
	r.got = append(r.got, start)
	return tools.NewResult("watching " + start.Task.TaskID)
}

func TestModernCallToolCompleteResultUnchanged(t *testing.T) {
	complete := func(withResultType bool) mcptest.ToolHandler {
		return func(context.Context, mcptest.ToolCall) mcptest.Reply {
			res := map[string]any{"content": []any{map[string]any{"type": "text", "text": "hello"}}}
			if withResultType {
				res["resultType"] = "complete"
			}
			return mcptest.ResultReply(res)
		}
	}
	legacySrv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeLegacy})
	legacySrv.AddTool("echo", "Echo", complete(false))
	legacyTool, _ := bridgeToolFor(t, legacySrv, "echo")
	want := legacyTool.Execute(context.Background(), nil)
	if want.IsError || !strings.Contains(want.ForLLM, "hello") {
		t.Fatalf("legacy result = %+v", want)
	}

	for _, withRT := range []bool{false, true} {
		srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
		srv.AddTool("echo", "Echo", complete(withRT))
		bt, ss := bridgeToolFor(t, srv, "echo")
		if ss.era() != EraModern {
			t.Fatalf("era = %v", ss.era())
		}
		got := bt.Execute(context.Background(), map[string]any{"q": "x"})
		if got.IsError || got.ForLLM != want.ForLLM {
			t.Errorf("resultType present=%v: modern result %q differs from legacy %q", withRT, got.ForLLM, want.ForLLM)
		}
		if len(srv.RequestsFor("tools/call")) != 1 {
			t.Errorf("tools/call count = %d", len(srv.RequestsFor("tools/call")))
		}
	}
}

func TestModernCallToolErrorFlagUnchanged(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
	srv.AddTool("fail", "Fail", func(context.Context, mcptest.ToolCall) mcptest.Reply {
		return mcptest.ResultReply(map[string]any{"isError": true, "content": []any{map[string]any{"type": "text", "text": "boom"}}})
	})
	bt, _ := bridgeToolFor(t, srv, "fail")
	got := bt.Execute(context.Background(), nil)
	if !got.IsError || got.ForLLM != "boom" {
		t.Errorf("result = %+v, want error \"boom\"", got)
	}
}

func TestModernCallToolTaskResultIsDiscriminated(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
	srv.AddTaskTool("run_workflow", "Start a run")

	bt, _ := bridgeToolFor(t, srv, "run_workflow")
	got := bt.Execute(context.Background(), map[string]any{"workflow": "w"})
	if got.IsError || !strings.Contains(got.ForLLM, "MCP task task-1 started (status working).") {
		t.Fatalf("nil starter result = %+v", got)
	}
	call := srv.RequestsFor("tools/call")[0]
	if !call.HasExtension(mcptest.ExtensionTasks) || !call.HasExtension(mcptest.ExtensionEvents) {
		t.Errorf("advertised extensions not declared: %v", call.ClientCapabilities())
	}
	if _, ok := call.ClientCapabilities()["elicitation"]; ok {
		t.Errorf("elicitation declared without a form-capable run: %v", call.ClientCapabilities())
	}

	starter := &recordingTaskStarter{}
	bt.WithTaskStarter(starter)
	got = bt.Execute(WithElicitationForm(context.Background(), true), nil)
	if got.ForLLM != "watching task-2" || len(starter.got) != 1 {
		t.Fatalf("starter result = %+v, calls %d", got, len(starter.got))
	}
	start := starter.got[0]
	if start.Task.Status != "working" || start.Task.TTLMs == nil || *start.Task.TTLMs != 2592000000 ||
		start.Task.PollIntervalMs == nil || *start.Task.PollIntervalMs != 5000 || start.Task.CreatedAt == "" {
		t.Errorf("CreateTaskResult decoded wrong: %+v", start.Task)
	}
	if !start.Capabilities.Tasks || !start.Capabilities.Events || !start.Capabilities.ElicitationForm || start.Capabilities.ID != "" {
		t.Errorf("capability set = %+v", start.Capabilities)
	}
	if start.Client == nil || start.ToolName != "run_workflow" {
		t.Errorf("task start = %+v", start)
	}
	call = srv.RequestsFor("tools/call")[1]
	elic, _ := call.ClientCapabilities()["elicitation"].(map[string]any)
	if _, ok := elic["form"]; !ok {
		t.Errorf("elicitation form not declared for a form-capable run: %v", call.ClientCapabilities())
	}
}

func TestModernCallToolDeclaresOnlyAdvertisedExtensions(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern, Extensions: map[string]any{}})
	srv.AddTaskTool("run_workflow", "Start a run")
	bt, _ := bridgeToolFor(t, srv, "run_workflow")
	got := bt.Execute(context.Background(), nil)
	if got.IsError || !strings.Contains(got.ForLLM, "run_id: task-1") {
		t.Fatalf("result = %+v, want the plain result", got)
	}
	call := srv.RequestsFor("tools/call")[0]
	if call.HasExtension(mcptest.ExtensionTasks) || call.HasExtension(mcptest.ExtensionEvents) {
		t.Errorf("declared extensions the server did not advertise: %v", call.ClientCapabilities())
	}
}

func TestModernCallToolUnknownResultTypeIsError(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
	srv.AddTool("weird", "Weird", func(context.Context, mcptest.ToolCall) mcptest.Reply {
		return mcptest.ResultReply(map[string]any{"resultType": "banana", "content": []any{}})
	})
	srv.AddTool("ask", "Ask", func(context.Context, mcptest.ToolCall) mcptest.Reply {
		return mcptest.ResultReply(map[string]any{"resultType": "input_required", "requestState": "s"})
	})
	weird, _ := bridgeToolFor(t, srv, "weird")
	if got := weird.Execute(context.Background(), nil); !got.IsError || !strings.Contains(got.ForLLM, "banana") {
		t.Errorf("unknown resultType result = %+v", got)
	}
	ask, _ := bridgeToolFor(t, srv, "ask")
	if got := ask.Execute(context.Background(), nil); !got.IsError ||
		!strings.Contains(got.ForLLM, "server asks for input before the task starts; not supported") {
		t.Errorf("input_required result = %+v", got)
	}
}

func TestModern404IsNotSessionReset(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
	srv.AddTool("echo", "Echo", func(context.Context, mcptest.ToolCall) mcptest.Reply { return mcptest.TextReply("ok") })
	var calls atomic.Int32
	srv.Handle("tools/call", func(context.Context, mcptest.Request) mcptest.Reply {
		if calls.Add(1) == 1 {
			return mcptest.HTTPReply(404, `{"jsonrpc":"2.0","id":null,"error":{"code":-32601,"message":"Method not found"}}`)
		}
		return mcptest.TextReply("ok")
	})
	bt, _ := bridgeToolFor(t, srv, "echo")
	var reconnects atomic.Int32
	bt.WithForceReconnect(func(string) { reconnects.Add(1) })

	got := bt.Execute(context.Background(), nil)
	if !got.IsError || strings.Contains(got.ForLLM, "reset its session") {
		t.Errorf("404 result = %+v, want a plain error", got)
	}
	if reconnects.Load() != 0 || !bt.IsConnected() {
		t.Errorf("a modern 404 started a reconnect (count %d, connected %v)", reconnects.Load(), bt.IsConnected())
	}
	if again := bt.Execute(context.Background(), nil); again.IsError || !strings.Contains(again.ForLLM, "ok") {
		t.Errorf("call after the 404 = %+v", again)
	}
}

func TestLegacyHTTPSessionResetStillReconnects(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeLegacy})
	srv.AddTool("echo", "Echo", func(context.Context, mcptest.ToolCall) mcptest.Reply { return mcptest.TextReply("ok") })
	bt, ss := bridgeToolFor(t, srv, "echo")
	if ss.era() != EraLegacy {
		t.Fatalf("era = %v", ss.era())
	}
	var reasons []string
	bt.WithForceReconnect(func(reason string) { reasons = append(reasons, reason) })

	srv.ForgetSessions()
	got := bt.Execute(context.Background(), nil)
	if !got.IsError || !strings.Contains(got.ForLLM, "reset its session") {
		t.Errorf("result = %+v, want the session-reset hint", got)
	}
	if len(reasons) != 1 {
		t.Errorf("force reconnect calls = %v, want 1", reasons)
	}
}
