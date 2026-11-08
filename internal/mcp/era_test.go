package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client/transport"
	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/nextlevelbuilder/goclaw/internal/mcp/mcptest"
)

// The test binary also runs as a fake stdio MCP server when envStdioFake is
// set. The fake answers initialize (2025-11-25), tools/list and ping, writes
// every method it receives to the file named by envStdioLog, and ignores
// unknown methods without a reply, as some real stdio servers do.
const (
	envStdioFake = "GOCLAW_MCP_TEST_STDIO_FAKE"
	envStdioLog  = "GOCLAW_MCP_TEST_STDIO_LOG"
)

func TestMain(m *testing.M) {
	if os.Getenv(envStdioFake) == "1" {
		runStdioFake()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runStdioFake() {
	logFile, err := os.OpenFile(os.Getenv(envStdioLog), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(2)
	}
	defer logFile.Close()
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	enc := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if json.Unmarshal(scanner.Bytes(), &msg) != nil {
			continue
		}
		fmt.Fprintln(logFile, msg.Method)
		if len(msg.ID) == 0 {
			continue
		}
		var result any
		switch msg.Method {
		case "initialize":
			result = map[string]any{
				"protocolVersion": "2025-11-25",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "stdio-fake", "version": "1"},
			}
		case "tools/list":
			result = map[string]any{"tools": []any{}}
		case "ping":
			result = map[string]any{}
		default:
			continue
		}
		_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": result})
	}
}

func stdioFakeEnv(t *testing.T) (map[string]string, string) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "methods.log")
	return map[string]string{envStdioFake: "1", envStdioLog: logPath}, logPath
}

func readMethods(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read methods log: %v", err)
	}
	return strings.Fields(string(data))
}

func TestStdioServersNeverProbeDiscover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	env, logPath := stdioFakeEnv(t)

	start := time.Now()
	ss, _, err := connectAndDiscover(ctx, "stdio-fake", "stdio", os.Args[0], nil, env, "", nil, 5, ProtocolAuto)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer ss.client.Close()

	// A server/discover probe that gets no reply waits 5 s.
	if elapsed > 3*time.Second {
		t.Errorf("connect took %v; a stdio connect must not wait for a discover probe", elapsed)
	}
	methods := readMethods(t, logPath)
	if len(methods) == 0 || methods[0] != "initialize" {
		t.Fatalf("first method = %v, want initialize", methods)
	}
	if slices.Contains(methods, "server/discover") {
		t.Errorf("stdio connection sent server/discover: %v", methods)
	}
	if ss.era() != EraLegacy || ss.client.ProtocolVersion() != mcpgo.LATEST_LEGACY_PROTOCOL_VERSION {
		t.Errorf("era = %v, version = %q; want legacy 2025-11-25", ss.era(), ss.client.ProtocolVersion())
	}
}

func TestDiscoverToolsStdioNeverProbesDiscover(t *testing.T) {
	env, logPath := stdioFakeEnv(t)
	start := time.Now()
	if _, err := DiscoverTools(context.Background(), "stdio", os.Args[0], nil, env, "", nil); err != nil {
		t.Fatalf("DiscoverTools: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("DiscoverTools took %v; want no discover probe wait", elapsed)
	}
	if methods := readMethods(t, logPath); slices.Contains(methods, "server/discover") {
		t.Errorf("DiscoverTools sent server/discover on stdio: %v", methods)
	}
}

func connectStub(t *testing.T, srv *mcptest.Server, protocol ProtocolMode) *serverState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ss, _, err := connectAndDiscover(ctx, "stub", "streamable-http", "", nil, nil, srv.URL, nil, 5, protocol)
	if err != nil {
		t.Fatalf("connect to stub: %v", err)
	}
	t.Cleanup(func() { _ = ss.client.Close() })
	return ss
}

func TestEraFromNegotiatedVersion(t *testing.T) {
	cases := []struct {
		name       string
		opts       mcptest.Options
		protocol   ProtocolMode
		want       ProtocolEra
		wantProbes bool // server/discover sent
	}{
		{"modern stub", mcptest.Options{Mode: mcptest.ModeModern}, ProtocolAuto, EraModern, true},
		{"dual stub auto", mcptest.Options{Mode: mcptest.ModeDual}, ProtocolAuto, EraModern, true},
		{"legacy stub method not found", mcptest.Options{Mode: mcptest.ModeLegacy}, ProtocolAuto, EraLegacy, true},
		{"legacy stub 404", mcptest.Options{Mode: mcptest.ModeLegacy, LegacyDiscover: mcptest.RejectHTTP404}, ProtocolAuto, EraLegacy, true},
		{"legacy stub 400 empty", mcptest.Options{Mode: mcptest.ModeLegacy, LegacyDiscover: mcptest.RejectHTTP400Empty}, ProtocolAuto, EraLegacy, true},
		{"setting legacy on dual stub", mcptest.Options{Mode: mcptest.ModeDual}, ProtocolLegacy, EraLegacy, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := mcptest.New(t, tc.opts)
			ss := connectStub(t, srv, tc.protocol)
			if got := ss.era(); got != tc.want {
				t.Errorf("era = %v, want %v (version %q)", got, tc.want, ss.client.ProtocolVersion())
			}
			if probed := len(srv.RequestsFor("server/discover")) > 0; probed != tc.wantProbes {
				t.Errorf("server/discover sent = %v, want %v (methods %v)", probed, tc.wantProbes, srv.Methods())
			}
			cs := ss.currentSession()
			if cs == nil || cs.client != ss.client || cs.router == nil {
				t.Fatal("session state is not set for the connected client")
			}
			if tc.want == EraModern {
				if !cs.discover.HasExtension(ExtensionTasks) || !cs.discover.HasExtension(ExtensionEvents) {
					t.Errorf("discover result lost the advertised extensions: %+v", cs.discover)
				}
				if cs.discover.ServerInfo.Name != "mcptest-stub" {
					t.Errorf("server info = %+v", cs.discover.ServerInfo)
				}
			} else if cs.discover != nil {
				t.Errorf("legacy connection has a discover result: %+v", cs.discover)
			}
		})
	}
}

func TestFullReconnectRefreshesEra(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeDual})
	ss := connectStub(t, srv, ProtocolAuto)
	if ss.era() != EraModern {
		t.Fatalf("initial era = %v, want modern", ss.era())
	}
	ss.conn.protocol = ProtocolLegacy
	if !fullReconnect(context.Background(), ss) {
		t.Fatal("fullReconnect failed")
	}
	t.Cleanup(func() { _ = ss.client.Close() })
	if ss.era() != EraLegacy || ss.currentSession().client != ss.clientPtr.Load() {
		t.Errorf("after reconnect era = %v; session must follow the new client", ss.era())
	}
}

func TestParseProtocolMode(t *testing.T) {
	cases := map[string]ProtocolMode{
		``:                          ProtocolAuto,
		`{}`:                        ProtocolAuto,
		`not json`:                  ProtocolAuto,
		`{"protocol":"auto"}`:       ProtocolAuto,
		`{"protocol":"legacy"}`:     ProtocolLegacy,
		`{"protocol":" LEGACY "}`:   ProtocolLegacy,
		`{"protocol":"2026-07-28"}`: ProtocolAuto,
		`{"protocol":5}`:            ProtocolAuto,
	}
	for in, want := range cases {
		if got := ParseProtocolMode(json.RawMessage(in)); got != want {
			t.Errorf("ParseProtocolMode(%s) = %q, want %q", in, got, want)
		}
	}
}

func TestModernLivenessUsesDiscover(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
	ss := connectStub(t, srv, ProtocolAuto)

	before := len(srv.RequestsFor("server/discover"))
	if err := checkLiveness(context.Background(), ss.client); err != nil {
		t.Fatalf("liveness on a running modern server: %v", err)
	}
	if after := len(srv.RequestsFor("server/discover")); after != before+1 {
		t.Errorf("liveness sent %d server/discover requests, want 1", after-before)
	}

	srv.Close()
	if err := ss.client.Ping(context.Background()); err != nil {
		t.Logf("Ping returned %v (the library sends nothing on modern connections)", err)
	}
	if err := checkLiveness(context.Background(), ss.client); err == nil {
		t.Fatal("liveness reported a stopped modern server as healthy")
	}
}

func TestLegacyLivenessUsesPing(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeLegacy})
	ss := connectStub(t, srv, ProtocolAuto)
	if err := checkLiveness(context.Background(), ss.client); err != nil {
		t.Fatalf("liveness on a running legacy server: %v", err)
	}
	if len(srv.RequestsFor("ping")) != 1 {
		t.Errorf("legacy liveness methods = %v, want one ping", srv.Methods())
	}
}

func TestNoTasksOrEventsCapabilityOnInitialize(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern})
	srv.AddTool("echo", "Echo", func(context.Context, mcptest.ToolCall) mcptest.Reply { return mcptest.TextReply("x") })
	connectStub(t, srv, ProtocolAuto)

	reqs := append(srv.RequestsFor("server/discover"), srv.RequestsFor("tools/list")...)
	if len(reqs) < 2 {
		t.Fatalf("expected discover and tools/list, got %v", srv.Methods())
	}
	for _, r := range reqs {
		if r.HasExtension(mcptest.ExtensionTasks) || r.HasExtension(mcptest.ExtensionEvents) {
			t.Errorf("%s declared tasks/events in _meta: %v", r.Method, r.ClientCapabilities())
		}
	}
}

func TestConnectUnauthorizedKeepsLegacyErrorText(t *testing.T) {
	srv := mcptest.New(t, mcptest.Options{Mode: mcptest.ModeModern, RequireBearer: "good"})
	_, _, err := connectAndDiscover(context.Background(), "stub", "streamable-http", "", nil, nil, srv.URL,
		map[string]string{"Authorization": "Bearer bad"}, 5, ProtocolAuto)
	if err == nil {
		t.Fatal("expected an error for a bad token")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unauthorized (401)") {
		t.Errorf("error %q must contain \"unauthorized (401)\" for the credential purge path", err)
	}
	if !isUnauthorizedErr(err) {
		t.Errorf("isUnauthorizedErr(%v) = false", err)
	}
	if !isUnauthorizedErr(transport.NewError(&transport.AuthorizationRequiredError{})) {
		t.Error("isUnauthorizedErr must match a typed AuthorizationRequiredError")
	}
}
