package acpserver

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func helperMCPServer(t *testing.T) (MCPServer, string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	return MCPServer{
		Name: "helper", Command: executable,
		Args: []string{"-test.run=^TestMCPProcessSurvivesSessionNewContext$"},
		Env:  []EnvVariable{{Name: "ACP_TEST_MCP_HELPER", Value: "1"}},
	}, filepath.Dir(executable)
}

func TestIdleSessionKeepsIDAndStartsMCPAgain(t *testing.T) {
	definition, execRoot := helperMCPServer(t)
	root := t.TempDir()
	cwd := filepath.Join(root, "project")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{Gateway: NewGatewayClient(GatewayClientConfig{}), AgentID: "fixed", WorkspaceRoot: root, MCPExecRoots: []string{execRoot}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.sessions.Close()
	result, err := server.handleNewSession(context.Background(), NewSessionParams{CWD: cwd, MCPServers: []MCPServer{definition}})
	if err != nil {
		t.Fatal(err)
	}
	record, _ := server.sessions.Get(result.SessionID)

	if stopped := server.sessions.ReapIdle(time.Now().Add(SessionIdleTTL+time.Minute), SessionIdleTTL); stopped != 1 {
		t.Fatalf("ReapIdle() stopped %d sessions, want 1", stopped)
	}
	if _, ok := server.sessions.Get(result.SessionID); !ok {
		t.Fatal("idle session was removed; a later wake would get Unknown session")
	}
	if record.mcp != nil {
		t.Fatal("idle session kept its MCP processes")
	}

	_, generation, _, err := server.sessions.BeginPrompt(result.SessionID, context.Background(), nil)
	if err != nil {
		t.Fatalf("prompt on idle session: %v", err)
	}
	if err := server.resumeSessionMCP(context.Background(), record, generation); err != nil {
		t.Fatalf("resumeSessionMCP() = %v", err)
	}
	if record.mcp == nil || len(record.tools) != 1 || record.tools[0].Name != "mcp_helper__echo" {
		t.Fatalf("MCP not started again: tools = %#v", record.tools)
	}
	content, isError, err := record.mcp.Call(context.Background(), "mcp_helper__echo", map[string]any{"text": "hi"})
	if err != nil || isError || content == "" {
		t.Fatalf("Call() after resume = %q, %v, %v", content, isError, err)
	}
	server.sessions.FinishPrompt(result.SessionID, generation)

	// A prompt that already ended must not attach new MCP processes.
	server.sessions.ReapIdle(time.Now().Add(2*SessionIdleTTL), SessionIdleTTL)
	if err := server.resumeSessionMCP(context.Background(), record, generation); err == nil {
		t.Fatal("ended prompt started MCP processes")
	}
	if record.mcp != nil {
		t.Fatal("ended prompt kept MCP processes")
	}
}

func TestReapIdleSkipsActivePrompt(t *testing.T) {
	r := NewSessionRegistry(0)
	manager, err := NewSessionMCPManager(t.TempDir(), []string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Add(&sessionRecord{ID: "s", SessionKey: "key", mcp: manager}); err != nil {
		t.Fatal(err)
	}
	_, generation, _, err := r.BeginPrompt("s", context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.FinishPrompt("s", generation)
	if stopped := r.ReapIdle(time.Now().Add(24*time.Hour), SessionIdleTTL); stopped != 0 {
		t.Fatalf("ReapIdle() stopped %d sessions with an active prompt", stopped)
	}
}

func TestFullRegistryEvictsOldestIdleSession(t *testing.T) {
	r := NewSessionRegistry(0)
	r.limit = 2
	clock := time.Unix(1_000_000, 0)
	r.now = func() time.Time { clock = clock.Add(time.Second); return clock }

	for _, id := range []string{"active-oldest", "idle"} {
		if _, err := r.Add(&sessionRecord{ID: id, SessionKey: id}); err != nil {
			t.Fatal(err)
		}
	}
	// The oldest session has a prompt, so it must stay.
	record, _ := r.Get("active-oldest")
	record.mu.Lock()
	record.lastActive = time.Unix(0, 0)
	record.mu.Unlock()
	_, generation, _, err := r.BeginPrompt("active-oldest", context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	record.mu.Lock()
	record.lastActive = time.Unix(0, 0)
	record.mu.Unlock()

	evicted, err := r.Add(&sessionRecord{ID: "new", SessionKey: "new"})
	if err != nil || evicted != "idle" {
		t.Fatalf("Add() evicted %q, %v; want idle", evicted, err)
	}
	if _, ok := r.Get("active-oldest"); !ok {
		t.Fatal("session with an active prompt was evicted")
	}
	_, second, _, err := r.BeginPrompt("new", context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Add(&sessionRecord{ID: "third", SessionKey: "third"}); err == nil {
		t.Fatal("Add() succeeded with only active sessions")
	}
	r.FinishPrompt("active-oldest", generation)
	r.FinishPrompt("new", second)
}

func TestTransportAcceptsSessionsBeyondLimitWhenOldOnesAreEvicted(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "project")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{Gateway: NewGatewayClient(GatewayClientConfig{}), AgentID: "fixed", WorkspaceRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Unix(1_000_000, 0)
	server.sessions.now = func() time.Time { clock = clock.Add(time.Second); return clock }
	transport := NewTransport(strings.NewReader(""), io.Discard, Handler{}, "test-version")
	server.transport = transport
	var first string
	for i := 0; i < MaxActiveSessions+10; i++ {
		if !transport.reserveSession() {
			t.Fatalf("session/new %d refused after old sessions were evicted", i+1)
		}
		result, err := server.handleNewSession(context.Background(), NewSessionParams{CWD: cwd, MCPServers: []MCPServer{}})
		transport.releaseSessionReservation()
		if err != nil {
			t.Fatalf("session/new %d: %v", i+1, err)
		}
		if !transport.addSession(result.SessionID) {
			t.Fatalf("session/new %d: duplicate id", i+1)
		}
		if i == 0 {
			first = result.SessionID
		}
	}
	if server.sessions.Len() != MaxActiveSessions {
		t.Fatalf("registry holds %d sessions, want %d", server.sessions.Len(), MaxActiveSessions)
	}
	transport.stateMu.Lock()
	known := len(transport.state.sessions)
	transport.stateMu.Unlock()
	if known != MaxActiveSessions {
		t.Fatalf("transport knows %d sessions, want %d", known, MaxActiveSessions)
	}
	if transport.hasSession(first) {
		t.Fatal("transport still accepts an evicted session")
	}
}

func TestFailedMCPRestartDropsSession(t *testing.T) {
	definition, execRoot := helperMCPServer(t)
	root := t.TempDir()
	cwd := filepath.Join(root, "project")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{Gateway: NewGatewayClient(GatewayClientConfig{}), AgentID: "fixed", WorkspaceRoot: root, MCPExecRoots: []string{execRoot}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.sessions.Close()
	transport := NewTransport(strings.NewReader(""), io.Discard, Handler{HasSession: server.hasSession}, "test-version")
	server.transport = transport
	result, err := server.handleNewSession(context.Background(), NewSessionParams{CWD: cwd, MCPServers: []MCPServer{definition}})
	if err != nil {
		t.Fatal(err)
	}
	transport.addSession(result.SessionID)
	record, _ := server.sessions.Get(result.SessionID)
	server.sessions.ReapIdle(time.Now().Add(SessionIdleTTL+time.Minute), SessionIdleTTL)
	record.mu.Lock()
	record.servers[0].Command = filepath.Join(execRoot, "missing-mcp-server")
	record.mu.Unlock()

	_, generation, _, err := server.sessions.BeginPrompt(result.SessionID, context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = server.resumeSessionMCP(context.Background(), record, generation)
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != CodeUnknownSession {
		t.Fatalf("resumeSessionMCP() = %v, want Unknown session so the client creates a new session", err)
	}
	if _, ok := server.sessions.Get(result.SessionID); ok {
		t.Fatal("session with a failed MCP restart was kept")
	}
	if transport.hasSession(result.SessionID) {
		t.Fatal("transport still accepts the dropped session")
	}
}

func TestEvictedSessionIsMarkedAndStopsMCP(t *testing.T) {
	r := NewSessionRegistry(0)
	r.limit = 1
	manager, err := NewSessionMCPManager(t.TempDir(), []string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	old := &sessionRecord{ID: "old", SessionKey: "old", mcp: manager}
	if _, err := r.Add(old); err != nil {
		t.Fatal(err)
	}
	if evicted, err := r.Add(&sessionRecord{ID: "new", SessionKey: "new"}); err != nil || evicted != "old" {
		t.Fatalf("Add() = %q, %v", evicted, err)
	}
	old.mu.Lock()
	removed, mcp := old.removed, old.mcp
	old.mu.Unlock()
	if !removed || mcp != nil {
		t.Fatalf("evicted session removed=%v mcp=%v; want removed and MCP stopped", removed, mcp)
	}
	manager.mu.RLock()
	closed := manager.closed
	manager.mu.RUnlock()
	if !closed {
		t.Fatal("evicted session's MCP manager is still open")
	}
}

func TestTransportRefusesSessionTheServerDropped(t *testing.T) {
	transport := NewTransport(strings.NewReader(""), io.Discard, Handler{HasSession: func(string) bool { return false }}, "test-version")
	transport.addSession("gone")
	if transport.hasSession("gone") {
		t.Fatal("transport accepted a session the server no longer holds")
	}
	transport.stateMu.Lock()
	_, kept := transport.state.sessions["gone"]
	transport.stateMu.Unlock()
	if kept {
		t.Fatal("transport kept the dropped session id")
	}
}
