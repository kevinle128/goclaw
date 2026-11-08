package acpserver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestServerNewSessionUsesCanonicalOpaqueKey(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "project")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{Gateway: NewGatewayClient(GatewayClientConfig{}), AgentID: "fixed", WorkspaceRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	result, err := server.handleNewSession(context.Background(), NewSessionParams{CWD: cwd, MCPServers: []MCPServer{}})
	if err != nil {
		t.Fatal(err)
	}
	record, ok := server.sessions.Get(result.SessionID)
	if !ok {
		t.Fatal("session missing")
	}
	if record.SessionKey == result.SessionID || record.SessionKey != "agent:fixed:ws:direct:"+result.SessionID {
		t.Fatalf("session key = %q", record.SessionKey)
	}
}

func TestServerRejectsWorkspaceEscape(t *testing.T) {
	root := t.TempDir()
	sibling := t.TempDir()
	server, err := NewServer(ServerConfig{Gateway: NewGatewayClient(GatewayClientConfig{}), AgentID: "fixed", WorkspaceRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.handleNewSession(context.Background(), NewSessionParams{CWD: sibling}); err == nil {
		t.Fatal("workspace escape accepted")
	}
	if server.sessions.Len() != 0 {
		t.Fatal("rejected workspace created a session")
	}
}

func TestServerNewSessionStoresThreadContext(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "project")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{Gateway: NewGatewayClient(GatewayClientConfig{}), AgentID: "fixed", WorkspaceRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	thread := []byte(`{"channelId":"c1"}`)
	result, err := server.handleNewSession(context.Background(), NewSessionParams{CWD: cwd, MCPServers: []MCPServer{}, Thread: thread})
	if err != nil {
		t.Fatal(err)
	}
	thread[0] = 'x'
	_, generation, _, err := server.sessions.BeginPrompt(result.SessionID, context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.sessions.FinishPrompt(result.SessionID, generation)
	got, ok := server.PromptContext(result.SessionID, generation)
	if !ok || got.SessionID != result.SessionID || string(got.Thread) != `{"channelId":"c1"}` {
		t.Fatalf("PromptContext() = %#v, %v", got, ok)
	}
}

func TestServerValidatesMaxPromptDuration(t *testing.T) {
	root := t.TempDir()
	for _, duration := range []time.Duration{-time.Minute, time.Second, 169 * time.Hour} {
		if _, err := NewServer(ServerConfig{Gateway: NewGatewayClient(GatewayClientConfig{}), AgentID: "fixed", WorkspaceRoot: root, MaxPromptDuration: duration}); err == nil {
			t.Errorf("NewServer() accepted max prompt duration %v", duration)
		}
	}
	server, err := NewServer(ServerConfig{Gateway: NewGatewayClient(GatewayClientConfig{}), AgentID: "fixed", WorkspaceRoot: root, MaxPromptDuration: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if server.sessions.maxPrompt != 24*time.Hour {
		t.Fatalf("registry max prompt duration = %v", server.sessions.maxPrompt)
	}
}
