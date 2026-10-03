package acpserver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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
