package acpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestMCPProcessSurvivesSessionNewContext(t *testing.T) {
	if os.Getenv("ACP_TEST_MCP_HELPER") == "1" {
		runMCPTestHelper()
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	manager, err := NewSessionMCPManager(workspace, []string{filepath.Dir(executable)})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	ctx, cancel := context.WithCancel(context.Background())
	tools, err := manager.Start(ctx, workspace, []MCPServer{{
		Name: "helper", Command: executable,
		Args: []string{"-test.run=^TestMCPProcessSurvivesSessionNewContext$"},
		Env:  []EnvVariable{{Name: "ACP_TEST_MCP_HELPER", Value: "1"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	if len(tools) != 1 || tools[0].Name != "mcp_helper__echo" {
		t.Fatalf("tools = %#v", tools)
	}
	content, isError, err := manager.Call(context.Background(), tools[0].Name, map[string]any{"text": "hi"})
	if err != nil || isError || content == "" {
		t.Fatalf("Call() = %q, %v, %v", content, isError, err)
	}
}

func runMCPTestHelper() {
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
		}
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			continue
		}
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "helper", "version": "1"}}
		case "tools/list":
			result = map[string]any{"tools": []any{map[string]any{"name": "echo", "description": "echo", "inputSchema": map[string]any{"type": "object"}}}}
		case "tools/call":
			result = map[string]any{"content": []any{map[string]string{"type": "text", "text": "ok"}}, "isError": false}
		default:
			continue
		}
		_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
	}
}

func TestMCPEnvironmentDoesNotInheritHostSecrets(t *testing.T) {
	t.Setenv("GOCLAW_GATEWAY_TOKEN", "host-secret")
	environment, secrets, err := minimalMCPEnvironment([]EnvVariable{{Name: "BUZZ_PRIVATE_KEY", Value: "session-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(environment, "GOCLAW_GATEWAY_TOKEN=host-secret") {
		t.Fatal("host Gateway token was inherited")
	}
	if !slices.Contains(environment, "BUZZ_PRIVATE_KEY=session-secret") || !slices.Contains(secrets, "session-secret") {
		t.Fatalf("accepted overlay was lost: %v", environment)
	}
	if _, _, err := minimalMCPEnvironment([]EnvVariable{{Name: "GOCLAW_ACP_API_KEY", Value: "leak"}}); err == nil {
		t.Fatal("protected adapter credential was accepted")
	}
}

func TestMCPExecutableRequiresApprovedSecureRoot(t *testing.T) {
	root := t.TempDir()
	workspace := t.TempDir()
	manager, err := NewSessionMCPManager(workspace, []string{root})
	if err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(root, "server")
	if err := os.WriteFile(executable, []byte("probe"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.resolveExecutable(executable); err != nil {
		t.Fatalf("secure executable rejected: %v", err)
	}
	if err := os.Chmod(executable, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.resolveExecutable(executable); err == nil {
		t.Fatal("writable executable was accepted")
	}
	outside := filepath.Join(t.TempDir(), "server")
	if err := os.WriteFile(outside, []byte("probe"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.resolveExecutable(outside); err == nil {
		t.Fatal("executable outside the approved root was accepted")
	}
}

func TestMCPDiscoveryRejectsSecretSchemaKeyAndRedactsValues(t *testing.T) {
	_, err := redactMCPDiscovery(map[string]any{"secret-value": map[string]any{}}, []string{"secret-value"})
	if err == nil {
		t.Fatal("secret schema key was accepted")
	}
	filtered, err := redactMCPDiscovery(map[string]any{"description": "before secret-value after"}, []string{"secret-value"})
	if err != nil {
		t.Fatal(err)
	}
	got := filtered.(map[string]any)["description"]
	if got != "before [REDACTED] after" {
		t.Fatalf("description = %q", got)
	}
}

func TestMCPWorkspaceRejectsEscape(t *testing.T) {
	root := t.TempDir()
	manager, err := NewSessionMCPManager(root, []string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.resolveWorkspace(t.TempDir()); err == nil {
		t.Fatal("workspace escape was accepted")
	}
	if !errors.Is(manager.Close(), nil) {
		t.Fatal("empty manager close failed")
	}
}
