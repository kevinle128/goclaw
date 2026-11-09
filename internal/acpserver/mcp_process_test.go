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

func TestMCPEnvironmentRedactsOnlySecretLookingValues(t *testing.T) {
	entries := []EnvVariable{
		{Name: "GIT_TERMINAL_PROMPT", Value: "0"},
		{Name: "GIT_CONFIG_COUNT", Value: "11"},
		{Name: "GIT_CONFIG_VALUE_1", Value: "nostr"},
		{Name: "GIT_CONFIG_VALUE_2", Value: "true"},
		{Name: "GIT_CONFIG_VALUE_5", Value: "x509"},
		{Name: "GIT_CONFIG_VALUE_3", Value: "/opt/buzz-goclaw/bin/mbuzz-261006/git-credential-nostr"},
		{Name: "GIT_CONFIG_VALUE_10", Value: "https://vtc.example.ts.net/git/"},
		{Name: "BUZZ_OUTPUT_FORMAT", Value: "agent"},
		{Name: "BUZZ_GIT_IDENTITY", Value: "agent"},
		{Name: "BUZZ_PRIVATE_KEY", Value: "k1"},
		{Name: "BUZZ_AUTH_TAG", Value: "tag-9"},
		{Name: "OPAQUE_VALUE", Value: "c2VjcmV0LXZhbHVlLXJhbmRvbQ"},
		{Name: "MIRROR_URL", Value: "https://user:pass@mirror.example/repo"},
	}
	environment, secrets, err := minimalMCPEnvironment(entries)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(environment, "GIT_TERMINAL_PROMPT=0") || !slices.Contains(environment, "GIT_CONFIG_VALUE_2=true") {
		t.Fatalf("every value must still reach the MCP process: %v", environment)
	}
	want := []string{"k1", "tag-9", "c2VjcmV0LXZhbHVlLXJhbmRvbQ", "https://user:pass@mirror.example/repo"}
	slices.Sort(want)
	got := slices.Clone(secrets)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("secrets = %v, want %v", got, want)
	}
	schema := map[string]any{
		"$schema":     "https://json-schema.org/draft/2020-12/schema",
		"description": "returns true for 11 agent items from /opt/buzz-goclaw/bin/mbuzz-261006/git-credential-nostr; tag tag-9",
	}
	filtered := redactSecretValues(schema, secrets).(map[string]any)
	if filtered["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		t.Fatalf("schema URI was changed: %v", filtered["$schema"])
	}
	if filtered["description"] != "returns true for 11 agent items from /opt/buzz-goclaw/bin/mbuzz-261006/git-credential-nostr; tag [REDACTED]" {
		t.Fatalf("description = %q", filtered["description"])
	}
}

func TestMCPEnvironmentAllowsClientPathOverlay(t *testing.T) {
	t.Setenv("PATH", "/inherited/bin")
	entries := []EnvVariable{{Name: "PATH", Value: "/buzz/bin:/usr/bin"}}
	environment, _, err := minimalMCPEnvironment(entries)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(environment, "PATH=/buzz/bin:/usr/bin") || slices.Contains(environment, "PATH=/inherited/bin") {
		t.Fatalf("client PATH overlay was lost: %v", environment)
	}
	entries = append(entries, EnvVariable{Name: "PATH", Value: "/other/bin"})
	if _, _, err := minimalMCPEnvironment(entries); err == nil {
		t.Fatal("duplicate client PATH was accepted")
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
