package acpserver

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProtocolLimits(t *testing.T) {
	t.Parallel()

	checks := map[string]int{
		"input line":       MaxInputLineBytes,
		"output text":      MaxOutputTextChunkBytes,
		"output frame":     MaxOutputFrameBytes,
		"prompt text":      MaxPromptTextBytes,
		"active sessions":  MaxActiveSessions,
		"output queue":     MaxOutputQueue,
		"pending requests": MaxPendingRequests,
		"MCP servers":      MaxMCPServersPerSession,
		"tools":            MaxTools,
		"MCP entries":      MaxMCPEntries,
		"Gateway request":  MaxGatewayFrameBytes,
	}
	want := map[string]int{
		"input line":       1 << 20,
		"output text":      64 << 10,
		"output frame":     256 << 10,
		"prompt text":      384 << 10,
		"active sessions":  64,
		"output queue":     128,
		"pending requests": 256,
		"MCP servers":      8,
		"tools":            256,
		"MCP entries":      128,
		"Gateway request":  480 << 10,
	}
	for name, got := range checks {
		if got != want[name] {
			t.Errorf("%s limit = %d, want %d", name, got, want[name])
		}
	}
}

func TestDecodeNewSessionStructuredMCP(t *testing.T) {
	t.Parallel()

	const secret = "MCP_ENV_CANARY"
	raw := json.RawMessage(`{
		"cwd":"/tmp/project",
		"mcpServers":[{
			"name":"tools",
			"command":"/usr/bin/tools",
			"args":["--stdio","--safe"],
			"env":[{"name":"TOKEN","value":"` + secret + `"}],
			"_meta":{"client":"buzz"}
		}],
		"_meta":{"trace":"ignored"}
	}`)

	got, rpcErr := DecodeNewSessionParams(raw)
	if rpcErr != nil {
		t.Fatalf("DecodeNewSessionParams() error = %v", rpcErr)
	}
	if got.CWD != "/tmp/project" || len(got.MCPServers) != 1 {
		t.Fatalf("DecodeNewSessionParams() = %#v", got)
	}
	server := got.MCPServers[0]
	if server.Name != "tools" || server.Command != "/usr/bin/tools" {
		t.Fatalf("server identity = %#v", server)
	}
	if strings.Join(server.Args, ",") != "--stdio,--safe" {
		t.Fatalf("server args = %#v", server.Args)
	}
	if len(server.Env) != 1 || server.Env[0].Name != "TOKEN" || server.Env[0].Value != secret {
		t.Fatalf("server env = %#v", server.Env)
	}
	if strings.Contains(rpcErrString(rpcErr), secret) {
		t.Fatal("decoder error exposed an MCP environment value")
	}
}

func TestDecodeNewSessionRejectsInvalidMCP(t *testing.T) {
	t.Parallel()

	tests := []string{
		`{"cwd":"relative","mcpServers":[]}`,
		`{"cwd":"/tmp","mcpServers":[{"name":"x","command":"/bin/x","args":[],"env":[],"type":"http"}]}`,
		`{"cwd":"/tmp"}`,
		`{"cwd":"/tmp","mcpServers":[{"name":"","command":"/bin/x","args":[],"env":[]}]}`,
		`{"cwd":"/tmp","mcpServers":[{"name":"x","command":"/bin/x","env":[]}]}`,
		`{"cwd":"/tmp","mcpServers":[{"name":"x","command":"/bin/x","args":[]}]}`,
	}
	for _, raw := range tests {
		if _, rpcErr := DecodeNewSessionParams(json.RawMessage(raw)); rpcErr == nil || rpcErr.Code != CodeInvalidParams {
			t.Errorf("DecodeNewSessionParams(%s) error = %#v, want invalid params", raw, rpcErr)
		}
	}
}

func TestDecodePromptPreservesTextBlockOrder(t *testing.T) {
	t.Parallel()

	raw := json.RawMessage(`{"sessionId":"session-1","prompt":[{"type":"text","text":"first"},{"type":"text","text":"second"}]}`)
	got, rpcErr := DecodePromptParams(raw)
	if rpcErr != nil {
		t.Fatalf("DecodePromptParams() error = %v", rpcErr)
	}
	if len(got.Prompt) != 2 || got.Prompt[0].Text != "first" || got.Prompt[1].Text != "second" {
		t.Fatalf("prompt order = %#v", got.Prompt)
	}

	unsupported := json.RawMessage(`{"sessionId":"session-1","prompt":[{"type":"image","data":"AA==","mimeType":"image/png"}]}`)
	if _, rpcErr := DecodePromptParams(unsupported); rpcErr == nil || rpcErr.Code != CodeInvalidParams {
		t.Fatalf("unsupported block error = %#v, want invalid params", rpcErr)
	}
}

func TestDecodePromptHonorsSerializedGatewayBudget(t *testing.T) {
	t.Parallel()

	within := strings.Repeat("a", MaxPromptTextBytes)
	raw, err := json.Marshal(map[string]any{
		"sessionId": "session-1",
		"prompt":    []map[string]string{{"type": "text", "text": within}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, rpcErr := DecodePromptParams(raw); rpcErr != nil {
		t.Fatalf("plain prompt at decoded limit rejected: %v", rpcErr)
	}

	escaped := strings.Repeat("\n", MaxPromptTextBytes)
	raw, err = json.Marshal(map[string]any{
		"sessionId": "session-1",
		"prompt":    []map[string]string{{"type": "text", "text": escaped}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, rpcErr := DecodePromptParams(raw); rpcErr == nil || rpcErr.Code != CodeInvalidParams {
		t.Fatalf("escaped prompt error = %#v, want serialized-budget rejection", rpcErr)
	}
}

func rpcErrString(err *RPCError) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
