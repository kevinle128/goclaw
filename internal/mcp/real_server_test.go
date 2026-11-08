package mcp

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

// TestMCPGoServerInterop runs the goclaw client against the mcp-go server
// implementation, so the client is not checked only against the stub that
// was written with it.
func TestMCPGoServerInterop(t *testing.T) {
	srv := mcpserver.NewMCPServer("interop", "1.0.0", mcpserver.WithToolCapabilities(false))
	srv.AddTool(mcpgo.NewTool("echo", mcpgo.WithDescription("Echo")),
		func(context.Context, mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
			return mcpgo.NewToolResultText("interop-ok"), nil
		})
	httpSrv := httptest.NewServer(mcpserver.NewStreamableHTTPServer(srv))
	t.Cleanup(httpSrv.Close)
	url := httpSrv.URL + "/mcp"

	for _, protocol := range []ProtocolMode{ProtocolAuto, ProtocolLegacy} {
		t.Run(string(protocol), func(t *testing.T) {
			ss, mcpTools, err := connectAndDiscover(context.Background(), "interop", "streamable-http", "", nil, nil, url, nil, 5, protocol)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			t.Cleanup(func() { _ = ss.client.Close() })
			t.Logf("protocol=%s era=%s version=%s session=%q", protocol, ss.era(), ss.client.ProtocolVersion(), ss.client.GetSessionId())
			if protocol == ProtocolLegacy && (ss.era() != EraLegacy || ss.client.GetSessionId() == "") {
				t.Errorf("legacy setting: era %v, session %q; want legacy with a session", ss.era(), ss.client.GetSessionId())
			}
			if len(mcpTools) != 1 {
				t.Fatalf("tools = %v", mcpTools)
			}
			bt := NewBridgeTool("interop", mcpTools[0], &ss.clientPtr, "", 5, &ss.connected, uuid.Nil, nil)
			got := bt.Execute(context.Background(), nil)
			if got.IsError || !strings.Contains(got.ForLLM, "interop-ok") {
				t.Errorf("tools/call result = %+v", got)
			}
			if err := checkLiveness(context.Background(), ss.client); err != nil {
				t.Errorf("liveness: %v", err)
			}
		})
	}
}
