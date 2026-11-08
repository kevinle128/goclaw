package agent

import (
	"context"
	"sync"
	"testing"

	mcpbridge "github.com/nextlevelbuilder/goclaw/internal/mcp"
	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// toolCallIDExecutor records the tool call id that the tool context holds.
type toolCallIDExecutor struct {
	stubExecutor
	mu  sync.Mutex
	ids []string
}

func (e *toolCallIDExecutor) ExecuteWithContext(ctx context.Context, _ string, _ map[string]any, _, _, _, _ string, _ tools.AsyncCallback) *tools.Result {
	e.mu.Lock()
	e.ids = append(e.ids, mcpbridge.ToolCallIDFromContext(ctx))
	e.mu.Unlock()
	return &tools.Result{ForLLM: "ok"}
}

// TestToolCallIDMatchesACPToolCallID checks that the tool context of each
// execution path holds the id of the tool.call event. The ACP adapter sends
// that event id as toolCallId, so an elicitation of the tool is scoped to the
// same call.
func TestToolCallIDMatchesACPToolCallID(t *testing.T) {
	for name, run := range map[string]func(l *Loop, req *RunRequest, tc providers.ToolCall) error{
		"sequential": func(l *Loop, req *RunRequest, tc providers.ToolCall) error {
			_, err := l.makeExecuteToolCall(req, &runState{})(context.Background(), &pipeline.RunState{RunID: req.RunID}, tc)
			return err
		},
		"parallel": func(l *Loop, req *RunRequest, tc providers.ToolCall) error {
			_, _, err := l.makeExecuteToolRaw(req)(context.Background(), tc)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			col := &eventCollector{}
			executor := &toolCallIDExecutor{}
			l := &Loop{id: "test-agent", tools: executor, onEvent: col.onEvent}
			req := &RunRequest{RunID: "run-1", ACPGeneration: 2, SessionKey: "agent:a:ws:direct:s", UserID: "u-1", Channel: "ws"}
			tc := providers.ToolCall{ID: "toolu_01ABC", Name: "mcp_task", Arguments: map[string]any{"action": "input"}}
			if err := run(l, req, tc); err != nil {
				t.Fatal(err)
			}
			calls := col.filter(protocol.AgentEventToolCall)
			if len(calls) != 1 {
				t.Fatalf("tool.call events = %d", len(calls))
			}
			payload, ok := calls[0].Payload.(map[string]any)
			if !ok {
				t.Fatalf("payload type = %T", calls[0].Payload)
			}
			if len(executor.ids) != 1 || executor.ids[0] != payload["id"] || executor.ids[0] != tc.ID {
				t.Fatalf("tool context id = %v, event id = %v", executor.ids, payload["id"])
			}
		})
	}
}
