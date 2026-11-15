package agent

import (
	"context"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// TestPipeline_SteerDuringToolCallReachesNextThinkAfterToolResult runs the
// real inject wiring and the real think, tool, observe and checkpoint stages.
// A steer arrives (through the router call chat.steer uses) while a tool is
// running. The next model call must see it as a user message after the tool
// result, so the assistant tool call and its result stay adjacent, and the
// persisted history keeps the same order.
func TestPipeline_SteerDuringToolCallReachesNextThinkAfterToolResult(t *testing.T) {
	router := NewRouter()
	injectCh := router.RegisterRun(context.Background(), "run-steer", "steer-session", "agent-1", func() {})
	defer router.UnregisterRun("run-steer")

	l := &Loop{maxIterations: 6}
	req := &RunRequest{RunID: "run-steer", SessionKey: "steer-session", InjectCh: injectCh}
	deps := l.buildPipelineDeps(req, &runState{})
	deps.BuildFilteredTools = nil

	var calls [][]providers.Message
	deps.CallLLM = func(_ context.Context, _ *pipeline.RunState, r providers.ChatRequest) (*providers.ChatResponse, error) {
		calls = append(calls, append([]providers.Message(nil), r.Messages...))
		if len(calls) == 1 {
			return &providers.ChatResponse{FinishReason: "tool_calls", ToolCalls: []providers.ToolCall{{ID: "tc-1", Name: "slow_tool"}}}, nil
		}
		return &providers.ChatResponse{Content: "done with both", FinishReason: "stop"}, nil
	}
	deps.ExecuteToolCall = func(_ context.Context, _ *pipeline.RunState, tc providers.ToolCall) ([]providers.Message, error) {
		outcome := router.InjectMessageDetailed("steer-session", InjectedMessage{Content: "also look at file B"})
		if !outcome.Injected {
			t.Errorf("steer refused while a tool ran: %+v", outcome)
		}
		return []providers.Message{{Role: "tool", ToolCallID: tc.ID, Content: "tool output"}}, nil
	}
	d := &deps
	p := pipeline.NewPipeline(nil, []pipeline.Stage{
		pipeline.NewThinkStage(d), pipeline.NewToolStage(d), pipeline.NewObserveStage(d), pipeline.NewCheckpointStage(d),
	}, nil, deps)
	state := pipeline.NewRunState(&pipeline.RunInput{SessionKey: "steer-session", RunID: "run-steer"}, nil, "test-model", nil)
	state.Messages.SetHistory([]providers.Message{{Role: "user", Content: "check file A"}})

	result, err := p.Run(context.Background(), state)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("model calls = %d, want 2", len(calls))
	}
	second := calls[1]
	n := len(second)
	if n < 4 {
		t.Fatalf("second call messages = %#v", second)
	}
	if second[n-3].Role != "assistant" || len(second[n-3].ToolCalls) != 1 ||
		second[n-2].Role != "tool" || second[n-2].ToolCallID != second[n-3].ToolCalls[0].ID ||
		second[n-1].Role != "user" || second[n-1].Content != "also look at file B" {
		t.Fatalf("tail of second call = %#v, want assistant tool call, its tool result, then the steer", second[n-3:])
	}
	if result.Content != "done with both" {
		t.Fatalf("result content = %q", result.Content)
	}
	// The run's own messages (history after the user turn, plus pending) keep
	// the steer behind the complete tool call group.
	own := append(append([]providers.Message(nil), state.Messages.History()[1:]...), state.Messages.Pending()...)
	var order []string
	for _, m := range own {
		order = append(order, m.Role)
	}
	if len(order) < 3 || order[0] != "assistant" || order[1] != "tool" || order[2] != "user" {
		t.Fatalf("run message roles = %v, want assistant, tool, user first", order)
	}
}
