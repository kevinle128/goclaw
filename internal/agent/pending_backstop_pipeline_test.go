package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// TestPipeline_PendingBackstopSurvivesProviderOverflowWithoutGuard replays the
// production case with the spill guard off (threshold 0): the huge result
// reaches the provider once and is rejected, emergency compaction runs, and the
// pending trim must then let the run finish without running the tool again.
func TestPipeline_PendingBackstopSurvivesProviderOverflowWithoutGuard(t *testing.T) {
	const budget = 206621
	counter := quarterCounter{}
	placeholder := resolvePruningSettings(nil).hardClearPlaceholder
	toolRuns, llmCalls := 0, 0
	deps := pipeline.PipelineDeps{
		TokenCounter: counter,
		Config:       pipeline.PipelineConfig{MaxIterations: 6, CheckpointInterval: 5, ContextWindow: budget},
		PruneMessages: func(msgs []providers.Message, _ int) ([]providers.Message, pipeline.PruneStats) {
			return msgs, pipeline.PruneStats{}
		},
		CompactMessages: func(context.Context, []providers.Message, string) ([]providers.Message, error) {
			return []providers.Message{{Role: "user", Content: "[compacted summary]"}}, nil
		},
		FitHistory: func(msgs []providers.Message, b int, model string) ([]providers.Message, pipeline.FitStats) {
			return fitHistoryToBudget(msgs, b, counter, model, placeholder)
		},
		FitPending: func(msgs []providers.Message, b int, model string) ([]providers.Message, pipeline.FitStats) {
			return fitPendingToolResults(msgs, b, counter, model, placeholder)
		},
		OverBudgetMessage: func(context.Context) string { return "context too long" },
		CallLLM: func(_ context.Context, _ *pipeline.RunState, req providers.ChatRequest) (*providers.ChatResponse, error) {
			llmCalls++
			if counter.CountMessages("", req.Messages) > budget {
				return nil, errors.New("context length exceeded for this request")
			}
			if llmCalls == 1 {
				return &providers.ChatResponse{FinishReason: "tool_calls", ToolCalls: []providers.ToolCall{{ID: "tc-1", Name: "mcp_search"}}}, nil
			}
			return &providers.ChatResponse{Content: "done", FinishReason: "stop"}, nil
		},
		ExecuteToolCall: func(_ context.Context, _ *pipeline.RunState, tc providers.ToolCall) ([]providers.Message, error) {
			toolRuns++
			return []providers.Message{toolMsg(strings.Repeat("a", 5_893_076))}, nil
		},
	}
	d := &deps
	p := pipeline.NewPipeline(nil, []pipeline.Stage{
		pipeline.NewThinkStage(d), pipeline.NewPruneStage(d, nil), pipeline.NewToolStage(d),
		pipeline.NewObserveStage(d), pipeline.NewCheckpointStage(d),
	}, nil, deps)
	state := spillState("backstop")
	state.Messages.SetHistory([]providers.Message{{Role: "user", Content: "find the code"}})

	if _, err := p.Run(context.Background(), state); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if state.ExitCode != pipeline.BreakLoop || state.Observe.FinalContent != "done" {
		t.Errorf("exit=%v final=%q, want a final answer after the overflow", state.ExitCode, state.Observe.FinalContent)
	}
	if toolRuns != 1 {
		t.Errorf("tool executions = %d, want 1", toolRuns)
	}
}
