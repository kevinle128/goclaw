package pipeline

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// pendingBackstopDeps builds PruneStage deps for a run whose pending buffer
// alone is larger than the whole history budget (budget = 10,000 tokens).
func pendingBackstopDeps(fitPending func([]providers.Message, int, string) ([]providers.Message, FitStats)) *PipelineDeps {
	return &PipelineDeps{
		Config:       PipelineConfig{ContextWindow: 10000},
		TokenCounter: quarterCounter{},
		PruneMessages: func(msgs []providers.Message, _ int) ([]providers.Message, PruneStats) {
			return msgs, PruneStats{}
		},
		CompactMessages: func(_ context.Context, msgs []providers.Message, _ string) ([]providers.Message, error) {
			return msgs, nil
		},
		FitHistory: func(msgs []providers.Message, _ int, _ string) ([]providers.Message, FitStats) {
			return msgs, FitStats{}
		},
		FitPending:        fitPending,
		OverBudgetMessage: func(context.Context) string { return "context too long" },
	}
}

func pendingBackstopState() (*RunState, providers.Message) {
	state := defaultState()
	state.Messages.SetHistory([]providers.Message{{Role: "user", Content: "search the code"}})
	assistant := providers.Message{
		Role:      "assistant",
		Content:   "searching",
		ToolCalls: []providers.ToolCall{{ID: "tc-1", Name: "mcp_search", Arguments: map[string]any{"q": "x"}}},
	}
	state.Messages.AppendPending(assistant)
	state.Messages.AppendPending(providers.Message{Role: "tool", ToolCallID: "tc-1", Content: strings.Repeat("r", 200_000)}) // 50,000 tokens
	return state, assistant
}

func TestPruneStage_PendingAloneOverBudget_TrimsPendingToolResults(t *testing.T) {
	t.Parallel()
	deps := pendingBackstopDeps(func(msgs []providers.Message, budget int, _ string) ([]providers.Message, FitStats) {
		out := make([]providers.Message, len(msgs))
		copy(out, msgs)
		for i, m := range out {
			if m.Role == "tool" && len(m.Content) > 4000 {
				out[i].Content = m.Content[:2000] + "\n...\n" + m.Content[len(m.Content)-2000:]
			}
		}
		return out, FitStats{ResultsTrimmed: 1}
	})
	stage := NewPruneStage(deps, nil)
	state, assistant := pendingBackstopState()

	if err := stage.Execute(context.Background(), state); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if stage.Result() != Continue {
		t.Fatalf("Result() = %v, want Continue", stage.Result())
	}
	pending := state.Messages.Pending()
	if len(pending) != 2 {
		t.Fatalf("pending has %d messages, want 2 (no message dropped)", len(pending))
	}
	if !reflect.DeepEqual(pending[0], assistant) {
		t.Errorf("assistant tool_calls message changed: %+v", pending[0])
	}
	if pending[1].Role != "tool" || pending[1].ToolCallID != "tc-1" {
		t.Errorf("tool message lost its pairing: %+v", pending[1])
	}
	if total := (quarterCounter{}).CountMessages("", append(state.Messages.History(), pending...)); total > state.Prune.HistoryBudget {
		t.Errorf("total tokens %d exceed the budget %d", total, state.Prune.HistoryBudget)
	}
}

func TestPruneStage_PendingAloneOverBudget_WithoutFitPendingAborts(t *testing.T) {
	t.Parallel()
	stage := NewPruneStage(pendingBackstopDeps(nil), nil)
	state, _ := pendingBackstopState()

	if err := stage.Execute(context.Background(), state); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if stage.Result() != AbortRun {
		t.Fatalf("Result() = %v, want AbortRun (documents the behavior without the backstop)", stage.Result())
	}
}

func TestPruneStage_PendingUnderHalfBudget_IsNotTrimmed(t *testing.T) {
	t.Parallel()
	called := false
	deps := pendingBackstopDeps(func(msgs []providers.Message, _ int, _ string) ([]providers.Message, FitStats) {
		called = true
		return msgs, FitStats{}
	})
	// History is what pushes the total over the budget; pending stays small.
	stage := NewPruneStage(deps, nil)
	state := defaultState()
	state.Messages.SetHistory([]providers.Message{{Role: "user", Content: strings.Repeat("h", 50_000)}}) // 12,500 tokens
	state.Messages.AppendPending(providers.Message{Role: "tool", ToolCallID: "tc-1", Content: "small"})

	if err := stage.Execute(context.Background(), state); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if called {
		t.Error("FitPending ran although pending is far below half of the budget")
	}
}
