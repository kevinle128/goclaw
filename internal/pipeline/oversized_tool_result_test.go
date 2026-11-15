package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// quarterCounter counts one token per four bytes of content, so a result of
// 5,893,076 chars is the 1,473,269 tokens seen in production.
type quarterCounter struct{}

func (quarterCounter) Count(_ string, text string) int { return len(text) / 4 }
func (quarterCounter) CountMessages(_ string, msgs []providers.Message) int {
	n := 0
	for _, m := range msgs {
		n += len(m.Content) / 4
	}
	return n
}
func (quarterCounter) CountToolSchemas(_ string, _ []providers.ToolDefinition) int { return 0 }
func (quarterCounter) ModelContextWindow(_ string) int                             { return 206621 }

const (
	// productionHistoryBudget and productionToolResultChars come from the
	// 2026-11-15 run that logged "still over budget after compaction
	// tokens=1473269 budget=206621".
	productionHistoryBudget   = 206621
	productionToolResultChars = 5_893_076
)

// overflowPipeline runs the real iteration stages against a scripted provider.
// The provider answers like the production route: a request above the context
// window fails with a context overflow error.
type overflowPipeline struct {
	deps         PipelineDeps
	toolRuns     atomic.Int32
	llmCalls     atomic.Int32
	maxToolBytes int // largest tool message the provider received
}

func newOverflowPipeline(toolResult string) *overflowPipeline {
	o := &overflowPipeline{}
	counter := quarterCounter{}
	o.deps = PipelineDeps{
		TokenCounter: counter,
		Config: PipelineConfig{
			MaxIterations:      6,
			CheckpointInterval: 5,
			ContextWindow:      productionHistoryBudget,
		},
		PruneMessages: func(msgs []providers.Message, _ int) ([]providers.Message, PruneStats) {
			return msgs, PruneStats{}
		},
		CompactMessages: func(_ context.Context, _ []providers.Message, _ string) ([]providers.Message, error) {
			return []providers.Message{{Role: "user", Content: "[compacted summary]"}}, nil
		},
		OverBudgetMessage: func(context.Context) string { return "context too long" },
		CallLLM: func(_ context.Context, _ *RunState, req providers.ChatRequest) (*providers.ChatResponse, error) {
			n := o.llmCalls.Add(1)
			for _, m := range req.Messages {
				if m.Role == "tool" && len(m.Content) > o.maxToolBytes {
					o.maxToolBytes = len(m.Content)
				}
			}
			if counter.CountMessages("", req.Messages) > productionHistoryBudget {
				return nil, errors.New("context length exceeded for this request")
			}
			if n == 1 {
				return &providers.ChatResponse{
					FinishReason: "tool_calls",
					ToolCalls:    []providers.ToolCall{{ID: "tc-1", Name: "mcp_search_code", Arguments: map[string]any{}}},
				}, nil
			}
			return &providers.ChatResponse{Content: "done", FinishReason: "stop"}, nil
		},
		ExecuteToolCall: func(_ context.Context, _ *RunState, tc providers.ToolCall) ([]providers.Message, error) {
			o.toolRuns.Add(1)
			return []providers.Message{{Role: "tool", ToolCallID: tc.ID, Content: toolResult}}, nil
		},
	}
	return o
}

func (o *overflowPipeline) run(t *testing.T) *RunState {
	t.Helper()
	d := &o.deps
	p := NewPipeline(nil, []Stage{
		NewThinkStage(d), NewPruneStage(d, nil), NewToolStage(d), NewObserveStage(d), NewCheckpointStage(d),
	}, nil, o.deps)
	state := defaultState()
	state.Messages.SetHistory([]providers.Message{{Role: "user", Content: "search the code"}})
	if _, err := p.Run(context.Background(), state); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	return state
}

// TestPipeline_OversizedToolResult_AbortsWithoutGuard records the production
// failure: a tool result of about 1.47M tokens is sent by the next ThinkStage
// before PruneStage counts it, the provider rejects it, and compaction cannot
// shrink a pending result, so the run aborts.
func TestPipeline_OversizedToolResult_AbortsWithoutGuard(t *testing.T) {
	t.Parallel()
	o := newOverflowPipeline(strings.Repeat("x", productionToolResultChars))
	state := o.run(t)

	if state.Prune.HistoryBudget != productionHistoryBudget {
		t.Fatalf("HistoryBudget = %d, want %d", state.Prune.HistoryBudget, productionHistoryBudget)
	}
	if state.ExitCode != AbortRun {
		t.Fatalf("ExitCode = %v, want AbortRun (still over budget after compaction)", state.ExitCode)
	}
	if state.Observe.FinalContent != "context too long" {
		t.Errorf("FinalContent = %q, want the over-budget reply", state.Observe.FinalContent)
	}
}

// TestPipeline_OversizedToolResult_BoundBeforeNextLLMCall shows that a guard at
// the ToolStage append keeps the run alive: the provider never sees the huge
// result and the run reaches its final answer.
func TestPipeline_OversizedToolResult_BoundBeforeNextLLMCall(t *testing.T) {
	t.Parallel()
	o := newOverflowPipeline(strings.Repeat("x", productionToolResultChars))
	o.deps.BoundToolResult = func(_ context.Context, _ *RunState, _ providers.ToolCall, msg providers.Message) providers.Message {
		msg.Content = msg.Content[:1000] + "\n[Tool result too large]"
		return msg
	}
	state := o.run(t)

	if state.ExitCode != BreakLoop {
		t.Fatalf("ExitCode = %v, want BreakLoop (final answer)", state.ExitCode)
	}
	if state.Observe.FinalContent != "done" {
		t.Errorf("FinalContent = %q, want %q", state.Observe.FinalContent, "done")
	}
	if o.maxToolBytes > 2000 {
		t.Errorf("provider received a tool message of %d bytes, want it bounded", o.maxToolBytes)
	}
	if got := o.llmCalls.Load(); got != 2 {
		t.Errorf("LLM calls = %d, want 2 (no overflow retry)", got)
	}
}

// TestToolStage_BoundToolResult_OnlyToolMessagesOnBothPaths checks that the guard
// sees tool messages from the sequential and the parallel path, and nothing else.
func TestToolStage_BoundToolResult_OnlyToolMessagesOnBothPaths(t *testing.T) {
	t.Parallel()
	for _, parallel := range []bool{false, true} {
		name := "sequential"
		if parallel {
			name = "parallel"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var seen atomic.Int32
			deps := &PipelineDeps{
				BoundToolResult: func(_ context.Context, _ *RunState, tc providers.ToolCall, msg providers.Message) providers.Message {
					seen.Add(1)
					msg.Content = "bounded:" + tc.ID
					return msg
				},
				ExecuteToolCall: func(_ context.Context, _ *RunState, tc providers.ToolCall) ([]providers.Message, error) {
					return []providers.Message{
						{Role: "tool", ToolCallID: tc.ID, Content: "raw"},
						{Role: "user", Content: "not a tool message"},
					}, nil
				},
			}
			if parallel {
				deps.ExecuteToolRaw = func(_ context.Context, tc providers.ToolCall) (providers.Message, any, error) {
					return providers.Message{Role: "tool", ToolCallID: tc.ID, Content: "raw"}, nil, nil
				}
				deps.ProcessToolResult = func(_ context.Context, _ *RunState, tc providers.ToolCall, raw providers.Message, _ any) []providers.Message {
					return []providers.Message{raw, {Role: "user", Content: "not a tool message"}}
				}
				deps.ParallelEligibleToolCall = func(providers.ToolCall) bool { return true }
			}
			state := defaultState()
			state.Think.LastResponse = &providers.ChatResponse{ToolCalls: []providers.ToolCall{
				{ID: "a", Name: "mcp_x"}, {ID: "b", Name: "mcp_y"},
			}}
			if err := NewToolStage(deps).Execute(context.Background(), state); err != nil {
				t.Fatalf("Execute() error: %v", err)
			}
			if got := seen.Load(); got != 2 {
				t.Errorf("guard calls = %d, want 2 (one per tool message)", got)
			}
			var tools, others int
			for _, m := range state.Messages.Pending() {
				switch m.Role {
				case "tool":
					tools++
					if !strings.HasPrefix(m.Content, "bounded:") {
						t.Errorf("tool message %q was not bounded", m.Content)
					}
				default:
					others++
					if m.Content != "not a tool message" {
						t.Errorf("non-tool message was changed: %q", m.Content)
					}
				}
			}
			if tools != 2 || others != 2 {
				t.Errorf("pending has %d tool and %d other messages, want 2 and 2", tools, others)
			}
		})
	}
}
