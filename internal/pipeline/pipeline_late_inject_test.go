package pipeline

import (
	"context"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// lateInjectDeps returns deps whose model answers "answer N" on call N and whose
// inject channel stays empty during the observe drain of the first iteration,
// then yields one message on the drain that follows the last observe.
func lateInjectDeps(maxIterations int, calls *[][]providers.Message, lateMessages func(drainCall int) []providers.Message) PipelineDeps {
	deps := PipelineDeps{
		Config: PipelineConfig{MaxIterations: maxIterations, MaxTokens: 1000},
	}
	deps.CallLLM = func(_ context.Context, _ *RunState, req providers.ChatRequest) (*providers.ChatResponse, error) {
		*calls = append(*calls, append([]providers.Message(nil), req.Messages...))
		return &providers.ChatResponse{Content: "answer " + string(rune('0'+len(*calls))), FinishReason: "stop"}, nil
	}
	drains := 0
	deps.DrainInjectCh = func() []providers.Message {
		drains++
		return lateMessages(drains)
	}
	return deps
}

func runLateInjectPipeline(t *testing.T, deps PipelineDeps) *RunResult {
	t.Helper()
	p := NewPipeline(nil, []Stage{NewThinkStage(&deps), NewObserveStage(&deps)}, nil, deps)
	result, err := p.Run(context.Background(), buildMinimalRunState())
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	return result
}

func TestPipelineRun_InjectAfterFinalAnswerContinuesRun(t *testing.T) {
	t.Parallel()
	var calls [][]providers.Message
	deps := lateInjectDeps(5, &calls, func(drainCall int) []providers.Message {
		// Drain 1 is the observe drain (empty). Drain 2 is the late drain: the
		// message landed after the last observe.
		if drainCall == 2 {
			return []providers.Message{{Role: "user", Content: "late follow-up"}}
		}
		return nil
	})

	result := runLateInjectPipeline(t, deps)

	if len(calls) != 2 {
		t.Fatalf("model calls = %d, want 2 (the follow-up must be answered)", len(calls))
	}
	second := calls[1]
	if len(second) < 2 {
		t.Fatalf("second call messages = %#v", second)
	}
	last, prev := second[len(second)-1], second[len(second)-2]
	if last.Role != "user" || last.Content != "late follow-up" {
		t.Fatalf("last message = %#v, want the late follow-up as a user message", last)
	}
	if prev.Role != "assistant" || prev.Content != "answer 1" || !prev.Transient {
		t.Fatalf("message before follow-up = %#v, want the first answer as a transient assistant message", prev)
	}
	if result.Content != "answer 2" {
		t.Fatalf("result content = %q, want the answer to the follow-up", result.Content)
	}
}

func TestPipelineRun_NoLateInjectFinishesNormally(t *testing.T) {
	t.Parallel()
	var calls [][]providers.Message
	deps := lateInjectDeps(5, &calls, func(int) []providers.Message { return nil })

	result := runLateInjectPipeline(t, deps)

	if len(calls) != 1 {
		t.Fatalf("model calls = %d, want 1", len(calls))
	}
	if result.Content != "answer 1" {
		t.Fatalf("result content = %q, want %q", result.Content, "answer 1")
	}
}

func TestPipelineRun_LateInjectRespectsMaxIterations(t *testing.T) {
	t.Parallel()
	var calls [][]providers.Message
	// Every late drain yields a message; the run must still stop at MaxIterations.
	deps := lateInjectDeps(2, &calls, func(drainCall int) []providers.Message {
		if drainCall%2 == 0 {
			return []providers.Message{{Role: "user", Content: "again"}}
		}
		return nil
	})

	runLateInjectPipeline(t, deps)

	if len(calls) > 2 {
		t.Fatalf("model calls = %d, want at most MaxIterations (2)", len(calls))
	}
}

func TestPipelineRun_LateInjectSkippedAfterCancel(t *testing.T) {
	t.Parallel()
	var calls [][]providers.Message
	ctx, cancel := context.WithCancel(context.Background())
	deps := lateInjectDeps(5, &calls, func(drainCall int) []providers.Message {
		if drainCall == 2 {
			t.Errorf("late drain ran after the run context was cancelled")
		}
		return nil
	})
	inner := deps.CallLLM
	deps.CallLLM = func(c context.Context, s *RunState, r providers.ChatRequest) (*providers.ChatResponse, error) {
		resp, err := inner(c, s, r)
		cancel()
		return resp, err
	}
	p := NewPipeline(nil, []Stage{NewThinkStage(&deps), NewObserveStage(&deps)}, nil, deps)
	if _, err := p.Run(ctx, buildMinimalRunState()); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
}

func TestPipelineRun_LateInjectIgnoredWhenRunStoppedOnToolCalls(t *testing.T) {
	t.Parallel()
	drains := 0
	deps := PipelineDeps{Config: PipelineConfig{MaxIterations: 5}}
	deps.DrainInjectCh = func() []providers.Message {
		drains++
		return []providers.Message{{Role: "user", Content: "late"}}
	}
	stopper := newMockStageWithResult("stopper", BreakLoop)
	stopper.execFn = func(_ context.Context, state *RunState) error {
		state.Think.LastResponse = &providers.ChatResponse{ToolCalls: []providers.ToolCall{{ID: "t1", Name: "x"}}}
		return nil
	}
	p := NewPipeline(nil, []Stage{stopper}, nil, deps)
	if _, err := p.Run(context.Background(), buildMinimalRunState()); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if drains != 0 || stopper.execCnt != 1 {
		t.Fatalf("late drains=%d iterations=%d, want 0 and 1: a run stopped on tool calls must not resume", drains, stopper.execCnt)
	}
}
