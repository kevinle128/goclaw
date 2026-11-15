package agent

import (
	"context"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// lateInjectStage injects one message through the router right after the
// observe stage, which is the window a web UI or channel follow-up lands in
// when it arrives while the final answer is being checkpointed.
type lateInjectStage struct {
	router *Router
	done   bool
}

func (s *lateInjectStage) Name() string { return "late-inject" }

func (s *lateInjectStage) Execute(context.Context, *pipeline.RunState) error {
	if s.done {
		return nil
	}
	s.done = true
	s.router.InjectMessage("web-session", InjectedMessage{Content: "and also check the second file"})
	return nil
}

// TestPipeline_FollowUpInjectedAfterFinalAnswerIsAnswered runs the real inject
// channel wiring of the loop (the path shared by the web UI and channel
// consumers): a follow-up accepted by Router.InjectMessage after the last
// observe stage must be answered, not acked and dropped.
func TestPipeline_FollowUpInjectedAfterFinalAnswerIsAnswered(t *testing.T) {
	router := NewRouter()
	injectCh := router.RegisterRun(context.Background(), "run-late", "web-session", "agent-1", func() {})
	defer router.UnregisterRun("run-late")

	l := &Loop{maxIterations: 5}
	req := &RunRequest{RunID: "run-late", SessionKey: "web-session", InjectCh: injectCh}
	deps := l.buildPipelineDeps(req, &runState{})

	// The model needs no tools here; only the inject channel wiring is real.
	deps.BuildFilteredTools = nil
	var calls [][]providers.Message
	deps.CallLLM = func(_ context.Context, _ *pipeline.RunState, r providers.ChatRequest) (*providers.ChatResponse, error) {
		calls = append(calls, append([]providers.Message(nil), r.Messages...))
		if len(calls) == 1 {
			return &providers.ChatResponse{Content: "first answer", FinishReason: "stop"}, nil
		}
		return &providers.ChatResponse{Content: "answer for both", FinishReason: "stop"}, nil
	}

	d := &deps
	p := pipeline.NewPipeline(nil, []pipeline.Stage{
		pipeline.NewThinkStage(d), pipeline.NewObserveStage(d), &lateInjectStage{router: router},
	}, nil, deps)
	state := pipeline.NewRunState(&pipeline.RunInput{SessionKey: "web-session", RunID: "run-late"}, nil, "test-model", nil)

	result, err := p.Run(context.Background(), state)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("model calls = %d, want 2 (the accepted follow-up must be answered)", len(calls))
	}
	last := calls[1][len(calls[1])-1]
	if last.Role != "user" || last.Content != "and also check the second file" {
		t.Fatalf("last message of second call = %#v, want the injected follow-up", last)
	}
	if result.Content != "answer for both" {
		t.Fatalf("result content = %q, want %q", result.Content, "answer for both")
	}
}
