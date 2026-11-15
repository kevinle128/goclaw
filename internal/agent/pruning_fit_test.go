package agent

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/tokencount"
)

// TestPruneStage_FewHugeMessages_FitsInsteadOfAbort reproduces a session with
// few but huge messages: soft prune protects the recent tool results and
// compaction cannot split the single tool chain, so before the deterministic
// fit the run aborted with no reply.
func TestPruneStage_FewHugeMessages_FitsInsteadOfAbort(t *testing.T) {
	cap := &capturingProvider{response: "summary"}
	loop := &Loop{provider: cap, model: "claude-3-5-sonnet"}
	tc := tokencount.NewTiktokenCounter()

	history := singleTurnToolChain(6, strings.Repeat("log line with some words 12345\n", 2000))
	newest := history[len(history)-1].Content

	deps := &pipeline.PipelineDeps{
		Config:          pipeline.PipelineConfig{ContextWindow: 20000, MaxTokens: 1000},
		TokenCounter:    tc,
		PruneMessages:   loop.makePruneMessages(),
		SanitizeHistory: sanitizeHistory,
		CompactMessages: loop.makeCompactMessages(nil),
		FitHistory: func(msgs []providers.Message, budget int, model string) ([]providers.Message, pipeline.FitStats) {
			return fitHistoryToBudget(msgs, budget, tc, model, defaultHardClearPlaceholder)
		},
		OverBudgetMessage: func(context.Context) string { return "too long" },
	}
	state := pipeline.NewRunState(&pipeline.RunInput{SessionKey: "s"}, nil, loop.model, nil)
	state.Messages.SetHistory(history)

	stage := pipeline.NewPruneStage(deps, nil)
	if err := stage.Execute(context.Background(), state); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stage.Result() != pipeline.Continue {
		t.Fatalf("Result() = %v, want Continue (final content %q)", stage.Result(), state.Observe.FinalContent)
	}
	got := state.Messages.History()
	if tokens := tc.CountMessages(loop.model, got); tokens > state.Prune.HistoryBudget {
		t.Fatalf("history tokens %d still over budget %d", tokens, state.Prune.HistoryBudget)
	}
	if last := got[len(got)-1]; last.Role != "user" || last.Content != newest {
		t.Fatalf("newest user message changed: %+v", last)
	}
	assertToolPairing(t, got)
}

// TestFitHistoryToBudget_DropsOldestWhenBodiesStillTooLarge checks the last
// pass: when old user bodies alone exceed the budget, the oldest turns are
// dropped and the kept history starts at a user message.
func TestFitHistoryToBudget_DropsOldestWhenBodiesStillTooLarge(t *testing.T) {
	big := strings.Repeat("word ", 1000)
	var msgs []providers.Message
	for i := 0; i < 8; i++ {
		msgs = append(msgs,
			providers.Message{Role: "user", Content: big},
			providers.Message{Role: "assistant", Content: "ok"},
		)
	}
	msgs = append(msgs, providers.Message{Role: "user", Content: "newest " + big})

	out, stats := fitHistoryToBudget(msgs, 2500, nil, "", defaultHardClearPlaceholder)
	if stats.Dropped == 0 {
		t.Fatalf("expected dropped messages, stats=%+v", stats)
	}
	if out[0].Role != "user" {
		t.Fatalf("kept history starts with %q, want user", out[0].Role)
	}
	if last := out[len(out)-1]; !strings.HasPrefix(last.Content, "newest ") || len(last.Content) != len("newest ")+len(big) {
		t.Fatalf("newest user message was changed")
	}
}

// TestPruneStage_RecordedHistory runs the prune stage on a history exported
// from a real session (JSON array of providers.Message). The data is not in
// the repository because it holds internal logs and code; set
// GOCLAW_FIT_HISTORY to the file path to run it. An exported history can hold
// less than the live one did, so the budget keeps the ratio of the production
// run that aborted (211204 history tokens for a 152230 budget).
func TestPruneStage_RecordedHistory(t *testing.T) {
	path := os.Getenv("GOCLAW_FIT_HISTORY")
	if path == "" {
		t.Skip("GOCLAW_FIT_HISTORY not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var history []providers.Message
	if err := json.Unmarshal(raw, &history); err != nil {
		t.Fatal(err)
	}
	tc := tokencount.NewTiktokenCounter()
	loop := &Loop{provider: &capturingProvider{response: "summary"}, model: "grok-4.6"}
	total := tc.CountMessages(loop.model, history)
	budget := total * 152230 / 211204

	run := func(withFit bool) (*pipeline.RunState, pipeline.StageResult) {
		deps := &pipeline.PipelineDeps{
			Config:            pipeline.PipelineConfig{ContextWindow: budget + 8192, MaxTokens: 8192},
			TokenCounter:      tc,
			PruneMessages:     loop.makePruneMessages(),
			SanitizeHistory:   sanitizeHistory,
			CompactMessages:   loop.makeCompactMessages(nil),
			OverBudgetMessage: func(context.Context) string { return "too long" },
		}
		if withFit {
			deps.FitHistory = func(msgs []providers.Message, b int, model string) ([]providers.Message, pipeline.FitStats) {
				return fitHistoryToBudget(msgs, b, tc, model, defaultHardClearPlaceholder)
			}
		}
		state := pipeline.NewRunState(&pipeline.RunInput{SessionKey: "recorded"}, nil, loop.model, nil)
		state.Messages.SetHistory(append([]providers.Message(nil), history...))
		stage := pipeline.NewPruneStage(deps, nil)
		if err := stage.Execute(context.Background(), state); err != nil {
			t.Fatal(err)
		}
		return state, stage.Result()
	}

	t.Logf("recorded history: %d messages, %d tokens, budget %d", len(history), total, budget)
	if state, res := run(false); res != pipeline.AbortRun || state.Observe.FinalContent != "too long" {
		t.Errorf("without fit: result=%v content=%q, want AbortRun with the over-budget reply", res, state.Observe.FinalContent)
	}
	state, res := run(true)
	if res != pipeline.Continue {
		t.Fatalf("with fit: result=%v, want Continue", res)
	}
	got := state.Messages.History()
	t.Logf("after fit: %d messages, %d tokens", len(got), tc.CountMessages(loop.model, got))
	if got[len(got)-1].Content != history[len(history)-1].Content {
		t.Fatal("newest user message changed")
	}
	assertToolPairing(t, got)
}

func assertToolPairing(t *testing.T, msgs []providers.Message) {
	t.Helper()
	open := map[string]bool{}
	for i, m := range msgs {
		for _, call := range m.ToolCalls {
			open[call.ID] = true
		}
		if m.Role == "tool" {
			if !open[m.ToolCallID] {
				t.Fatalf("tool result at %d has no matching call %q", i, m.ToolCallID)
			}
			delete(open, m.ToolCallID)
		}
	}
	if len(open) > 0 {
		t.Fatalf("tool calls without results: %v", open)
	}
}
