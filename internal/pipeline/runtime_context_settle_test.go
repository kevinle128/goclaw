package pipeline

import (
	"context"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// settleWithHistory runs the context stage over a history whose earlier user
// message already holds prevBlock (when non-empty) and whose current message
// carries curBlock, then returns the current message and the recorded block.
func settleWithHistory(t *testing.T, stable bool, prevBlock, curBlock, memory string) (string, string, bool) {
	t.Helper()
	prev := "older"
	if prevBlock != "" {
		prev = WrapRuntimeContext(prevBlock, "older")
	}
	var recorded string
	called := false
	deps := &PipelineDeps{
		Config: PipelineConfig{StablePromptPrefix: stable},
		BuildMessages: func(_ context.Context, _ *RunInput, _ []providers.Message, _ string) ([]providers.Message, error) {
			return []providers.Message{
				{Role: "system", Content: "stable system"},
				{Role: "user", Content: prev},
				{Role: "assistant", Content: "answer"},
				{Role: "user", Content: WrapRuntimeContext(curBlock, "question")},
			}, nil
		},
		AutoInject:           func(_ context.Context, _, _, _ string) (string, error) { return memory, nil },
		RecordRuntimeContext: func(b string) { recorded = b; called = true },
	}
	state := NewRunState(&RunInput{Message: "question"}, nil, "m", nil)
	if err := NewContextStage(deps).Execute(context.Background(), state); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	h := state.Messages.History()
	return h[len(h)-1].Content, recorded, called
}

func TestSettleRuntimeContext_UnchangedBlockIsDropped(t *testing.T) {
	// The previous message holds the block with the same dynamic text and memory.
	prev := "MEMORY\n\nDYNAMIC"
	got, recorded, _ := settleWithHistory(t, true, prev, "DYNAMIC", "MEMORY")
	if got != "question" || recorded != "" {
		t.Fatalf("content = %q, recorded = %q, want raw text and no block", got, recorded)
	}
}

func TestSettleRuntimeContext_ChangedMemoryIsSentAgain(t *testing.T) {
	prev := "OLD-MEMORY\n\nDYNAMIC"
	got, recorded, _ := settleWithHistory(t, true, prev, "DYNAMIC", "NEW-MEMORY")
	want := "<runtime_context>\nNEW-MEMORY\n\nDYNAMIC\n</runtime_context>\n\n"
	if recorded != want || got != want+"question" {
		t.Fatalf("content = %q, recorded = %q, want block %q", got, recorded, want)
	}
}

func TestSettleRuntimeContext_ChangedDynamicTextIsSentAgain(t *testing.T) {
	got, recorded, _ := settleWithHistory(t, true, "DATE-1", "DATE-2", "")
	want := "<runtime_context>\nDATE-2\n</runtime_context>\n\n"
	if recorded != want || got != want+"question" {
		t.Fatalf("content = %q, recorded = %q, want block %q", got, recorded, want)
	}
}

func TestSettleRuntimeContext_NoEarlierBlockIsSent(t *testing.T) {
	got, recorded, _ := settleWithHistory(t, true, "", "DATE-1", "")
	want := "<runtime_context>\nDATE-1\n</runtime_context>\n\n"
	if recorded != want || got != want+"question" {
		t.Fatalf("content = %q, recorded = %q, want block %q", got, recorded, want)
	}
}

func TestSettleRuntimeContext_FlagOffLeavesMessageAndSkipsRecord(t *testing.T) {
	got, _, called := settleWithHistory(t, false, "DATE-1", "DATE-1", "")
	want := "<runtime_context>\nDATE-1\n</runtime_context>\n\nquestion"
	if got != want || called {
		t.Fatalf("content = %q, record called = %v, want message untouched and no record", got, called)
	}
}
