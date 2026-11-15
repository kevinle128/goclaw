package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

func runContextStageWithMemory(t *testing.T, stable bool, userContent, memory string) *RunState {
	t.Helper()
	deps := &PipelineDeps{
		Config: PipelineConfig{StablePromptPrefix: stable},
		BuildMessages: func(_ context.Context, _ *RunInput, _ []providers.Message, _ string) ([]providers.Message, error) {
			return []providers.Message{
				{Role: "system", Content: "stable system"},
				{Role: "user", Content: "older"},
				{Role: "assistant", Content: "answer"},
				{Role: "user", Content: userContent},
			}, nil
		},
		AutoInject: func(_ context.Context, _, _, _ string) (string, error) { return memory, nil },
	}
	state := NewRunState(&RunInput{Message: "question"}, nil, "m", nil)
	if err := NewContextStage(deps).Execute(context.Background(), state); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return state
}

func lastUserContent(state *RunState) string {
	h := state.Messages.History()
	return h[len(h)-1].Content
}

func TestContextStage_Memory_DefaultAppendsToSystem(t *testing.T) {
	state := runContextStageWithMemory(t, false, "question", "MEMORY-L0")
	if got := state.Messages.System().Content; !strings.HasSuffix(got, "MEMORY-L0") {
		t.Fatalf("system = %q, want memory appended", got)
	}
	if got := lastUserContent(state); got != "question" {
		t.Fatalf("user message changed with flag off: %q", got)
	}
}

func TestContextStage_Memory_StableKeepsSystemUntouched(t *testing.T) {
	state := runContextStageWithMemory(t, true, "question", "MEMORY-L0")
	if got := state.Messages.System().Content; got != "stable system" {
		t.Fatalf("system = %q, want unchanged", got)
	}
	got := lastUserContent(state)
	want := "<runtime_context>\nMEMORY-L0\n</runtime_context>\n\nquestion"
	if got != want {
		t.Fatalf("user = %q, want %q", got, want)
	}
}

func TestContextStage_Memory_StableJoinsExistingRuntimeBlock(t *testing.T) {
	in := "<runtime_context>\nDYNAMIC\n</runtime_context>\n\nquestion"
	state := runContextStageWithMemory(t, true, in, "MEMORY-L0")
	want := "<runtime_context>\nMEMORY-L0\n\nDYNAMIC\n</runtime_context>\n\nquestion"
	if got := lastUserContent(state); got != want {
		t.Fatalf("user = %q, want %q", got, want)
	}
	if n := strings.Count(lastUserContent(state), "<runtime_context>"); n != 1 {
		t.Fatalf("want exactly one runtime block, got %d", n)
	}
}

func TestStripRuntimeContext(t *testing.T) {
	in := "<runtime_context>\nDYNAMIC\n</runtime_context>\n\nquestion"
	if got := StripRuntimeContext(in); got != "question" {
		t.Fatalf("got %q", got)
	}
	if got := StripRuntimeContext("plain"); got != "plain" {
		t.Fatalf("plain text changed: %q", got)
	}
}
