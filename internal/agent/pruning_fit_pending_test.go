package agent

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

func pendingFixture() []providers.Message {
	return []providers.Message{
		{Role: "assistant", Content: "calling", ToolCalls: []providers.ToolCall{{ID: "a"}, {ID: "b"}}},
		{Role: "tool", ToolCallID: "a", Content: strings.Repeat("A", 40_000)},
		{Role: "tool", ToolCallID: "b", Content: strings.Repeat("B", 40_000)},
		{Role: "user", Content: strings.Repeat("U", 40_000)},
	}
}

func TestFitPendingToolResults_TrimsOldestToolResultsFirst(t *testing.T) {
	in := pendingFixture()
	want := in[3]
	// Each 40k-char message is 10k tokens (30k in total). Cutting the oldest
	// tool result to head + tail (about 1k tokens) fits the 22k budget.
	out, stats := fitPendingToolResults(in, 22_000, quarterCounter{}, "m", "[cleared]")

	if len(out) != len(in) {
		t.Fatalf("len = %d, want %d (no message dropped)", len(out), len(in))
	}
	if !reflect.DeepEqual(out[0], in[0]) {
		t.Error("assistant tool_calls message was changed")
	}
	if !reflect.DeepEqual(out[3], want) {
		t.Error("non-tool message was changed")
	}
	if utf8.RuneCountInString(out[1].Content) >= 40_000 || out[1].ToolCallID != "a" {
		t.Errorf("first tool result not trimmed or lost its ID: %d runes id=%s", utf8.RuneCountInString(out[1].Content), out[1].ToolCallID)
	}
	if out[2].Content != in[2].Content {
		t.Error("second tool result was trimmed although the first cut already fit the budget")
	}
	if stats.ResultsTrimmed != 1 || stats.ResultsCleared != 0 {
		t.Errorf("stats = %+v, want one trimmed", stats)
	}
	if in[1].Content != strings.Repeat("A", 40_000) {
		t.Error("input slice was modified")
	}
}

func TestFitPendingToolResults_FallsBackToPlaceholder(t *testing.T) {
	in := pendingFixture()
	out, stats := fitPendingToolResults(in, 10_500, quarterCounter{}, "m", "[cleared]")

	if out[1].Content != "[cleared]" || out[2].Content != "[cleared]" {
		t.Errorf("tool results have %d / %d runes, want the placeholder", len(out[1].Content), len(out[2].Content))
	}
	if out[1].ToolCallID != "a" || out[2].ToolCallID != "b" {
		t.Error("tool results lost their ToolCallID")
	}
	if out[3].Content != in[3].Content {
		t.Error("user message was changed")
	}
	if stats.ResultsCleared != 2 {
		t.Errorf("ResultsCleared = %d, want 2", stats.ResultsCleared)
	}
}

func TestFitPendingToolResults_FittingInputIsReturnedAsIs(t *testing.T) {
	in := []providers.Message{{Role: "tool", ToolCallID: "a", Content: "small"}}
	out, stats := fitPendingToolResults(in, 1000, quarterCounter{}, "m", "[cleared]")
	if !reflect.DeepEqual(out, in) || stats.ResultsTrimmed+stats.ResultsCleared != 0 {
		t.Errorf("fitting input changed: %+v %+v", out, stats)
	}
}
