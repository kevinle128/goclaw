package agent

import (
	"encoding/json"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
)

func TestConvertRunResult_CarriesAbort(t *testing.T) {
	t.Parallel()
	got := convertRunResult(&pipeline.RunResult{
		RunID:   "r1",
		Content: "too long",
		Abort: &pipeline.TurnAbort{
			Reason:       pipeline.AbortReasonContextOverBudget,
			Message:      "too long",
			UsedTokens:   1473269,
			BudgetTokens: 206621,
		},
	})
	want := &TurnAbort{Reason: "context_over_budget", Message: "too long", UsedTokens: 1473269, BudgetTokens: 206621}
	if got.Abort == nil || *got.Abort != *want {
		t.Fatalf("Abort = %+v, want %+v", got.Abort, want)
	}
}

func TestConvertRunResult_NoAbortStaysOutOfJSON(t *testing.T) {
	t.Parallel()
	got := convertRunResult(&pipeline.RunResult{RunID: "r1", Content: "answer"})
	if got.Abort != nil {
		t.Fatalf("Abort = %+v, want nil", got.Abort)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["abort"]; ok {
		t.Errorf("RunResult JSON has an abort key for a normal run: %s", raw)
	}
}
