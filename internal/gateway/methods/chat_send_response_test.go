package methods

import (
	"encoding/json"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/agent"
)

func TestBuildChatSendResponse_AbortObject(t *testing.T) {
	t.Parallel()
	result := &agent.RunResult{
		RunID:   "r1",
		Content: "too long",
		Abort: &agent.TurnAbort{
			Reason:       "context_over_budget",
			Message:      "too long",
			UsedTokens:   1473269,
			BudgetTokens: 206621,
		},
	}
	raw, err := json.Marshal(buildChatSendResponse(result, result.Content, nil))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Content string          `json:"content"`
		Abort   json.RawMessage `json:"abort"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Content != "too long" {
		t.Errorf("content = %q, want the reply kept for clients that ignore abort", got.Content)
	}
	want := `{"reason":"context_over_budget","message":"too long","usedTokens":1473269,"budgetTokens":206621}`
	if string(got.Abort) != want {
		t.Errorf("abort = %s, want %s", got.Abort, want)
	}
}

func TestBuildChatSendResponse_NoAbortKeyForNormalRun(t *testing.T) {
	t.Parallel()
	resp := buildChatSendResponse(&agent.RunResult{RunID: "r1", Content: "answer", Thinking: "t"}, "answer", nil)
	if _, ok := resp["abort"]; ok {
		t.Errorf("response has an abort key for a normal run: %v", resp)
	}
	if resp["runId"] != "r1" || resp["content"] != "answer" || resp["thinking"] != "t" {
		t.Errorf("response = %v, want runId, content and thinking unchanged", resp)
	}
}
