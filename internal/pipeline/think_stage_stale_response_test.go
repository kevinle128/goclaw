package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// TestPipeline_EmergencyCompactionDoesNotRerunToolCalls: when the provider
// rejects a request, ThinkStage compacts and returns before it reads a new
// response. ToolStage must not run the previous iteration's tool calls again.
func TestPipeline_EmergencyCompactionDoesNotRerunToolCalls(t *testing.T) {
	t.Parallel()
	o := newOverflowPipeline("small result")
	var rejected atomic.Bool
	inner := o.deps.CallLLM
	o.deps.CallLLM = func(ctx context.Context, state *RunState, req providers.ChatRequest) (*providers.ChatResponse, error) {
		// Reject the second request once, as a provider that counts differently would.
		if o.llmCalls.Load() == 1 && rejected.CompareAndSwap(false, true) {
			o.llmCalls.Add(1)
			return nil, errors.New("context length exceeded for this request")
		}
		return inner(ctx, state, req)
	}
	o.run(t)

	if got := o.toolRuns.Load(); got != 1 {
		t.Errorf("tool executions = %d, want 1 (tool calls must not re-run after emergency compaction)", got)
	}
}
