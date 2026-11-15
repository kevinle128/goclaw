package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Pipeline orchestrates stage execution for a single agent run.
type Pipeline struct {
	setup     []Stage // runs once before iteration loop
	iteration []Stage // runs per iteration
	finalize  []Stage // runs once after loop

	Deps PipelineDeps
}

// NewPipeline creates a pipeline from explicit stage lists.
func NewPipeline(setup, iteration, finalize []Stage, deps PipelineDeps) *Pipeline {
	return &Pipeline{
		setup:     setup,
		iteration: iteration,
		finalize:  finalize,
		Deps:      deps,
	}
}

// NewDefaultPipeline creates the standard 8-stage pipeline.
// Setup: [ContextStage]. Iteration: [ThinkStage, PruneStage, ToolStage, ObserveStage, CheckpointStage].
// Finalize: [FinalizeStage].
func NewDefaultPipeline(deps PipelineDeps) *Pipeline {
	d := &deps
	memFlush := NewMemoryFlushStage(d)

	setup := []Stage{
		NewContextStage(d),
	}
	iteration := []Stage{
		NewThinkStage(d),
		NewPruneStage(d, memFlush),
		NewToolStage(d),
		NewObserveStage(d),
		NewCheckpointStage(d),
	}
	finalize := []Stage{
		NewFinalizeStage(d),
	}
	return NewPipeline(setup, iteration, finalize, deps)
}

// Run executes the full pipeline for a single agent run.
func (p *Pipeline) Run(ctx context.Context, state *RunState) (*RunResult, error) {
	start := time.Now()

	// 1. Setup (once)
	for _, stage := range p.setup {
		if err := stage.Execute(ctx, state); err != nil {
			return nil, fmt.Errorf("setup %s: %w", stage.Name(), err)
		}
	}
	// Propagate enriched context from setup stages (ContextStage injects agent/user/workspace values).
	if state.Ctx != nil {
		ctx = state.Ctx
	}

	// 2. Iteration loop
	// BreakLoop: complete all remaining stages in this iteration (ObserveStage must
	// capture FinalContent), then exit the outer loop.
	// AbortRun: exit inner loop immediately (unrecoverable, e.g. over budget after compaction).
	for state.Iteration = 0; state.Iteration < p.Deps.Config.MaxIterations; state.Iteration++ {
		for _, stage := range p.iteration {
			if err := stage.Execute(ctx, state); err != nil {
				return nil, fmt.Errorf("iter %d %s: %w", state.Iteration, stage.Name(), err)
			}
			// AbortRun exits inner loop immediately — skip remaining stages.
			if swr, ok := stage.(StageWithResult); ok && swr.Result() == AbortRun {
				state.ExitCode = AbortRun
				break
			}
		}

		// Check exit after all stages (or after AbortRun broke early).
		if state.ExitCode == AbortRun {
			break
		}
		for _, stage := range p.iteration {
			if swr, ok := stage.(StageWithResult); ok && swr.Result() == BreakLoop {
				state.ExitCode = BreakLoop
				break
			}
		}
		if state.ExitCode == BreakLoop {
			if state.Observe.ContinueAfterFinal {
				state.Observe.ContinueAfterFinal = false
				state.ExitCode = Continue
				continue
			}
			// A follow-up can land after the last observe drain. Without this
			// second drain the run would finish while the message sits unread
			// in the inject channel, and the sender has already been told it
			// was accepted. Only drain when another iteration is allowed and
			// the run is still live.
			if p.drainLateInjection(ctx, state) {
				state.Observe.ContinueAfterFinal = false
				state.ExitCode = Continue
				continue
			}
			break
		}
		if ctx.Err() != nil {
			state.ExitCode = AbortRun
			break
		}
	}

	// 3. Finalize (once, errors logged not fatal).
	// Use background context so finalize stages can persist state even after cancellation.
	finalizeCtx := context.WithoutCancel(ctx)
	for _, stage := range p.finalize {
		if err := stage.Execute(finalizeCtx, state); err != nil {
			slog.Warn("finalize stage error", "stage", stage.Name(), "err", err)
		}
	}

	result := state.BuildResult()
	result.Duration = time.Since(start)
	if result.Duration <= 0 {
		result.Duration = time.Nanosecond
	}
	return result, nil
}

// drainLateInjection reads the inject channel once more after the run decided
// to finish. It reports true when it queued follow-up messages behind the final
// answer and the loop should run another iteration.
func (p *Pipeline) drainLateInjection(ctx context.Context, state *RunState) bool {
	if p.Deps.DrainInjectCh == nil || ctx.Err() != nil {
		return false
	}
	// Only a model answer without tool calls can be followed by a user
	// message. A run that stopped for another reason (for example a killed
	// tool loop) must not resume.
	if resp := state.Think.LastResponse; resp == nil || len(resp.ToolCalls) > 0 {
		return false
	}
	if state.Iteration+1 >= p.Deps.Config.MaxIterations {
		return false
	}
	injected := p.Deps.DrainInjectCh()
	if len(injected) == 0 {
		return false
	}
	continueAfterFinal(state, state.Observe.FinalContent, state.Observe.FinalThinking, injected)
	return true
}
