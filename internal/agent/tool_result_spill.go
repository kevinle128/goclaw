package agent

import (
	"context"
	"fmt"
	"log/slog"
	"unicode/utf8"

	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/tokencount"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

const (
	// spillPreviewHeadRunes and spillPreviewTailRunes size the preview the model
	// keeps in context for a spilled tool result.
	spillPreviewHeadRunes = 6000
	spillPreviewTailRunes = 2000

	// readFileToolName is the tool that reads spill files back. Its own output is
	// never spilled, or reading a spill file would spill again.
	readFileToolName = "read_file"
)

// toolResultSpiller bounds oversized tool results. It holds no per-run state.
type toolResultSpiller struct {
	maxTokens int
	counter   tokencount.TokenCounter
	store     *tools.ToolResultSpillStore
	// resolve maps a model-facing tool call name to the registered tool name.
	resolve func(name string) string
	// isReadFile reports whether a registered tool name is the file reader,
	// including its aliases.
	isReadFile func(name string) bool
}

// makeBoundToolResult returns the pipeline callback that spills oversized tool
// results to a file. It returns nil (no guard) when the threshold is 0 or no
// spill store is configured.
func (l *Loop) makeBoundToolResult(counter tokencount.TokenCounter) func(ctx context.Context, state *pipeline.RunState, tc providers.ToolCall, msg providers.Message) providers.Message {
	if l.toolResultMaxTokens <= 0 || l.toolResultSpill == nil || counter == nil {
		return nil
	}
	sp := &toolResultSpiller{
		maxTokens: l.toolResultMaxTokens,
		counter:   counter,
		store:     l.toolResultSpill,
		resolve:   l.resolveToolCallName,
		isReadFile: func(name string) bool {
			if l.registry == nil {
				return name == readFileToolName
			}
			t, ok := l.registry.Get(name)
			return ok && t.Name() == readFileToolName
		},
	}
	return func(ctx context.Context, state *pipeline.RunState, tc providers.ToolCall, msg providers.Message) providers.Message {
		return sp.bound(ctx, state.Model, state.Input.SessionKey, tc, msg)
	}
}

// bound returns msg unchanged when it fits, is exempt, or cannot be saved.
// Otherwise it returns msg with Content replaced by a head and tail preview that
// names the file holding the full result. Saving is best effort: a failure
// keeps the inline result and never turns the tool call into an error.
func (sp *toolResultSpiller) bound(ctx context.Context, model, sessionKey string, tc providers.ToolCall, msg providers.Message) providers.Message {
	// Every token is at least one byte, so a result of at most maxTokens bytes
	// cannot exceed the threshold and needs no token count.
	if msg.Role != "tool" || len(msg.Content) <= sp.maxTokens {
		return msg
	}
	if sp.isReadFile(sp.resolve(tc.Name)) {
		return msg
	}
	tokens := sp.counter.Count(model, msg.Content)
	if tokens <= sp.maxTokens {
		return msg
	}

	res, err := sp.store.Save(tools.ToolWorkspaceFromCtx(ctx), store.TenantIDFromContext(ctx), sessionKey, sp.resolve(tc.Name), msg.Content)
	if err != nil {
		slog.Warn("tool_result.spill_failed", "tool", tc.Name, "tokens", tokens, "chars", len(msg.Content), "error", err)
		return msg
	}

	chars := utf8.RuneCountInString(msg.Content)
	slog.Info("tool_result.spilled", "tool", tc.Name, "tokens", tokens, "chars", chars, "path", res.RelPath)
	msg.Content = spillNotice(msg.Content, tokens, chars, res)
	return msg
}

// spillNotice builds the model-facing replacement for a spilled result. English
// only: the text is consumed by the model, like the bootstrap templates.
func spillNotice(content string, tokens, chars int, res tools.SpillResult) string {
	wrapNote := ""
	if res.Wrapped {
		wrapNote = fmt.Sprintf(" The file has %d lines; lines longer than 2000 chars were wrapped.", res.Lines)
	}
	return fmt.Sprintf("%s\n\n[...]\n\n%s\n\n[Tool result too large: about %d tokens (%d chars). Showing the first %d and last %d chars. Full result saved to: %s.%s Use read_file with offset/limit on this path, or exec grep on it, to see the rest.]",
		takeHead(content, spillPreviewHeadRunes), takeTail(content, spillPreviewTailRunes),
		tokens, chars, spillPreviewHeadRunes, spillPreviewTailRunes, res.RelPath, wrapNote)
}
