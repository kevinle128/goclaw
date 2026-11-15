package pipeline

import (
	"strings"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// Per-turn prompt content (current chat metadata, date, extra instructions,
// L0 memory) goes into one block at the start of the current-turn user message
// when PipelineConfig.StablePromptPrefix is on. The block is request-only: the
// session stores the raw user message, so later turns keep a byte-stable prefix.
const (
	RuntimeContextOpen  = "<runtime_context>\n"
	RuntimeContextClose = "\n</runtime_context>\n\n"
)

// WrapRuntimeContext puts block in front of text inside a runtime block.
// An empty block returns text unchanged.
func WrapRuntimeContext(block, text string) string {
	if block == "" {
		return text
	}
	return RuntimeContextOpen + block + RuntimeContextClose + text
}

// StripRuntimeContext removes a leading runtime block from content, so
// recall queries and compaction input do not carry per-turn prompt content.
func StripRuntimeContext(content string) string {
	if !strings.HasPrefix(content, RuntimeContextOpen) {
		return content
	}
	_, rest, ok := strings.Cut(content[len(RuntimeContextOpen):], RuntimeContextClose)
	if !ok {
		return content
	}
	return rest
}

// addToRuntimeContext puts section at the start of the runtime block in
// content, creating the block when content has none.
func addToRuntimeContext(content, section string) string {
	if strings.HasPrefix(content, RuntimeContextOpen) {
		return RuntimeContextOpen + section + "\n\n" + content[len(RuntimeContextOpen):]
	}
	return WrapRuntimeContext(section, content)
}

// addMemoryToCurrentTurn puts the L0 memory section into the runtime block of
// the current-turn user message (the last user message in history). It returns
// false when history has no user message, so the caller can fall back to the
// system message. Only Content changes: images and media refs stay attached.
func addMemoryToCurrentTurn(state *RunState, section string) bool {
	history := state.Messages.History()
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role != "user" {
			continue
		}
		updated := make([]providers.Message, len(history))
		copy(updated, history)
		updated[i].Content = addToRuntimeContext(updated[i].Content, section)
		state.Messages.SetHistory(updated)
		return true
	}
	return false
}
