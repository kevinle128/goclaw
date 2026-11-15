package pipeline

import (
	"strings"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// Per-turn prompt content (current chat metadata, date, extra instructions,
// L0 memory) goes into one block at the start of the current-turn user message
// when PipelineConfig.StablePromptPrefix is on. The session stores the user
// message exactly as it was sent, block included, so the next request begins
// with the whole previous request and the provider can reuse its cache. To keep
// history small, the block is sent only when it differs from the last block
// already in history (see settleRuntimeContext).
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

// leadingRuntimeContext returns the complete leading runtime block of content,
// including its delimiters and the blank line after it, or "" when content
// does not start with one.
func leadingRuntimeContext(content string) string {
	rest := StripRuntimeContext(content)
	if len(rest) == len(content) {
		return ""
	}
	return content[:len(content)-len(rest)]
}

// settleRuntimeContext decides whether the current-turn user message (the last
// user message in history) keeps its runtime block. The block is dropped when
// the most recent block already stored in earlier history is identical, because
// the provider then sees it in the cached prefix. It returns the block that
// stays on the message, or "" when the message carries none. A block missing
// from history (new session, compaction, history limit) or a block with changed
// content stays on the message, so the model always has a current block.
func settleRuntimeContext(state *RunState) string {
	history := state.Messages.History()
	cur := -1
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == "user" {
			cur = i
			break
		}
	}
	if cur < 0 {
		return ""
	}
	block := leadingRuntimeContext(history[cur].Content)
	if block == "" {
		return ""
	}
	for i := cur - 1; i >= 0; i-- {
		if history[i].Role != "user" {
			continue
		}
		prev := leadingRuntimeContext(history[i].Content)
		if prev == "" {
			continue
		}
		if prev != block {
			return block
		}
		updated := make([]providers.Message, len(history))
		copy(updated, history)
		updated[cur].Content = history[cur].Content[len(block):]
		state.Messages.SetHistory(updated)
		return ""
	}
	return block
}
