package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"

	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// promptHashes are short digests of the parts of an LLM request that decide
// whether a provider prefix cache can be reused. They carry no prompt text.
type promptHashes struct {
	// SystemStable covers the system text before the cache boundary marker
	// (the whole system text when there is no marker).
	SystemStable string
	// SystemDynamic covers the system text after the marker; "none" when the
	// system message has no marker.
	SystemDynamic string
	// Tools covers ordered tool names, descriptions and parameter schemas.
	Tools string
	// ToolNames covers the ordered tool names only, so an order change can be
	// told apart from a schema change.
	ToolNames string
}

const promptHashNone = "none"

func shortHash(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

// computePromptHashes hashes the system message and tool list of a request.
func computePromptHashes(msgs []providers.Message, toolDefs []providers.ToolDefinition) promptHashes {
	out := promptHashes{SystemStable: promptHashNone, SystemDynamic: promptHashNone}
	if len(msgs) > 0 && msgs[0].Role == "system" {
		before, after, found := strings.Cut(msgs[0].Content, CacheBoundaryMarker)
		out.SystemStable = shortHash(before)
		if found {
			out.SystemDynamic = shortHash(after)
		}
	}

	full := make([]string, 0, len(toolDefs)*3)
	names := make([]string, 0, len(toolDefs))
	for _, td := range toolDefs {
		name := toolDefKey(td)
		names = append(names, name)
		desc, params := "", ""
		if td.Function != nil {
			desc = td.Function.Description
			raw, err := json.Marshal(td.Function.Parameters)
			if err != nil {
				params = "marshal-error"
			} else {
				params = string(raw)
			}
		}
		full = append(full, name, desc, params)
	}
	out.Tools = shortHash(full...)
	out.ToolNames = shortHash(names...)
	return out
}

// toolDefKey is the identity of a tool definition in an ordered list. Native
// tools such as image_generation have no function wrapper, so they use their type.
func toolDefKey(td providers.ToolDefinition) string {
	if td.Function != nil {
		return td.Function.Name
	}
	return "type:" + td.Type
}

// applyStableToolOrder keeps tool definitions in the order the run first sent
// them. A tool seen for the first time (for example a deferred MCP tool that
// mcp_tool_search activated) is appended after the known tools, sorted within
// its batch; a tool that is missing from one build is skipped without moving
// the others, and takes its old place if it returns. An empty list (final
// iteration) leaves the recorded order unchanged.
func applyStableToolOrder(state *pipeline.RunState, defs []providers.ToolDefinition) []providers.ToolDefinition {
	if len(defs) == 0 {
		return defs
	}
	order := state.Think.ToolOrder
	if len(order) == 0 {
		state.Think.ToolOrder = make([]string, 0, len(defs))
		for _, td := range defs {
			state.Think.ToolOrder = append(state.Think.ToolOrder, toolDefKey(td))
		}
		return defs
	}

	position := make(map[string]int, len(order))
	for i, key := range order {
		position[key] = i
	}
	known := make([]providers.ToolDefinition, 0, len(defs))
	var fresh []providers.ToolDefinition
	for _, td := range defs {
		if _, ok := position[toolDefKey(td)]; ok {
			known = append(known, td)
		} else {
			fresh = append(fresh, td)
		}
	}
	slices.SortStableFunc(known, func(a, b providers.ToolDefinition) int {
		return position[toolDefKey(a)] - position[toolDefKey(b)]
	})
	slices.SortStableFunc(fresh, func(a, b providers.ToolDefinition) int {
		return strings.Compare(toolDefKey(a), toolDefKey(b))
	})
	for _, td := range fresh {
		state.Think.ToolOrder = append(state.Think.ToolOrder, toolDefKey(td))
	}
	return append(known, fresh...)
}
