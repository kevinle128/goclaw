package agent

import (
	"strings"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

func hashTestTools(names ...string) []providers.ToolDefinition {
	defs := make([]providers.ToolDefinition, 0, len(names))
	for _, n := range names {
		defs = append(defs, providers.ToolDefinition{Type: "function", Function: &providers.ToolFunctionSchema{
			Name: n, Description: "d " + n, Parameters: map[string]any{"type": "object", "b": 1, "a": 2},
		}})
	}
	return defs
}

func hashTestMsgs(stable, dynamic string) []providers.Message {
	return []providers.Message{
		{Role: "system", Content: stable + "\n" + CacheBoundaryMarker + "\n" + dynamic},
		{Role: "user", Content: "hi"},
	}
}

func TestPromptHashes_DynamicChangeOnlyMovesDynamicHash(t *testing.T) {
	a := computePromptHashes(hashTestMsgs("stable", "turn one"), hashTestTools("a", "b"))
	b := computePromptHashes(hashTestMsgs("stable", "turn two"), hashTestTools("a", "b"))
	if a.SystemStable != b.SystemStable || a.Tools != b.Tools || a.ToolNames != b.ToolNames {
		t.Fatalf("stable system and tools hashes must not move: %+v vs %+v", a, b)
	}
	if a.SystemDynamic == b.SystemDynamic {
		t.Fatal("dynamic hash must change when the dynamic section changes")
	}
}

func TestPromptHashes_ToolOrderMovesNameHash(t *testing.T) {
	a := computePromptHashes(hashTestMsgs("s", "d"), hashTestTools("a", "b"))
	b := computePromptHashes(hashTestMsgs("s", "d"), hashTestTools("b", "a"))
	if a.ToolNames == b.ToolNames || a.Tools == b.Tools {
		t.Fatalf("a different tool order must change both tool hashes: %+v vs %+v", a, b)
	}
}

func TestPromptHashes_SchemaChangeKeepsNameHash(t *testing.T) {
	a := hashTestTools("a")
	b := hashTestTools("a")
	b[0].Function.Description = "changed"
	ha := computePromptHashes(nil, a)
	hb := computePromptHashes(nil, b)
	if ha.ToolNames != hb.ToolNames || ha.Tools == hb.Tools {
		t.Fatalf("schema change must move only the tools hash: %+v vs %+v", ha, hb)
	}
}

func TestPromptHashes_NoMarkerAndNativeTool(t *testing.T) {
	msgs := []providers.Message{{Role: "system", Content: "only stable"}}
	h := computePromptHashes(msgs, []providers.ToolDefinition{{Type: "image_generation"}})
	if h.SystemDynamic != promptHashNone {
		t.Fatalf("dynamic hash = %q, want %q", h.SystemDynamic, promptHashNone)
	}
	if h.Tools == "" || h.ToolNames == "" {
		t.Fatal("native tool without a function wrapper must still hash")
	}
}

func TestPromptHashes_NoPromptTextInOutput(t *testing.T) {
	h := computePromptHashes(hashTestMsgs("SECRET-STABLE", "SECRET-DYNAMIC"), hashTestTools("secret_tool"))
	for _, v := range []string{h.SystemStable, h.SystemDynamic, h.Tools, h.ToolNames} {
		if len(v) != 12 || strings.Contains(strings.ToUpper(v), "SECRET") {
			t.Fatalf("hash %q must be 12 hex characters without prompt text", v)
		}
	}
}
