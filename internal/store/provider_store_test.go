package store

import "testing"

// TestValidProviderTypes_IncludesCustom guards the "custom" provider type —
// a generic OpenAI-compatible shim for endpoints that don't fit other types
// (local 9router, LiteLLM, bring-your-own proxy).
func TestValidProviderTypes_IncludesCustom(t *testing.T) {
	if ProviderCustom != "custom" {
		t.Fatalf("ProviderCustom = %q, want %q", ProviderCustom, "custom")
	}
	if !ValidProviderTypes[ProviderCustom] {
		t.Fatalf("ValidProviderTypes[%q] = false, want true", ProviderCustom)
	}
}
