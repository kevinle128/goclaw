//go:build integration

package http_api

import (
	"fmt"
	"testing"
	"time"
)

// CONTRACT: a provider with type="custom" can be created with a user-supplied
// apiBase + apiKey + default_model in Settings, and GET /v1/providers/{id}/models
// returns the single declared model without any upstream fetch attempt.
func TestContract_HTTP_CustomProvider_CreateAndListModels(t *testing.T) {
	baseURL, token := getTestServer(t)
	client := newHTTPClient(baseURL, token)

	// Unique name per run so reruns don't collide.
	name := fmt.Sprintf("contract-custom-%d", time.Now().UnixNano())

	created := client.post(t, "/v1/providers", map[string]any{
		"name":          name,
		"provider_type": "custom",
		"api_base":      "http://127.0.0.1:1/v1", // unreachable — proves we never dial upstream for /models
		"api_key":       "contract-test-key",
		"enabled":       true,
		"settings": map[string]any{
			"default_model": "cc/contract-test-model",
		},
	})

	assertField(t, created, "id", "string")
	assertField(t, created, "provider_type", "string")

	if got := created["provider_type"]; got != "custom" {
		t.Fatalf("provider_type = %v, want custom", got)
	}

	id, _ := created["id"].(string)
	if id == "" {
		t.Fatal("created provider id is empty")
	}

	resp := client.get(t, "/v1/providers/"+id+"/models")
	assertField(t, resp, "models", "array")

	models, ok := resp["models"].([]any)
	if !ok {
		t.Fatalf("models field is not an array: %T", resp["models"])
	}
	if len(models) != 1 {
		t.Fatalf("models len = %d, want 1 — custom provider must echo default_model only", len(models))
	}
	first, _ := models[0].(map[string]any)
	if first["id"] != "cc/contract-test-model" {
		t.Errorf("models[0].id = %v, want cc/contract-test-model", first["id"])
	}
}
