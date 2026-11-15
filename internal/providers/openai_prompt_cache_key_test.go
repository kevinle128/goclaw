package providers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// captureChatBody runs one non-stream Chat call against a test server and
// returns the decoded request body that reached the server.
func captureChatBody(t *testing.T, build func(apiBase string) *OpenAIProvider) map[string]any {
	t.Helper()
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer srv.Close()

	p := build(srv.URL)
	_, err := p.Chat(context.Background(), ChatRequest{
		Messages: []Message{{Role: "user", Content: "hi"}},
		Model:    "cx/gpt-test",
		Options:  map[string]any{OptPromptCacheKey: "goclaw:tenant:agent"},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	return got
}

func TestOpenAIProvider_PromptCacheKey_SentToOptedInProxy(t *testing.T) {
	body := captureChatBody(t, func(apiBase string) *OpenAIProvider {
		return NewOpenAIProvider("9router", "sk-fake", apiBase, "cx/gpt-test").WithPromptCacheKey(true)
	})
	if body["prompt_cache_key"] != "goclaw:tenant:agent" {
		t.Fatalf("prompt_cache_key = %v, want goclaw:tenant:agent", body["prompt_cache_key"])
	}
}

func TestOpenAIProvider_PromptCacheKey_NotSentToOtherProxy(t *testing.T) {
	body := captureChatBody(t, func(apiBase string) *OpenAIProvider {
		return NewOpenAIProvider("other-proxy", "sk-fake", apiBase, "cx/gpt-test")
	})
	if _, ok := body["prompt_cache_key"]; ok {
		t.Fatalf("prompt_cache_key sent to a proxy without opt-in: %v", body["prompt_cache_key"])
	}
}
