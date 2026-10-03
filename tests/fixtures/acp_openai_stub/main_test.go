package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestChatCompletionsStreamsTwoChunks(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}]}`))
	res := httptest.NewRecorder()

	chatCompletions(res, req)

	body := res.Body.String()
	if !strings.Contains(body, `"content":"first "`) || !strings.Contains(body, `"content":"second"`) {
		t.Fatalf("response does not contain both chunks: %s", body)
	}
	if !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("response does not terminate the stream: %s", body)
	}
}

func TestChatCompletionsEmitsNamedToolCall(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"use tool"}]}`))
	res := httptest.NewRecorder()

	chatCompletions(res, req)

	body := res.Body.String()
	if !strings.Contains(body, `"name":"mcp_buzz_dev_mcp__shell"`) || !strings.Contains(body, `"finish_reason":"tool_calls"`) {
		t.Fatalf("response does not contain the scripted tool call: %s", body)
	}
}

func TestChatCompletionsCancellationStopsDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"cancel"}]}`)).WithContext(ctx)
	res := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		chatCompletions(res, req)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not stop after cancellation")
	}
}
