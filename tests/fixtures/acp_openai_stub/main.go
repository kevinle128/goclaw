package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

type chatRequest struct {
	Messages []struct {
		Role    string `json:"role"`
		Content any    `json:"content"`
	} `json:"messages"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:18891", "listen address")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/chat/completions", chatCompletions)
	mux.HandleFunc("/v1/chat/completions", chatCompletions)

	server := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}

func chatCompletions(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req chatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	prompt := lastContent(req.Messages)
	if strings.Contains(prompt, "cancel") {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(30 * time.Second):
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusInternalServerError)
		return
	}

	if strings.Contains(prompt, "tool") && !hasToolResult(req.Messages) {
		if err := writeSSE(w, map[string]any{"choices": []any{map[string]any{
			"delta": map[string]any{"tool_calls": []any{map[string]any{
				"index": 0,
				"id":    "buzz-tool-call-1",
				"type":  "function",
				"function": map[string]any{
					"name":      "mcp_buzz_dev_mcp__shell",
					"arguments": `{"command":"printf from-goclaw"}`,
				},
			}}},
			"finish_reason": "tool_calls",
		}}}); err != nil {
			return
		}
		if err := writeDone(w); err != nil {
			return
		}
		flusher.Flush()
		return
	}

	if err := writeSSE(w, textChunk("first ", nil)); err != nil {
		return
	}
	flusher.Flush()
	if err := writeSSE(w, textChunk("second", nil)); err != nil {
		return
	}
	if err := writeSSE(w, textChunk("", "stop")); err != nil {
		return
	}
	if err := writeDone(w); err != nil {
		return
	}
	flusher.Flush()
}

func lastContent(messages []struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if text, ok := messages[i].Content.(string); ok {
			return text
		}
	}
	return ""
}

func hasToolResult(messages []struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}) bool {
	for _, message := range messages {
		if message.Role == "tool" {
			return true
		}
	}
	return false
}

func textChunk(content string, finishReason any) map[string]any {
	return map[string]any{"choices": []any{map[string]any{
		"delta":         map[string]any{"content": content},
		"finish_reason": finishReason,
	}}}
}

func writeSSE(w http.ResponseWriter, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
	return err
}

func writeDone(w http.ResponseWriter) error {
	_, err := fmt.Fprint(w, "data: [DONE]\n\n")
	return err
}
