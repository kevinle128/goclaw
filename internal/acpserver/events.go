package acpserver

import (
	"context"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

const MaxTurnOutputBytes = 8 << 20

func gatewayEventText(event GatewayEvent) (kind, text string, ok bool) {
	if event.Type != protocol.ChatEventChunk && event.Type != protocol.ChatEventThinking {
		return "", "", false
	}
	var envelope struct {
		Payload struct {
			Content string `json:"content"`
		} `json:"payload"`
	}
	if json.Unmarshal(event.Payload, &envelope) != nil || envelope.Payload.Content == "" {
		return "", "", false
	}
	if event.Type == protocol.ChatEventThinking {
		return "agent_thought_chunk", envelope.Payload.Content, true
	}
	return "agent_message_chunk", envelope.Payload.Content, true
}

func emitGatewayText(ctx context.Context, transport *Transport, sessionID string, event GatewayEvent, used *int) error {
	kind, content, ok := gatewayEventText(event)
	if !ok {
		return nil
	}
	remaining := MaxTurnOutputBytes - *used
	if remaining <= 0 {
		return nil
	}
	if len(content) > remaining {
		content = content[:remaining]
	}
	for len(content) > 0 {
		n := min(len(content), MaxOutputTextChunkBytes)
		for n > 0 && !utf8.ValidString(content[:n]) {
			n--
		}
		if n == 0 {
			break
		}
		chunk := content[:n]
		content = content[n:]
		if err := transport.SessionUpdate(ctx, SessionUpdateParams{SessionID: sessionID, Update: SessionUpdate{SessionUpdate: kind, Content: TextContent{Type: "text", Text: chunk}}}); err != nil {
			return err
		}
		*used += len(chunk)
	}
	return nil
}

func emitGatewayTool(ctx context.Context, transport *Transport, sessionID string, event GatewayEvent) error {
	if event.Type != protocol.AgentEventToolCall && event.Type != protocol.AgentEventToolResult {
		return nil
	}
	var envelope struct {
		Payload struct {
			Name      string         `json:"name"`
			ID        string         `json:"id"`
			Arguments map[string]any `json:"arguments"`
			Result    string         `json:"result"`
			Content   string         `json:"content"`
			IsError   bool           `json:"is_error"`
		} `json:"payload"`
	}
	if json.Unmarshal(event.Payload, &envelope) != nil || envelope.Payload.ID == "" {
		return nil
	}
	if event.Type == protocol.AgentEventToolCall {
		return transport.SessionRawUpdate(ctx, sessionID, map[string]any{
			"sessionUpdate": "tool_call", "toolCallId": envelope.Payload.ID,
			"title": envelope.Payload.Name, "kind": "other", "status": "pending",
			"rawInput": envelope.Payload.Arguments,
		})
	}
	status := "completed"
	if envelope.Payload.IsError {
		status = "failed"
	}
	content := envelope.Payload.Result
	if content == "" {
		content = envelope.Payload.Content
	}
	return transport.SessionRawUpdate(ctx, sessionID, map[string]any{
		"sessionUpdate": "tool_call_update", "toolCallId": envelope.Payload.ID,
		"status":    status,
		"content":   []any{map[string]any{"type": "content", "content": map[string]any{"type": "text", "text": content}}},
		"rawOutput": map[string]any{"isError": envelope.Payload.IsError},
	})
}

func joinPrompt(blocks []TextContent) string {
	parts := make([]string, 0, len(blocks))
	for _, block := range blocks {
		parts = append(parts, block.Text)
	}
	return strings.Join(parts, "\n")
}
