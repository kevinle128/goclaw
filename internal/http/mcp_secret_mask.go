package http

import (
	"encoding/json"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// mcpMaskedValue replaces every stored MCP credential value in API responses.
// It matches the marker the providers API uses for api_key, so clients can
// send the marker back unchanged to mean "keep the stored value".
const mcpMaskedValue = "***"

// maskMCPServer returns a response-safe copy of an MCP server. The api_key is
// replaced by the marker. Headers and env keep their keys, but each value is
// replaced by the marker, because the store keeps all of them encrypted.
// Internal callers that need the real values read from the store directly.
func maskMCPServer(srv store.MCPServerData) store.MCPServerData {
	if srv.APIKey != "" {
		srv.APIKey = mcpMaskedValue
	}
	srv.Headers = maskMCPStringMap(srv.Headers)
	srv.Env = maskMCPStringMap(srv.Env)
	return srv
}

func maskMCPStringMap(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		// Unknown shape: never return it unmasked.
		return nil
	}
	for k := range m {
		m[k] = mcpMaskedValue
	}
	out, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return out
}

// restoreMaskedMCPMap replaces marker values in a client-supplied headers/env
// object with the stored value for the same key. A marker for a key that has no
// stored value is dropped. Other values pass through as new values.
func restoreMaskedMCPMap(incoming any, stored json.RawMessage) any {
	in, ok := incoming.(map[string]any)
	if !ok {
		return incoming
	}
	var old map[string]any
	if len(stored) > 0 {
		_ = json.Unmarshal(stored, &old)
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if s, isStr := v.(string); isStr && s == mcpMaskedValue {
			if prev, found := old[k]; found {
				out[k] = prev
			}
			continue
		}
		out[k] = v
	}
	return out
}

// restoreMaskedMCPStrings is restoreMaskedMCPMap for string maps.
func restoreMaskedMCPStrings(in map[string]string, stored json.RawMessage) map[string]string {
	if in == nil {
		return nil
	}
	generic := make(map[string]any, len(in))
	for k, v := range in {
		generic[k] = v
	}
	restored, _ := restoreMaskedMCPMap(generic, stored).(map[string]any)
	out := make(map[string]string, len(restored))
	for k, v := range restored {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}
