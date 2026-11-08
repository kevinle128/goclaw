package mcp

import (
	"encoding/json"
	"log/slog"
	"strings"
)

// ProtocolMode is the per-server setting "protocol" in mcp_servers.settings.
type ProtocolMode string

const (
	// ProtocolAuto probes server/discover and falls back to the legacy
	// initialize handshake when the server is not on 2026-07-28. Default.
	ProtocolAuto ProtocolMode = "auto"
	// ProtocolLegacy always uses the initialize handshake (2025-11-25) and
	// never sends server/discover.
	ProtocolLegacy ProtocolMode = "legacy"
)

// ParseProtocolMode reads the "protocol" field from an MCP server's Settings
// JSONB. Empty, malformed, or unknown values give ProtocolAuto; an unknown
// value is logged. Safe to call with nil.
func ParseProtocolMode(settings json.RawMessage) ProtocolMode {
	if len(settings) == 0 {
		return ProtocolAuto
	}
	var s struct {
		Protocol string `json:"protocol"`
	}
	if err := json.Unmarshal(settings, &s); err != nil {
		return ProtocolAuto
	}
	switch ProtocolMode(strings.ToLower(strings.TrimSpace(s.Protocol))) {
	case "", ProtocolAuto:
		return ProtocolAuto
	case ProtocolLegacy:
		return ProtocolLegacy
	default:
		slog.Warn("mcp.settings.protocol_unknown", "value", s.Protocol, "using", ProtocolAuto)
		return ProtocolAuto
	}
}

// legacyOnly reports whether a connection must skip the server/discover
// probe. stdio servers are always legacy-only: a stdio server that ignores
// unknown methods makes each probe wait the full discover timeout (5 s), and
// the stdio init retry loop can repeat that wait.
func legacyOnly(transportType string, mode ProtocolMode) bool {
	return transportType == "stdio" || mode == ProtocolLegacy
}

// ConnectOption changes how a pooled connection is made.
type ConnectOption func(*connectConfig)

type connectConfig struct {
	protocol ProtocolMode
}

// WithProtocolMode sets the protocol mode of a pooled connection (default
// ProtocolAuto).
func WithProtocolMode(mode ProtocolMode) ConnectOption {
	return func(c *connectConfig) { c.protocol = mode }
}

func applyConnectOptions(opts []ConnectOption) connectConfig {
	cfg := connectConfig{protocol: ProtocolAuto}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.protocol == "" {
		cfg.protocol = ProtocolAuto
	}
	return cfg
}

func mapToEnvSlice(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	s := make([]string, 0, len(env))
	for k, v := range env {
		s = append(s, k+"="+v)
	}
	return s
}

func toSet(items []string) map[string]struct{} {
	if len(items) == 0 {
		return nil
	}
	s := make(map[string]struct{}, len(items))
	for _, item := range items {
		s[item] = struct{}{}
	}
	return s
}

func joinErrors(errs []string) string {
	var result strings.Builder
	for i, e := range errs {
		if i > 0 {
			result.WriteString("; ")
		}
		result.WriteString(e)
	}
	return result.String()
}

// ParseJSONBytesToStringSlice converts JSONB []byte to []string (exported for loop_mcp_user).
func ParseJSONBytesToStringSlice(data []byte) []string { return jsonBytesToStringSlice(data) }

// ParseJSONBytesToStringMap converts JSONB []byte to map[string]string (exported for loop_mcp_user).
func ParseJSONBytesToStringMap(data []byte) map[string]string { return jsonBytesToStringMap(data) }

// jsonBytesToStringSlice converts JSONB []byte to []string. Returns nil on error.
func jsonBytesToStringSlice(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	var result []string
	if err := jsonUnmarshal(data, &result); err != nil {
		return nil
	}
	return result
}

// jsonBytesToStringMap converts JSONB []byte to map[string]string. Returns nil on error.
func jsonBytesToStringMap(data []byte) map[string]string {
	if len(data) == 0 {
		return nil
	}
	var result map[string]string
	if err := jsonUnmarshal(data, &result); err != nil {
		return nil
	}
	return result
}
