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

// Limits of the task_events server setting.
const (
	taskEventsHeartbeatDefault  = 30
	taskEventsHeartbeatMin      = 5
	taskEventsHeartbeatMax      = 300
	taskEventsSafetyPollDefault = 300
	taskEventsSafetyPollMin     = 30
	taskEventsSafetyPollMax     = 3600
	taskEventsMaxEventLen       = 128
	taskEventsMaxArgumentLen    = 64
	taskEventsMaxBootstrapBytes = 1024
)

// TaskEventsBinding is the server setting "task_events": which event of the
// events extension reports the state of a task, and how to subscribe to it.
//
//	{"task_events": {"event": "workflow_run.status_changed",
//	  "task_argument": "run_id", "bootstrap_arguments": {"workflow": "w"},
//	  "heartbeat_sec": 30, "safety_poll_sec": 300}}
type TaskEventsBinding struct {
	// Event is the event name for events/poll and events/stream.
	Event string
	// TaskArgument is the subscription argument that takes the task id.
	TaskArgument string
	// BootstrapArguments are the events/poll arguments before tools/call
	// (a JSON object; {} when not set).
	BootstrapArguments json.RawMessage
	// HeartbeatSec is the expected heartbeat interval (5-300, default 30).
	HeartbeatSec int
	// SafetyPollSec is the interval of the safety tasks/get (30-3600,
	// default 300).
	SafetyPollSec int
}

// ParseTaskEvents reads the "task_events" field from an MCP server's
// Settings JSONB. It returns nil when the field is absent or not usable:
// event and task_argument must be non-empty strings within their length
// limits. bootstrap_arguments that is not a JSON object of at most 1 KiB
// becomes {}. A number out of its range takes its default. Safe to call with
// nil.
func ParseTaskEvents(settings json.RawMessage) *TaskEventsBinding {
	if len(settings) == 0 {
		return nil
	}
	var s struct {
		TaskEvents *struct {
			Event              string          `json:"event"`
			TaskArgument       string          `json:"task_argument"`
			BootstrapArguments json.RawMessage `json:"bootstrap_arguments"`
			HeartbeatSec       *float64        `json:"heartbeat_sec"`
			SafetyPollSec      *float64        `json:"safety_poll_sec"`
		} `json:"task_events"`
	}
	if err := json.Unmarshal(settings, &s); err != nil || s.TaskEvents == nil {
		return nil
	}
	te := s.TaskEvents
	event := strings.TrimSpace(te.Event)
	arg := strings.TrimSpace(te.TaskArgument)
	if event == "" || arg == "" || len(event) > taskEventsMaxEventLen || len(arg) > taskEventsMaxArgumentLen {
		slog.Warn("mcp.settings.task_events_invalid", "reason", "event or task_argument empty or too long")
		return nil
	}
	b := &TaskEventsBinding{
		Event:              event,
		TaskArgument:       arg,
		BootstrapArguments: json.RawMessage(`{}`),
		HeartbeatSec:       intSetting(te.HeartbeatSec, taskEventsHeartbeatDefault, taskEventsHeartbeatMin, taskEventsHeartbeatMax, "heartbeat_sec"),
		SafetyPollSec:      intSetting(te.SafetyPollSec, taskEventsSafetyPollDefault, taskEventsSafetyPollMin, taskEventsSafetyPollMax, "safety_poll_sec"),
	}
	if raw := te.BootstrapArguments; len(raw) > 0 && string(raw) != "null" {
		var obj map[string]any
		if len(raw) <= taskEventsMaxBootstrapBytes && json.Unmarshal(raw, &obj) == nil && obj != nil {
			b.BootstrapArguments = append(json.RawMessage(nil), raw...)
		} else {
			slog.Warn("mcp.settings.task_events_invalid", "reason", "bootstrap_arguments is not a JSON object of at most 1 KiB")
		}
	}
	return b
}

// intSetting returns the integer value of v when it is in [min, max], and def
// otherwise (also for an absent or fractional value).
func intSetting(v *float64, def, lo, hi int, name string) int {
	if v == nil {
		return def
	}
	n := int(*v)
	if float64(n) != *v || n < lo || n > hi {
		slog.Warn("mcp.settings.task_events_out_of_range", "field", name, "using", def)
		return def
	}
	return n
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
