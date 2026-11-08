package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

// ValidateServerSettings checks the "protocol" and "task_events" keys of an
// MCP server's Settings JSONB. The runtime parsers (ParseProtocolMode,
// ParseTaskEvents) accept bad values and use defaults; this check rejects
// them so that an admin sees the error when the server is saved. Other keys
// of settings (for example tool_hints) are not checked. Empty or null
// settings are valid.
func ValidateServerSettings(settings json.RawMessage) error {
	if isJSONNull(settings) {
		return nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(settings, &top); err != nil || top == nil {
		return errors.New("settings must be a JSON object")
	}
	if err := validateProtocolSetting(top["protocol"]); err != nil {
		return err
	}
	return validateTaskEventsSetting(top["task_events"])
}

func validateProtocolSetting(raw json.RawMessage) error {
	if isJSONNull(raw) {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return errors.New(`settings.protocol must be "auto" or "legacy"`)
	}
	switch ProtocolMode(strings.ToLower(strings.TrimSpace(s))) {
	case "", ProtocolAuto, ProtocolLegacy:
		return nil
	default:
		return errors.New(`settings.protocol must be "auto" or "legacy"`)
	}
}

// taskEventsKeys are the keys that task_events can have. An unknown key is
// an error, because it is usually a typo (for example "taskArgument").
var taskEventsKeys = map[string]bool{
	"event": true, "task_argument": true, "bootstrap_arguments": true,
	"heartbeat_sec": true, "safety_poll_sec": true,
}

func validateTaskEventsSetting(raw json.RawMessage) error {
	if isJSONNull(raw) {
		return nil
	}
	var te map[string]json.RawMessage
	if err := json.Unmarshal(raw, &te); err != nil || te == nil {
		return errors.New("settings.task_events must be a JSON object")
	}
	for k := range te {
		if !taskEventsKeys[k] {
			return fmt.Errorf("settings.task_events has an unknown key %q", k)
		}
	}
	if err := validateStringSetting(te["event"], "event", taskEventsMaxEventLen); err != nil {
		return err
	}
	if err := validateStringSetting(te["task_argument"], "task_argument", taskEventsMaxArgumentLen); err != nil {
		return err
	}
	if b := te["bootstrap_arguments"]; !isJSONNull(b) {
		if !isJSONObject(b) {
			return errors.New("settings.task_events.bootstrap_arguments must be a JSON object")
		}
		if compactLen(b) > taskEventsMaxBootstrapBytes {
			return fmt.Errorf("settings.task_events.bootstrap_arguments must be at most %d bytes", taskEventsMaxBootstrapBytes)
		}
	}
	if err := validateIntSetting(te["heartbeat_sec"], "heartbeat_sec", taskEventsHeartbeatMin, taskEventsHeartbeatMax); err != nil {
		return err
	}
	return validateIntSetting(te["safety_poll_sec"], "safety_poll_sec", taskEventsSafetyPollMin, taskEventsSafetyPollMax)
}

// validateStringSetting checks a required task_events string: after trim it
// must have 1 to maxLen characters.
func validateStringSetting(raw json.RawMessage, name string, maxLen int) error {
	var s string
	if isJSONNull(raw) || json.Unmarshal(raw, &s) != nil {
		return fmt.Errorf("settings.task_events.%s is required and must be a string", name)
	}
	if n := utf8.RuneCountInString(strings.TrimSpace(s)); n < 1 || n > maxLen {
		return fmt.Errorf("settings.task_events.%s must have 1 to %d characters", name, maxLen)
	}
	return nil
}

// validateIntSetting checks an optional task_events integer in [lo, hi].
func validateIntSetting(raw json.RawMessage, name string, lo, hi int) error {
	if isJSONNull(raw) {
		return nil
	}
	var v float64
	if err := json.Unmarshal(raw, &v); err != nil || v < float64(lo) || v > float64(hi) || v != math.Trunc(v) {
		return fmt.Errorf("settings.task_events.%s must be an integer from %d to %d", name, lo, hi)
	}
	return nil
}

// isJSONNull reports whether raw is absent or the JSON literal null.
func isJSONNull(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

// isJSONObject reports whether raw is a JSON object (not an array, a
// string, a number, or null).
func isJSONObject(raw json.RawMessage) bool {
	var obj map[string]json.RawMessage
	return json.Unmarshal(raw, &obj) == nil && obj != nil
}

// compactLen is the size of raw without insignificant white space. The size
// limit uses it because PostgreSQL JSONB adds spaces when it gives the
// value back.
func compactLen(raw json.RawMessage) int {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return len(raw)
	}
	return buf.Len()
}
