package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateServerSettings(t *testing.T) {
	long := func(n int) string { return strings.Repeat("a", n) }
	bootstrap := func(valueLen int) string {
		return `{"k":"` + long(valueLen) + `"}`
	}
	good := `{"event":"workflow_run.status_changed","task_argument":"run_id"`

	valid := map[string]string{
		"empty":                    ``,
		"null":                     `null`,
		"empty object":             `{}`,
		"other keys only":          `{"tool_hints":{"global":"x"},"require_user_credentials":true}`,
		"protocol auto":            `{"protocol":"auto"}`,
		"protocol legacy":          `{"protocol":"legacy"}`,
		"protocol trim and case":   `{"protocol":" LEGACY "}`,
		"protocol empty":           `{"protocol":""}`,
		"protocol null":            `{"protocol":null}`,
		"task_events null":         `{"task_events":null}`,
		"task_events minimal":      `{"task_events":` + good + `}}`,
		"task_events full":         `{"protocol":"auto","task_events":` + good + `,"bootstrap_arguments":{"workflow":"w"},"heartbeat_sec":30,"safety_poll_sec":300}}`,
		"range low ends":           `{"task_events":` + good + `,"heartbeat_sec":5,"safety_poll_sec":30}}`,
		"range high ends":          `{"task_events":` + good + `,"heartbeat_sec":300,"safety_poll_sec":3600}}`,
		"integer as float":         `{"task_events":` + good + `,"heartbeat_sec":30.0}}`,
		"bootstrap null":           `{"task_events":` + good + `,"bootstrap_arguments":null}}`,
		"bootstrap at limit":       `{"task_events":` + good + `,"bootstrap_arguments":` + bootstrap(1024-len(`{"k":""}`)) + `}}`,
		"bootstrap spaces ignored": `{"task_events":` + good + `,"bootstrap_arguments":{ "k" :  "` + long(1024-len(`{"k":""}`)) + `" }}}`,
		"event max characters":     `{"task_events":{"event":"` + strings.Repeat("é", 128) + `","task_argument":"` + long(64) + `"}}`,
	}
	for name, in := range valid {
		if err := ValidateServerSettings(json.RawMessage(in)); err != nil {
			t.Errorf("%s: unexpected error %v", name, err)
		}
	}

	invalid := map[string]struct{ in, want string }{
		"not json":                {`not json`, "settings must be a JSON object"},
		"array":                   {`[]`, "settings must be a JSON object"},
		"protocol unknown":        {`{"protocol":"2026-07-28"}`, "settings.protocol"},
		"protocol number":         {`{"protocol":5}`, "settings.protocol"},
		"task_events string":      {`{"task_events":"x"}`, "settings.task_events must be a JSON object"},
		"task_events array":       {`{"task_events":[]}`, "settings.task_events must be a JSON object"},
		"missing event":           {`{"task_events":{"task_argument":"run_id"}}`, "task_events.event is required"},
		"missing task_argument":   {`{"task_events":{"event":"e"}}`, "task_events.task_argument is required"},
		"event not string":        {`{"task_events":{"event":1,"task_argument":"run_id"}}`, "task_events.event is required"},
		"event blank":             {`{"task_events":{"event":"  ","task_argument":"run_id"}}`, "task_events.event must have 1 to 128"},
		"event too long":          {`{"task_events":{"event":"` + long(129) + `","task_argument":"run_id"}}`, "task_events.event must have 1 to 128"},
		"task_argument too long":  {`{"task_events":{"event":"e","task_argument":"` + long(65) + `"}}`, "task_events.task_argument must have 1 to 64"},
		"unknown key":             {`{"task_events":` + good + `,"taskArgument":"x"}}`, `unknown key "taskArgument"`},
		"bootstrap array":         {`{"task_events":` + good + `,"bootstrap_arguments":[]}}`, "bootstrap_arguments must be a JSON object"},
		"bootstrap string":        {`{"task_events":` + good + `,"bootstrap_arguments":"{}"}}`, "bootstrap_arguments must be a JSON object"},
		"bootstrap too big":       {`{"task_events":` + good + `,"bootstrap_arguments":` + bootstrap(1025-len(`{"k":""}`)) + `}}`, "at most 1024 bytes"},
		"heartbeat too low":       {`{"task_events":` + good + `,"heartbeat_sec":4}}`, "heartbeat_sec must be an integer from 5 to 300"},
		"heartbeat too high":      {`{"task_events":` + good + `,"heartbeat_sec":301}}`, "heartbeat_sec must be an integer from 5 to 300"},
		"heartbeat fraction":      {`{"task_events":` + good + `,"heartbeat_sec":30.5}}`, "heartbeat_sec must be an integer"},
		"heartbeat string":        {`{"task_events":` + good + `,"heartbeat_sec":"30"}}`, "heartbeat_sec must be an integer"},
		"heartbeat huge":          {`{"task_events":` + good + `,"heartbeat_sec":1e300}}`, "heartbeat_sec must be an integer"},
		"safety poll too low":     {`{"task_events":` + good + `,"safety_poll_sec":29}}`, "safety_poll_sec must be an integer from 30 to 3600"},
		"safety poll too high":    {`{"task_events":` + good + `,"safety_poll_sec":3601}}`, "safety_poll_sec must be an integer from 30 to 3600"},
		"safety poll negative":    {`{"task_events":` + good + `,"safety_poll_sec":-300}}`, "safety_poll_sec must be an integer"},
		"protocol bad with valid": {`{"protocol":"modern","task_events":` + good + `}}`, "settings.protocol"},
	}
	for name, tc := range invalid {
		err := ValidateServerSettings(json.RawMessage(tc.in))
		if err == nil {
			t.Errorf("%s: want error containing %q, got nil", name, tc.want)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q does not contain %q", name, err, tc.want)
		}
	}
}

// Every value that the check accepts must give a binding at run time with
// the same values, so that a saved server does not silently lose its
// task_events setting.
func TestValidSettingsParseToSameTaskEvents(t *testing.T) {
	pad := strings.Repeat("a", 1024-len(`{"k":""}`))
	// PostgreSQL JSONB gives the object back with spaces after ':' and ','.
	spaced := json.RawMessage(`{"task_events": {"event": "` + strings.Repeat("é", 128) + `", "task_argument": "run_id", "bootstrap_arguments": {"k": "` + pad + `"}, "heartbeat_sec": 5, "safety_poll_sec": 3600}}`)
	if err := ValidateServerSettings(spaced); err != nil {
		t.Fatalf("ValidateServerSettings: %v", err)
	}
	b := ParseTaskEvents(spaced)
	if b == nil {
		t.Fatal("ParseTaskEvents gave nil for a valid setting")
	}
	if b.Event != strings.Repeat("é", 128) || b.TaskArgument != "run_id" || b.HeartbeatSec != 5 || b.SafetyPollSec != 3600 {
		t.Fatalf("binding = %+v", b)
	}
	if !strings.Contains(string(b.BootstrapArguments), pad) {
		t.Fatalf("bootstrap_arguments were dropped: %s", b.BootstrapArguments)
	}
}
