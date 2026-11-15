package store

import (
	"encoding/json"
	"testing"
)

func TestProviderSendsPromptCacheKey(t *testing.T) {
	cases := []struct {
		settings string
		want     bool
	}{
		{"", false},
		{"{}", false},
		{`{"send_prompt_cache_key":true}`, true},
		{`{"send_prompt_cache_key":false}`, false},
		{`{"send_prompt_cache_key":"yes"}`, false},
		{"not json", false},
	}
	for _, c := range cases {
		if got := ProviderSendsPromptCacheKey(json.RawMessage(c.settings)); got != c.want {
			t.Errorf("settings %q: got %v, want %v", c.settings, got, c.want)
		}
	}
}
