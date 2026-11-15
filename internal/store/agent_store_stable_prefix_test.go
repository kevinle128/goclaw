package store

import "testing"

func TestParseStablePromptPrefix(t *testing.T) {
	cases := map[string]bool{
		``:                               false,
		`{}`:                             false,
		`not json`:                       false,
		`{"stable_prompt_prefix":true}`:  true,
		`{"stable_prompt_prefix":false}`: false,
		`{"stable_prompt_prefix":"yes"}`: false,
		`{"other":1,"stable_prompt_prefix":true}`: true,
	}
	for raw, want := range cases {
		a := &AgentData{OtherConfig: []byte(raw)}
		if got := a.ParseStablePromptPrefix(); got != want {
			t.Errorf("%q: got %v, want %v", raw, got, want)
		}
	}
}
