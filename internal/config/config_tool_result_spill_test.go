package config

import (
	"encoding/json"
	"testing"
)

func TestToolResultSpillRetentionDays(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		json string
		want int
	}{
		{"section absent", `{}`, DefaultToolResultSpillRetentionDays},
		{"key absent", `{"toolResultSpill":{}}`, DefaultToolResultSpillRetentionDays},
		{"zero turns the sweep off", `{"toolResultSpill":{"retentionDays":0}}`, 0},
		{"explicit", `{"toolResultSpill":{"retentionDays":3}}`, 3},
		{"negative falls back to default", `{"toolResultSpill":{"retentionDays":-1}}`, DefaultToolResultSpillRetentionDays},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var tools ToolsConfig
			if err := json.Unmarshal([]byte(tc.json), &tools); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got := tools.ToolResultSpill.EffectiveRetentionDays(); got != tc.want {
				t.Errorf("EffectiveRetentionDays() = %d, want %d", got, tc.want)
			}
		})
	}
}
