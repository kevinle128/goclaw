package acpserver

import (
	"encoding/json"
	"testing"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// A prompt whose run stopped early still ends the turn; the abort rides in
// _meta so a client can post the reply and reset the session. Results without
// an abort stay a bare end_turn.
func TestPromptResultCarriesTurnAbort(t *testing.T) {
	tests := map[string]struct {
		response map[string]any
		want     string
	}{
		"over budget resets the session": {
			response: map[string]any{"content": "too long", "abort": map[string]any{
				"reason": "context_over_budget", "message": "too long", "usedTokens": 1473269, "budgetTokens": 206621}},
			want: `{"stopReason":"end_turn","_meta":{"buzz.turnAbort":{"budgetTokens":206621,"message":"too long","reason":"context_over_budget","resetSession":true,"usedTokens":1473269}}}`,
		},
		"truncated output keeps the session and omits token counts": {
			response: map[string]any{"content": "", "abort": map[string]any{"reason": "output_truncated", "message": "cut off"}},
			want:     `{"stopReason":"end_turn","_meta":{"buzz.turnAbort":{"message":"cut off","reason":"output_truncated","resetSession":false}}}`,
		},
		"unknown reason keeps the session": {
			response: map[string]any{"content": "", "abort": map[string]any{"reason": "something_new", "message": "m", "usedTokens": 5, "budgetTokens": 9}},
			want:     `{"stopReason":"end_turn","_meta":{"buzz.turnAbort":{"budgetTokens":9,"message":"m","reason":"something_new","resetSession":false,"usedTokens":5}}}`,
		},
		"no abort stays a bare end_turn": {
			response: map[string]any{"content": "answer"},
			want:     `{"stopReason":"end_turn"}`,
		},
		"cancelled run has no meta even with an abort": {
			response: map[string]any{"cancelled": true, "abort": map[string]any{"reason": "context_over_budget"}},
			want:     `{"stopReason":"cancelled"}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: ""})
			h := startACPHarness(t, g.url, wakeCaps)
			conn := g.conn(t)
			_, req, _ := startPrompt(t, h, conn, `{"channelId":"c1"}`)
			conn.send(protocol.NewOKResponse(req.ID, test.response))
			frame := h.waitFrame(t, func(f map[string]json.RawMessage) bool { return string(f["id"]) == "3" })
			var got any
			if err := json.Unmarshal(frame["result"], &got); err != nil {
				t.Fatal(err)
			}
			var want any
			if err := json.Unmarshal([]byte(test.want), &want); err != nil {
				t.Fatal(err)
			}
			gotJSON, _ := json.Marshal(got)
			wantJSON, _ := json.Marshal(want)
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("result = %s, want %s", frame["result"], test.want)
			}
		})
	}
}

// A gateway that predates the abort object sends none; the response decodes
// and the prompt ends as before.
func TestChatSendResponseDecodesWithoutAbort(t *testing.T) {
	var old ChatSendResponse
	if err := json.Unmarshal([]byte(`{"runId":"r1","content":"hi","usage":{"promptTokens":1}}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Abort != nil {
		t.Fatalf("Abort = %+v, want nil", old.Abort)
	}
	if got := promptResultFor(old); got.StopReason != "end_turn" || got.Meta != nil {
		t.Fatalf("result = %+v", got)
	}
}

func TestInitializeAdvertisesTurnAbort(t *testing.T) {
	_, line := initializeCapabilities(t, `{}`)
	var response struct {
		Result struct {
			AgentCapabilities struct {
				Meta map[string]json.RawMessage `json:"_meta"`
			} `json:"agentCapabilities"`
		} `json:"result"`
	}
	if err := json.Unmarshal(line, &response); err != nil {
		t.Fatal(err)
	}
	meta := response.Result.AgentCapabilities.Meta
	if string(meta["buzz.turnAbort"]) != `{"version":1}` || string(meta["buzz.wake"]) != `{"version":1}` {
		t.Fatalf("advertised agent capabilities = %s", line)
	}
}
