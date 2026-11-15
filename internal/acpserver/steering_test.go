package acpserver

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// steerLine is a _session/steering request with the exact shape Buzz sends:
// session id and text blocks, no run id, no _meta.
func steerLine(id, sessionID, text string) string {
	return `{"jsonrpc":"2.0","id":` + id + `,"method":"_session/steering","params":{"sessionId":` + strconv.Quote(sessionID) +
		`,"prompt":[{"type":"text","text":` + strconv.Quote(text) + `}]}}` + "\n"
}

func initializeResultJSON(t *testing.T, handler Handler) []byte {
	t.Helper()
	var output lockedBuffer
	transport := NewTransport(strings.NewReader(initializeLine(`1`)), &output, handler, "test-version")
	if err := transport.Serve(context.Background()); err != nil {
		t.Fatal(err)
	}
	lines := splitLines(output.Bytes())
	if len(lines) != 1 {
		t.Fatalf("response count = %d", len(lines))
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(lines[0], &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Result
}

func steerHandler() Handler {
	return Handler{Steer: func(context.Context, SteerParams) (SteerResult, error) {
		return SteerResult{Outcome: SteerOutcomeInjected}, nil
	}}
}

// Buzz reads the flag at the ROOT of the initialize result. Under
// agentCapabilities._meta it would stay unread and steering would be off.
func TestInitialize_AdvertisesSteeringAtResultRoot(t *testing.T) {
	result := initializeResultJSON(t, steerHandler())
	var root map[string]json.RawMessage
	if err := json.Unmarshal(result, &root); err != nil {
		t.Fatal(err)
	}
	if string(root["_meta"]) != `{"steering":{"supported":true}}` {
		t.Fatalf("root _meta = %s, want steering.supported=true", root["_meta"])
	}
	var caps struct {
		Meta map[string]json.RawMessage `json:"_meta"`
	}
	if err := json.Unmarshal(root["agentCapabilities"], &caps); err != nil {
		t.Fatal(err)
	}
	if _, misplaced := caps.Meta[SteeringCapabilityKey]; misplaced {
		t.Fatalf("steering also under agentCapabilities._meta: %s", root["agentCapabilities"])
	}
	if string(caps.Meta["buzz.wake"]) != `{"version":1}` {
		t.Fatalf("buzz.wake lost: %s", root["agentCapabilities"])
	}
}

// The pointer is the one Buzz uses (acp.rs: /_meta/steering/supported).
func TestInitialize_SteeringReadableAtBuzzPointer(t *testing.T) {
	var doc any
	if err := json.Unmarshal(initializeResultJSON(t, steerHandler()), &doc); err != nil {
		t.Fatal(err)
	}
	meta, _ := doc.(map[string]any)["_meta"].(map[string]any)
	steering, _ := meta["steering"].(map[string]any)
	if supported, _ := steering["supported"].(bool); !supported {
		t.Fatalf("/_meta/steering/supported is not true in %v", doc)
	}
}

func TestInitialize_NoSteeringWhenDisabled(t *testing.T) {
	result := initializeResultJSON(t, Handler{})
	var root map[string]json.RawMessage
	if err := json.Unmarshal(result, &root); err != nil {
		t.Fatal(err)
	}
	if _, ok := root["_meta"]; ok {
		t.Fatalf("root _meta present with steering off: %s", result)
	}
}

func TestSteering_DisabledMethodNotFound(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: ""})
	h := startACPHarnessSteering(t, g.url, wakeCaps, false)
	sessionID := h.newSession(t)
	h.write(t, steerLine(`10`, sessionID, "more"))
	frame := h.waitFrame(t, func(f map[string]json.RawMessage) bool { return string(f["id"]) == "10" })
	if code := rpcErrorCode(t, frame); code != CodeMethodNotFound {
		t.Fatalf("error code = %d, want %d", code, CodeMethodNotFound)
	}
}

func (h *acpHarness) newSession(t *testing.T) string {
	t.Helper()
	h.write(t, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":`+strconv.Quote(h.cwd)+`,"mcpServers":[]}}`+"\n")
	created := h.waitFrame(t, func(f map[string]json.RawMessage) bool { return string(f["id"]) == "2" })
	var result NewSessionResult
	if err := json.Unmarshal(created["result"], &result); err != nil || result.SessionID == "" {
		t.Fatalf("session/new = %v", created)
	}
	return result.SessionID
}

func rpcErrorCode(t *testing.T, frame map[string]json.RawMessage) int {
	t.Helper()
	var rpcErr RPCError
	if err := json.Unmarshal(frame["error"], &rpcErr); err != nil || frame["error"] == nil {
		t.Fatalf("no error in frame %v", frame)
	}
	return rpcErr.Code
}

type steerHarness struct {
	g         *fakeGateway
	h         *acpHarness
	conn      *fakeGatewayConn
	sessionID string
	send      protocol.RequestFrame
}

// runningPrompt starts a prompt and leaves its chat.send open.
func runningPrompt(t *testing.T) *steerHarness {
	t.Helper()
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: ""})
	h := startACPHarnessSteering(t, g.url, wakeCaps, true)
	conn := g.conn(t)
	sessionID, req, _ := startPrompt(t, h, conn, `{"channelId":"c1"}`)
	// Cleanups run last in first out: end the open chat.send before the
	// harness closes its input, so Run can stop. A repeat answer is ignored.
	t.Cleanup(func() { conn.send(protocol.NewOKResponse(req.ID, map[string]any{"content": ""})) })
	return &steerHarness{g: g, h: h, conn: conn, sessionID: sessionID, send: req}
}

func (s *steerHarness) steer(t *testing.T, id, text string) {
	t.Helper()
	s.h.write(t, steerLine(id, s.sessionID, text))
}

func (s *steerHarness) answer(t *testing.T, id string) map[string]json.RawMessage {
	t.Helper()
	return s.h.waitFrame(t, func(f map[string]json.RawMessage) bool { return string(f["id"]) == id })
}

func TestSteering_InjectedWhilePromptRunning(t *testing.T) {
	s := runningPrompt(t)
	s.steer(t, "10", "framed steer text")

	req := s.conn.next(t, protocol.MethodChatSteer)
	var params ChatSteerRequest
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatal(err)
	}
	var send ChatSendRequest
	if err := json.Unmarshal(s.send.Params, &send); err != nil {
		t.Fatal(err)
	}
	if params.SessionKey != send.SessionKey || params.Message != "framed steer text" {
		t.Fatalf("chat.steer params = %+v, want the prompt's session key and the text unchanged", params)
	}
	s.conn.send(protocol.NewOKResponse(req.ID, map[string]any{"injected": true, "runId": "run-1"}))

	frame := s.answer(t, "10")
	if string(frame["result"]) != `{"outcome":"injected"}` {
		t.Fatalf("result = %s, want {\"outcome\":\"injected\"}", frame["result"])
	}
	// The prompt is still running: no prompt answer, no abort.
	s.conn.none(t, protocol.MethodChatAbort, 100*time.Millisecond)
	s.conn.send(protocol.NewOKResponse(s.send.ID, map[string]any{"content": "done"}))
	prompt := s.answer(t, "3")
	if string(prompt["result"]) != `{"stopReason":"end_turn"}` {
		t.Fatalf("prompt result = %s", prompt["result"])
	}
}

func TestSteering_TwoSequentialSteersBothAnswered(t *testing.T) {
	s := runningPrompt(t)
	for i, id := range []string{"10", "11"} {
		s.steer(t, id, "steer "+id)
		req := s.conn.next(t, protocol.MethodChatSteer)
		s.conn.send(protocol.NewOKResponse(req.ID, map[string]any{"injected": true, "runId": "run-1"}))
		frame := s.answer(t, id)
		if string(frame["result"]) != `{"outcome":"injected"}` {
			t.Fatalf("steer %d result = %s", i, frame["result"])
		}
	}
}

func TestSteering_NoActivePromptReturnsApplicationError(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: ""})
	h := startACPHarnessSteering(t, g.url, wakeCaps, true)
	conn := g.conn(t)
	sessionID := h.newSession(t)

	h.write(t, steerLine(`10`, sessionID, "more"))
	frame := h.waitFrame(t, func(f map[string]json.RawMessage) bool { return string(f["id"]) == "10" })

	if code := rpcErrorCode(t, frame); code != CodeSteerNotApplied {
		t.Fatalf("error code = %d, want %d (not method-not-found)", code, CodeSteerNotApplied)
	}
	if _, hasResult := frame["result"]; hasResult {
		t.Fatalf("frame carries a result: %v", frame)
	}
	conn.none(t, protocol.MethodChatSteer, 100*time.Millisecond)
}

func TestSteering_AfterPromptEndedReturnsApplicationError(t *testing.T) {
	s := runningPrompt(t)
	s.conn.send(protocol.NewOKResponse(s.send.ID, map[string]any{"content": "done"}))
	s.answer(t, "3")

	s.steer(t, "10", "too late")
	frame := s.answer(t, "10")
	if code := rpcErrorCode(t, frame); code != CodeSteerNotApplied {
		t.Fatalf("error code = %d, want %d", code, CodeSteerNotApplied)
	}
	s.conn.none(t, protocol.MethodChatSteer, 100*time.Millisecond)
}

func TestSteering_GatewayRefusalsReturnApplicationError(t *testing.T) {
	tests := map[string]func(conn *fakeGatewayConn, req protocol.RequestFrame){
		"inject channel full": func(conn *fakeGatewayConn, req protocol.RequestFrame) {
			conn.send(protocol.NewOKResponse(req.ID, map[string]any{"injected": false, "reason": "inject_full"}))
		},
		"run ended at the gateway": func(conn *fakeGatewayConn, req protocol.RequestFrame) {
			conn.send(protocol.NewOKResponse(req.ID, map[string]any{"injected": false, "reason": "no_active_run"}))
		},
		"gateway error": func(conn *fakeGatewayConn, req protocol.RequestFrame) {
			conn.send(protocol.NewErrorResponse(req.ID, protocol.ErrInternal, "boom"))
		},
		"older gateway without chat.steer": func(conn *fakeGatewayConn, req protocol.RequestFrame) {
			conn.send(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, "unknown method: chat.steer"))
		},
	}
	for name, reply := range tests {
		t.Run(name, func(t *testing.T) {
			s := runningPrompt(t)
			s.steer(t, "10", "more")
			reply(s.conn, s.conn.next(t, protocol.MethodChatSteer))
			frame := s.answer(t, "10")
			if code := rpcErrorCode(t, frame); code != CodeSteerNotApplied {
				t.Fatalf("error code = %d, want %d", code, CodeSteerNotApplied)
			}
		})
	}
}

func TestSteering_UnknownSession(t *testing.T) {
	s := runningPrompt(t)
	s.h.write(t, steerLine(`10`, "no-such-session", "more"))
	frame := s.answer(t, "10")
	if code := rpcErrorCode(t, frame); code != CodeUnknownSession {
		t.Fatalf("error code = %d, want %d", code, CodeUnknownSession)
	}
	s.conn.none(t, protocol.MethodChatSteer, 100*time.Millisecond)
}

func TestSteering_InvalidParams(t *testing.T) {
	s := runningPrompt(t)
	bad := map[string]string{
		"empty prompt":    `{"sessionId":"` + s.sessionID + `","prompt":[]}`,
		"non-text block":  `{"sessionId":"` + s.sessionID + `","prompt":[{"type":"image","data":"x"}]}`,
		"empty text":      `{"sessionId":"` + s.sessionID + `","prompt":[{"type":"text","text":""}]}`,
		"no session id":   `{"prompt":[{"type":"text","text":"x"}]}`,
		"oversize prompt": `{"sessionId":"` + s.sessionID + `","prompt":[{"type":"text","text":"` + strings.Repeat("a", MaxPromptTextBytes+1) + `"}]}`,
	}
	i := 20
	for name, params := range bad {
		id := strconv.Itoa(i)
		i++
		s.h.write(t, `{"jsonrpc":"2.0","id":`+id+`,"method":"_session/steering","params":`+params+`}`+"\n")
		frame := s.answer(t, id)
		if code := rpcErrorCode(t, frame); code != CodeInvalidParams {
			t.Errorf("%s: error code = %d, want %d", name, code, CodeInvalidParams)
		}
	}
	s.conn.none(t, protocol.MethodChatSteer, 100*time.Millisecond)
}

func TestDecodeSteerParams_ToleratesMeta(t *testing.T) {
	params, rpcErr := DecodeSteerParams(json.RawMessage(`{"sessionId":"s","prompt":[{"type":"text","text":"a"},{"type":"text","text":"b"}],"_meta":{"x":1}}`))
	if rpcErr != nil || params.SessionID != "s" || joinPrompt(params.Prompt) != "a\nb" {
		t.Fatalf("params = %+v, err = %v", params, rpcErr)
	}
}
