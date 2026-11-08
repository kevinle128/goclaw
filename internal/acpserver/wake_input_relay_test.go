package acpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// fakeGatewayConn is one authenticated socket of fakeGateway.
type fakeGatewayConn struct {
	requests chan protocol.RequestFrame
	out      chan any
	closed   chan struct{}
}

func (c *fakeGatewayConn) send(frame any) { c.out <- frame }

func (c *fakeGatewayConn) drop() { close(c.closed) }

// next returns the next request with method, skipping others.
func (c *fakeGatewayConn) next(t *testing.T, method string) protocol.RequestFrame {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case req := <-c.requests:
			if req.Method == method {
				return req
			}
		case <-deadline:
			t.Fatalf("gateway did not get %s", method)
			return protocol.RequestFrame{}
		}
	}
}

// none checks that no request with method arrives within d.
func (c *fakeGatewayConn) none(t *testing.T, method string, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case req := <-c.requests:
			if req.Method == method {
				t.Fatalf("gateway got an unexpected %s: %s", method, req.Params)
			}
		case <-deadline:
			return
		}
	}
}

// fakeGateway is a scripted Gateway. Requests of the methods in reply get
// an answer at once: {} or, when set, the error code.
type fakeGateway struct {
	url   string
	conns chan *fakeGatewayConn
	reply map[string]string
}

func newFakeGateway(t *testing.T, reply map[string]string) *fakeGateway {
	t.Helper()
	g := &fakeGateway{conns: make(chan *fakeGatewayConn, 8), reply: reply}
	g.url = gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		fc := &fakeGatewayConn{requests: make(chan protocol.RequestFrame, 64), out: make(chan any, 64), closed: make(chan struct{})}
		g.conns <- fc
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			for {
				var req protocol.RequestFrame
				if conn.ReadJSON(&req) != nil {
					return
				}
				if code, ok := g.reply[req.Method]; ok {
					if code == "" {
						fc.out <- protocol.NewOKResponse(req.ID, map[string]any{})
					} else {
						fc.out <- protocol.NewErrorResponse(req.ID, code, "refused")
					}
				}
				fc.requests <- req
			}
		}()
		for {
			select {
			case frame := <-fc.out:
				if conn.WriteJSON(frame) != nil {
					return
				}
			case <-readDone:
				return
			case <-fc.closed:
				return
			}
		}
	})
	return g
}

func (g *fakeGateway) conn(t *testing.T) *fakeGatewayConn {
	t.Helper()
	select {
	case c := <-g.conns:
		return c
	case <-time.After(3 * time.Second):
		t.Fatal("no gateway connection")
		return nil
	}
}

// acpHarness runs a Server with piped stdio, like an ACP client would.
type acpHarness struct {
	server *Server
	input  *io.PipeWriter
	output *lockedBuffer
	done   chan error
	read   int
	cwd    string
}

const wakeCaps = `{"elicitation":{"form":{}},"_meta":{"buzz.wake":{"version":1}}}`

func startACPHarness(t *testing.T, gatewayURL, capabilities string) *acpHarness {
	t.Helper()
	gateway := NewGatewayClient(GatewayClientConfig{URL: gatewayURL, Token: "secret"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := gateway.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cwd := filepath.Join(root, "project")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{Gateway: gateway, AgentID: "resolver", WorkspaceRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	reader, input := io.Pipe()
	h := &acpHarness{server: server, input: input, output: &lockedBuffer{}, done: make(chan error, 1), cwd: cwd}
	go func() { h.done <- server.Run(context.Background(), reader, h.output) }()
	t.Cleanup(func() {
		_ = input.Close()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not stop")
		}
		_ = server.Close()
	})
	h.write(t, initializeWithCapabilities(`1`, capabilities))
	h.waitFrame(t, func(f map[string]json.RawMessage) bool { return string(f["id"]) == "1" })
	return h
}

func (h *acpHarness) write(t *testing.T, line string) {
	t.Helper()
	if _, err := io.WriteString(h.input, line); err != nil {
		t.Fatal(err)
	}
}

// waitFrame returns the next output frame that matches.
func (h *acpHarness) waitFrame(t *testing.T, match func(map[string]json.RawMessage) bool) map[string]json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		lines := splitLines(h.output.Bytes())
		for ; h.read < len(lines); h.read++ {
			var frame map[string]json.RawMessage
			if json.Unmarshal(lines[h.read], &frame) == nil && match(frame) {
				h.read++
				return frame
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no matching ACP frame in %s", h.output.Bytes())
	return nil
}

func methodIs(method string) func(map[string]json.RawMessage) bool {
	want, _ := json.Marshal(method)
	return func(f map[string]json.RawMessage) bool { return string(f["method"]) == string(want) }
}

func decodeParams(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var params map[string]json.RawMessage
	if err := json.Unmarshal(raw, &params); err != nil {
		t.Fatalf("params %s: %v", raw, err)
	}
	return params
}

func TestAdapterSubscribesForWakesOnlyWithCapability(t *testing.T) {
	t.Run("without buzz.wake", func(t *testing.T) {
		g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: ""})
		startACPHarness(t, g.url, `{"elicitation":{"form":{}},"_meta":{"buzz.wake":{"version":2}}}`)
		g.conn(t).none(t, protocol.MethodACPWakeSubscribe, 200*time.Millisecond)
	})
	t.Run("with buzz.wake and after reconnect", func(t *testing.T) {
		g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: ""})
		startACPHarness(t, g.url, wakeCaps)
		first := g.conn(t)
		req := first.next(t, protocol.MethodACPWakeSubscribe)
		if params := decodeParams(t, req.Params); string(params["agentId"]) != `"resolver"` {
			t.Fatalf("subscribe params = %s", req.Params)
		}
		first.drop() // The Gateway restarts while the adapter is idle.
		g.conn(t).next(t, protocol.MethodACPWakeSubscribe)
	})
}

func TestAdapterKeepsWorkingWhenOldGatewayRejectsSubscribe(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: protocol.ErrUnauthorized})
	startACPHarness(t, g.url, wakeCaps)
	conn := g.conn(t)
	conn.next(t, protocol.MethodACPWakeSubscribe)
	conn.none(t, protocol.MethodACPWakeSubscribe, 300*time.Millisecond)
}

func sendWake(conn *fakeGatewayConn, wakeID string, params string) {
	conn.send(protocol.NewEvent(protocol.EventACPWake, map[string]any{"wakeId": wakeID, "params": json.RawMessage(params)}))
}

func wakeParamsJSON(wakeID string) string {
	return `{"wakeId":"` + wakeID + `","reason":"input_required","thread":{"channelId":"c1","rootEventId":"e1"},"sessionId":"s-old","task":{"server":"archon","taskId":"run-1","status":"input_required","lastUpdatedAt":"2026-11-08T03:12:00Z"},"prompt":"[goclaw task update] ...","createdAt":"2026-11-08T03:12:01Z"}`
}

func TestAdapterSendsBuzzWakeOutsidePromptAndAcks(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: "", protocol.MethodACPWakeAck: ""})
	h := startACPHarness(t, g.url, wakeCaps)
	conn := g.conn(t)
	conn.next(t, protocol.MethodACPWakeSubscribe)

	params := wakeParamsJSON("w_1")
	sendWake(conn, "w_1", params)
	request := h.waitFrame(t, methodIs(MethodBuzzWake))
	if string(request["params"]) != params {
		t.Fatalf("_buzz/wake params = %s, want %s unchanged", request["params"], params)
	}
	h.write(t, `{"jsonrpc":"2.0","id":`+string(request["id"])+`,"result":{"outcome":"accepted"}}`+"\n")
	ack := conn.next(t, protocol.MethodACPWakeAck)
	if string(ack.Params) != `{"outcome":"accepted","wakeId":"w_1"}` {
		t.Fatalf("ack params = %s", ack.Params)
	}
}

func TestAdapterMapsMethodNotFoundToUnsupported(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: "", protocol.MethodACPWakeAck: ""})
	h := startACPHarness(t, g.url, wakeCaps)
	conn := g.conn(t)
	conn.next(t, protocol.MethodACPWakeSubscribe)
	sendWake(conn, "w_2", wakeParamsJSON("w_2"))
	request := h.waitFrame(t, methodIs(MethodBuzzWake))
	h.write(t, `{"jsonrpc":"2.0","id":`+string(request["id"])+`,"error":{"code":-32601,"message":"Method not found"}}`+"\n")
	if ack := conn.next(t, protocol.MethodACPWakeAck); string(ack.Params) != `{"outcome":"unsupported","wakeId":"w_2"}` {
		t.Fatalf("ack params = %s", ack.Params)
	}
}

func TestAdapterMapsBusyAndTimeout(t *testing.T) {
	cases := []struct {
		name   string
		result string
		rpcErr *RPCError
		err    error
		want   string
	}{
		{"accepted", `{"outcome":"accepted"}`, nil, nil, protocol.ACPWakeOutcomeAccepted},
		{"duplicate", `{"outcome":"duplicate"}`, nil, nil, protocol.ACPWakeOutcomeDuplicate},
		{"busy", `{"outcome":"busy"}`, nil, nil, protocol.ACPWakeOutcomeBusy},
		{"unknown outcome", `{"outcome":"later"}`, nil, nil, protocol.ACPWakeOutcomeFailed},
		{"not an object", `"accepted"`, nil, nil, protocol.ACPWakeOutcomeFailed},
		{"method not found", ``, &RPCError{Code: CodeMethodNotFound}, nil, protocol.ACPWakeOutcomeUnsupported},
		{"invalid params", ``, &RPCError{Code: CodeInvalidParams}, nil, protocol.ACPWakeOutcomeFailed},
		{"timeout", ``, nil, context.DeadlineExceeded, protocol.ACPWakeOutcomeFailed},
		{"transport closed", ``, nil, ErrTransportClosed, protocol.ACPWakeOutcomeFailed},
	}
	for _, c := range cases {
		if got := wakeOutcome(json.RawMessage(c.result), c.rpcErr, c.err); got != c.want {
			t.Errorf("%s: outcome = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestAdapterAcksUnsupportedWithoutClientWake(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeAck: ""})
	h := startACPHarness(t, g.url, `{}`)
	conn := g.conn(t)
	sendWake(conn, "w_3", wakeParamsJSON("w_3"))
	if ack := conn.next(t, protocol.MethodACPWakeAck); string(ack.Params) != `{"outcome":"unsupported","wakeId":"w_3"}` {
		t.Fatalf("ack params = %s", ack.Params)
	}
	if strings.Contains(string(h.output.Bytes()), MethodBuzzWake) {
		t.Fatal("_buzz/wake was sent to a client without buzz.wake")
	}
}

func TestAdapterAcksFailedForMismatchedWakeID(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: "", protocol.MethodACPWakeAck: ""})
	startACPHarness(t, g.url, wakeCaps)
	conn := g.conn(t)
	conn.next(t, protocol.MethodACPWakeSubscribe)
	sendWake(conn, "w_4", wakeParamsJSON("w_other"))
	if ack := conn.next(t, protocol.MethodACPWakeAck); string(ack.Params) != `{"outcome":"failed","wakeId":"w_4"}` {
		t.Fatalf("ack params = %s", ack.Params)
	}
}

// startPrompt opens a session and a prompt with a thread and returns the
// ACP session id and the chat.send request.
func startPrompt(t *testing.T, h *acpHarness, conn *fakeGatewayConn, thread string) (string, protocol.RequestFrame, ChatSendRequest) {
	t.Helper()
	h.write(t, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":`+strconv.Quote(h.cwd)+`,"mcpServers":[]}}`+"\n")
	created := h.waitFrame(t, func(f map[string]json.RawMessage) bool { return string(f["id"]) == "2" })
	var result NewSessionResult
	if err := json.Unmarshal(created["result"], &result); err != nil || result.SessionID == "" {
		t.Fatalf("session/new = %v", created)
	}
	h.write(t, `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"`+result.SessionID+`","prompt":[{"type":"text","text":"hi"}],"_meta":{"buzz.thread":`+thread+`}}}`+"\n")
	req := conn.next(t, protocol.MethodChatSend)
	var send ChatSendRequest
	if err := json.Unmarshal(req.Params, &send); err != nil {
		t.Fatal(err)
	}
	return result.SessionID, req, send
}

func TestAdapterSendsACPClientOnChatSend(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: ""})
	h := startACPHarness(t, g.url, wakeCaps)
	conn := g.conn(t)
	thread := `{"channelId":"c1","rootEventId":"e1"}`
	sessionID, req, send := startPrompt(t, h, conn, thread)
	want := ChatACPClient{ElicitationForm: true, Wake: true, Thread: json.RawMessage(thread), ACPSessionID: sessionID}
	if send.ACPClient == nil || send.ACPClient.ElicitationForm != want.ElicitationForm || send.ACPClient.Wake != want.Wake ||
		string(send.ACPClient.Thread) != thread || send.ACPClient.ACPSessionID != sessionID {
		t.Fatalf("acpClient = %+v", send.ACPClient)
	}
	conn.send(protocol.NewOKResponse(req.ID, map[string]any{"content": ""}))
	h.waitFrame(t, func(f map[string]json.RawMessage) bool { return string(f["id"]) == "3" })
}

func TestAdapterRelaysInputRequestAsElicitationCreate(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: "", protocol.MethodACPInputRespond: ""})
	h := startACPHarness(t, g.url, wakeCaps)
	conn := g.conn(t)
	sessionID, chatReq, send := startPrompt(t, h, conn, `{"channelId":"c1","rootEventId":"e1"}`)

	schema := `{"type":"object","properties":{"decision":{"type":"string","enum":["approve","reject"]},"comment":{"type":"string"}},"required":["decision"]}`
	conn.send(protocol.NewEvent(protocol.EventAgent, map[string]any{"type": protocol.AgentEventRunStarted, "sessionKey": send.SessionKey, "runId": "run-1", "acpGeneration": send.Generation}))
	conn.send(protocol.NewEvent(protocol.EventACPInputRequest, map[string]any{
		"sessionKey": send.SessionKey, "acpGeneration": send.Generation, "requestId": "req-9",
		"toolCallId": "toolu_01ABC", "mode": "form", "message": "Approve the merge?", "requestedSchema": json.RawMessage(schema),
	}))
	request := h.waitFrame(t, methodIs(MethodElicitationCreate))
	params := decodeParams(t, request["params"])
	if string(params["sessionId"]) != strconv.Quote(sessionID) || string(params["toolCallId"]) != `"toolu_01ABC"` ||
		string(params["mode"]) != `"form"` || string(params["message"]) != `"Approve the merge?"` || string(params["requestedSchema"]) != schema {
		t.Fatalf("elicitation/create params = %s", request["params"])
	}
	content := `{"decision":"approve","comment":"looks good"}`
	h.write(t, `{"jsonrpc":"2.0","id":`+string(request["id"])+`,"result":{"action":"accept","content":`+content+`}}`+"\n")
	respond := conn.next(t, protocol.MethodACPInputRespond)
	reply := decodeParams(t, respond.Params)
	if string(reply["requestId"]) != `"req-9"` || string(reply["action"]) != `"accept"` || string(reply["content"]) != content ||
		string(reply["sessionKey"]) != strconv.Quote(send.SessionKey) || string(reply["acpGeneration"]) != strconv.FormatUint(send.Generation, 10) {
		t.Fatalf("acp.input.respond params = %s", respond.Params)
	}
	conn.send(protocol.NewOKResponse(chatReq.ID, map[string]any{"content": ""}))
	h.waitFrame(t, func(f map[string]json.RawMessage) bool { return string(f["id"]) == "3" })
}

func TestAdapterAnswersCancelOnClientError(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: "", protocol.MethodACPInputRespond: ""})
	h := startACPHarness(t, g.url, wakeCaps)
	conn := g.conn(t)
	_, chatReq, send := startPrompt(t, h, conn, `{"channelId":"c1"}`)
	conn.send(protocol.NewEvent(protocol.EventACPInputRequest, map[string]any{
		"sessionKey": send.SessionKey, "acpGeneration": send.Generation, "requestId": "req-1",
		"toolCallId": "t1", "mode": "form", "message": "Q", "requestedSchema": json.RawMessage(`{"type":"object"}`),
	}))
	request := h.waitFrame(t, methodIs(MethodElicitationCreate))
	h.write(t, `{"jsonrpc":"2.0","id":`+string(request["id"])+`,"error":{"code":-32603,"message":"boom"}}`+"\n")
	reply := decodeParams(t, conn.next(t, protocol.MethodACPInputRespond).Params)
	if string(reply["action"]) != `"cancel"` || reply["content"] != nil {
		t.Fatalf("reply = %v", reply)
	}
	conn.send(protocol.NewOKResponse(chatReq.ID, map[string]any{"content": ""}))
}

func TestAdapterIgnoresInputRequestOfOtherGeneration(t *testing.T) {
	g := newFakeGateway(t, map[string]string{protocol.MethodACPWakeSubscribe: "", protocol.MethodACPInputRespond: ""})
	h := startACPHarness(t, g.url, wakeCaps)
	conn := g.conn(t)
	_, chatReq, send := startPrompt(t, h, conn, `{"channelId":"c1"}`)
	// The subscription routes by generation, so an event of another
	// generation never reaches the prompt.
	conn.send(protocol.NewEvent(protocol.EventACPInputRequest, map[string]any{
		"sessionKey": send.SessionKey, "acpGeneration": send.Generation + 1, "requestId": "req-1",
		"mode": "form", "message": "Q", "requestedSchema": json.RawMessage(`{"type":"object"}`),
	}))
	conn.none(t, protocol.MethodACPInputRespond, 200*time.Millisecond)
	if strings.Contains(string(h.output.Bytes()), MethodElicitationCreate) {
		t.Fatal("elicitation/create sent for another generation")
	}
	conn.send(protocol.NewOKResponse(chatReq.ID, map[string]any{"content": ""}))
}

func TestPromptWithLargestThreadFitsGatewayFrame(t *testing.T) {
	thread := `{"channelId":"` + strings.Repeat("c", MaxThreadContextBytes-16) + `"}`
	accepted := 0
	for n := MaxGatewayFrameBytes/2 - 4096; n <= MaxGatewayFrameBytes/2; n += 128 {
		text := strings.Repeat(`"`, n)
		raw, _ := json.Marshal(map[string]any{
			"sessionId": "s1", "prompt": []any{map[string]string{"type": "text", "text": text}},
			"_meta": map[string]json.RawMessage{ThreadContextKey: json.RawMessage(thread)},
		})
		params, rpcErr := DecodePromptParams(raw)
		if rpcErr != nil {
			continue
		}
		accepted++
		frame, err := json.Marshal(protocol.RequestFrame{Type: protocol.FrameTypeRequest, ID: "acp-1234567890", Method: protocol.MethodChatSend,
			Params: mustMarshal(t, ChatSendRequest{
				AgentID: "resolver", SessionKey: "agent:resolver:ws:direct:00000000-0000-0000-0000-000000000000",
				Message: joinPrompt(params.Prompt), Stream: true, Generation: 1 << 62, TerminalMode: "metadata",
				ACPClient: &ChatACPClient{ElicitationForm: true, Wake: true, Thread: params.Thread, ACPSessionID: "00000000-0000-0000-0000-000000000000"},
			})})
		if err != nil {
			t.Fatal(err)
		}
		if len(frame) > MaxGatewayFrameBytes {
			t.Fatalf("accepted prompt gives a chat.send frame of %d bytes, over %d", len(frame), MaxGatewayFrameBytes)
		}
	}
	if accepted == 0 {
		t.Fatal("no prompt near the limit was accepted")
	}
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestElicitationAnswerKeepsContentUnchanged(t *testing.T) {
	action, content := elicitationAnswer(json.RawMessage(`{"action":"accept","content":{"b":2,"a":[1, 2]}}`))
	if action != "accept" || string(content) != `{"b":2,"a":[1, 2]}` {
		t.Fatalf("answer = %q %s", action, content)
	}
	for _, raw := range []string{`{"action":"accept"}`, `{"action":"accept","content":[1]}`, `{"action":"later"}`, `[]`} {
		if action, _ := elicitationAnswer(json.RawMessage(raw)); action != "cancel" {
			t.Errorf("%s: action = %q, want cancel", raw, action)
		}
	}
	if action, content := elicitationAnswer(json.RawMessage(`{"action":"decline","content":{"x":1}}`)); action != "decline" || content != nil {
		t.Fatalf("decline = %q %s", action, content)
	}
}

func TestGatewayResponseErrorIsTyped(t *testing.T) {
	var target *GatewayResponseError
	if err := gatewayResponseError(&protocol.ErrorShape{Code: protocol.ErrNotFound}); !errors.As(err, &target) || target.Code != protocol.ErrNotFound {
		t.Fatalf("error = %v", err)
	}
}

func TestAdapterResubscribesAfterGatewayRefusesConnect(t *testing.T) {
	var conns atomic.Int64
	subscribed := make(chan int64, 4)
	dropFirst := make(chan struct{})
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		n := conns.Add(1)
		if n == 2 {
			// The Gateway restarts and refuses the first connect.
			var req protocol.RequestFrame
			mustReadJSON(t, conn, &req)
			mustWriteJSON(t, conn, protocol.NewErrorResponse(req.ID, protocol.ErrUnavailable, "starting"))
			return
		}
		authenticateGatewayTestClient(t, conn)
		for {
			var req protocol.RequestFrame
			if n == 1 {
				// Answer the first subscribe, then drop the socket.
				mustReadJSON(t, conn, &req)
				mustWriteJSON(t, conn, protocol.NewOKResponse(req.ID, map[string]any{}))
				subscribed <- n
				<-dropFirst
				return
			}
			if conn.ReadJSON(&req) != nil {
				return
			}
			if req.Method == protocol.MethodACPWakeSubscribe {
				mustWriteJSON(t, conn, protocol.NewOKResponse(req.ID, map[string]any{}))
				subscribed <- n
			}
		}
	})
	startACPHarness(t, url, wakeCaps)
	if got := <-subscribed; got != 1 {
		t.Fatalf("first subscribe on connection %d", got)
	}
	close(dropFirst)
	select {
	case got := <-subscribed:
		if got < 3 {
			t.Fatalf("subscribe on connection %d, want a connection after the refused one", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the adapter did not subscribe again after a refused connect")
	}
}
