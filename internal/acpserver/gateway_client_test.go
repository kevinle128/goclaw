package acpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

func TestGatewayClientConnectAuthenticatesBeforeUse(t *testing.T) {
	var first protocol.RequestFrame
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		mustReadJSON(t, conn, &first)
		mustWriteJSON(t, conn, protocol.NewOKResponse(first.ID, map[string]any{
			"user_id": "user-1", "tenant_id": "tenant-1", "role": "operator",
			"is_owner": false, "is_master_scope": false,
			"server": map[string]any{"name": "goclaw", "version": "test"},
		}))
		<-time.After(100 * time.Millisecond)
	})

	client := NewGatewayClient(GatewayClientConfig{URL: url, Token: "secret", UserID: "hint"})
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer client.Close()
	if first.Method != protocol.MethodConnect {
		t.Fatalf("first method = %q, want %q", first.Method, protocol.MethodConnect)
	}
	identity := client.Identity()
	if identity.UserID != "user-1" || identity.TenantID != "tenant-1" || identity.Role != "operator" || identity.IsOwner || identity.IsMasterScope || identity.ServerName != "goclaw" {
		t.Fatalf("Identity() = %+v", identity)
	}
}

func TestGatewayClientRejectsUnboundIdentity(t *testing.T) {
	for _, payload := range []map[string]any{
		{"user_id": "", "tenant_id": "tenant-1"},
		{"user_id": "user-1", "tenant_id": ""},
	} {
		t.Run(payload["tenant_id"].(string)+payload["user_id"].(string), func(t *testing.T) {
			url := gatewayTestServer(t, func(conn *websocket.Conn) {
				var req protocol.RequestFrame
				mustReadJSON(t, conn, &req)
				mustWriteJSON(t, conn, protocol.NewOKResponse(req.ID, payload))
			})
			client := NewGatewayClient(GatewayClientConfig{URL: url, Token: "secret"})
			if err := client.Connect(context.Background()); !errors.Is(err, ErrUnboundGatewayIdentity) {
				t.Fatalf("Connect() error = %v, want %v", err, ErrUnboundGatewayIdentity)
			}
		})
	}
}

func TestGatewayClientDispatchesConcurrentResponses(t *testing.T) {
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		var first, second protocol.RequestFrame
		mustReadJSON(t, conn, &first)
		mustReadJSON(t, conn, &second)
		mustWriteJSON(t, conn, protocol.NewOKResponse(second.ID, map[string]string{"value": second.Method}))
		mustWriteJSON(t, conn, protocol.NewOKResponse(first.ID, map[string]string{"value": first.Method}))
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()

	start := make(chan struct{})
	results := make(chan string, 2)
	for _, method := range []string{"first", "second"} {
		method := method
		go func() {
			<-start
			var result struct {
				Value string `json:"value"`
			}
			if err := client.Call(context.Background(), method, nil, &result); err != nil {
				results <- "error:" + err.Error()
				return
			}
			results <- result.Value
		}()
	}
	close(start)
	got := map[string]bool{<-results: true, <-results: true}
	if !got["first"] || !got["second"] {
		t.Fatalf("correlated results = %v", got)
	}
}

func TestGatewayClientWriteDeadlineAndSerialization(t *testing.T) {
	const calls = 24
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		for range calls {
			var req protocol.RequestFrame
			mustReadJSON(t, conn, &req)
			mustWriteJSON(t, conn, protocol.NewOKResponse(req.ID, map[string]string{"method": req.Method}))
		}
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()
	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			var response struct {
				Method string `json:"method"`
			}
			method := fmt.Sprintf("method-%d", index)
			if err := client.Call(context.Background(), method, nil, &response); err != nil {
				errs <- err
				return
			}
			if response.Method != method {
				errs <- fmt.Errorf("response method = %q, want %q", response.Method, method)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestGatewayClientRoutesEventsBySessionAndRun(t *testing.T) {
	sendEvents := make(chan struct{})
	keepOpen := make(chan struct{})
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		<-sendEvents
		for _, payload := range []map[string]any{
			{"type": protocol.AgentEventRunStarted, "sessionKey": "session-a", "runId": "run-a", "acpGeneration": 1},
			{"type": protocol.AgentEventRunStarted, "sessionKey": "session-b", "runId": "run-b", "acpGeneration": 1},
			{"type": "chunk", "sessionKey": "session-b", "runId": "run-a", "acpGeneration": 1},
			{"type": "chunk", "sessionKey": "session-a", "runId": "run-a", "acpGeneration": 1},
		} {
			mustWriteJSON(t, conn, protocol.NewEvent(protocol.EventAgent, payload))
		}
		<-keepOpen
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()
	subA, err := client.Subscribe("acp-a", "session-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	subB, err := client.Subscribe("acp-b", "session-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	close(sendEvents)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, sub := range []*GatewaySubscription{subA, subB} {
		if _, err := sub.Next(ctx); err != nil {
			t.Fatal(err)
		}
	}
	event, err := subA.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.SessionKey != "session-a" || event.RunID != "run-a" {
		t.Fatalf("sub A event = %+v", event)
	}
	short, shortCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shortCancel()
	if _, err := subB.Next(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sub B consumed cross-routed event: %v", err)
	}
	close(keepOpen)
}

func TestGatewayClientLearnsRunIDFromStartedEvent(t *testing.T) {
	keepOpen := make(chan struct{})
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		mustWriteJSON(t, conn, protocol.NewEvent(protocol.EventAgent, map[string]any{
			"type": protocol.AgentEventRunStarted, "sessionKey": "session-a", "runId": "run-a", "acpGeneration": 7,
		}))
		mustWriteJSON(t, conn, protocol.NewEvent(protocol.EventAgent, map[string]any{
			"type": "chunk", "sessionKey": "session-a", "runId": "run-b", "acpGeneration": 7,
		}))
		mustWriteJSON(t, conn, protocol.NewEvent(protocol.EventAgent, map[string]any{
			"type": "chunk", "sessionKey": "session-a", "runId": "run-a", "acpGeneration": 7,
		}))
		<-keepOpen
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()
	sub, err := client.Subscribe("acp-a", "session-a", 7)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if started.RunID != "run-a" || chunk.RunID != "run-a" {
		t.Fatalf("events = %+v, %+v", started, chunk)
	}
	close(keepOpen)
}

func TestGatewayClientRoutesACPToolGeneration(t *testing.T) {
	keepOpen := make(chan struct{})
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		mustWriteJSON(t, conn, protocol.NewEvent(protocol.EventACPToolCall, map[string]any{
			"sessionKey": "session-a", "generation": 7, "capabilityId": "cap-1", "callId": "call-1",
		}))
		<-keepOpen
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()
	subscription, err := client.Subscribe("acp-a", "session-a", 7)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Event != protocol.EventACPToolCall || event.Generation != 7 {
		t.Fatalf("event = %+v", event)
	}
	close(keepOpen)
}

func TestGatewayClientEventOverflowFailsTurn(t *testing.T) {
	sendEvents := make(chan struct{})
	keepOpen := make(chan struct{})
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		<-sendEvents
		for i := 0; i <= MaxGatewayEventQueue; i++ {
			mustWriteJSON(t, conn, protocol.NewEvent(protocol.EventAgent, map[string]any{
				"type": "chunk", "sessionKey": "session-a", "acpGeneration": 1, "index": i,
			}))
		}
		<-keepOpen
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()
	sub, err := client.Subscribe("acp-a", "session-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	close(sendEvents)
	select {
	case <-sub.done:
	case <-time.After(time.Second):
		t.Fatal("event overflow did not fail subscription")
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, ErrGatewayEventOverflow) {
		t.Fatalf("Next() error = %v, want %v", err, ErrGatewayEventOverflow)
	}
	close(keepOpen)
}

func TestGatewayClientPendingLimit(t *testing.T) {
	var writes atomic.Int64
	ready := make(chan struct{})
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		close(ready)
		for {
			var req protocol.RequestFrame
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			writes.Add(1)
		}
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()
	<-ready
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range MaxPendingGatewayRequests {
		go func() { _ = client.Call(ctx, "hold", nil, nil) }()
	}
	deadline := time.Now().Add(time.Second)
	for client.pendingCount() != MaxPendingGatewayRequests && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := client.Call(ctx, "overflow", nil, nil); !errors.Is(err, ErrGatewayPendingLimit) {
		t.Fatalf("Call() error = %v, want %v", err, ErrGatewayPendingLimit)
	}
	if got := writes.Load(); got > MaxPendingGatewayRequests {
		t.Fatalf("server observed %d writes, want at most %d", got, MaxPendingGatewayRequests)
	}
}

func TestGatewayClientDisconnectFailsPendingOnce(t *testing.T) {
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		var first, second protocol.RequestFrame
		mustReadJSON(t, conn, &first)
		mustReadJSON(t, conn, &second)
		_ = conn.Close()
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()
	errs := make(chan error, 2)
	go func() { errs <- client.Call(context.Background(), "first", nil, nil) }()
	go func() { errs <- client.Call(context.Background(), "second", nil, nil) }()
	for range 2 {
		if err := <-errs; err == nil {
			t.Fatal("Call() error = nil after disconnect")
		}
	}
	if got := client.pendingCount(); got != 0 {
		t.Fatalf("pending count = %d", got)
	}
}

func TestGatewayClientNoReplayAfterIndeterminateDisconnect(t *testing.T) {
	var sends atomic.Int64
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		var req protocol.RequestFrame
		mustReadJSON(t, conn, &req)
		if req.Method == protocol.MethodChatSend {
			sends.Add(1)
		}
		_ = conn.Close()
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()
	err := client.Call(context.Background(), protocol.MethodChatSend, map[string]string{"message": "hi"}, nil)
	if !errors.Is(err, ErrGatewayIndeterminate) {
		t.Fatalf("Call() error = %v, want %v", err, ErrGatewayIndeterminate)
	}
	time.Sleep(50 * time.Millisecond)
	if got := sends.Load(); got != 1 {
		t.Fatalf("chat.send count = %d, want 1", got)
	}
}

func TestGatewayClientCloseIsIdempotent(t *testing.T) {
	url := gatewayTestServer(t, func(conn *websocket.Conn) { authenticateGatewayTestClient(t, conn); <-time.After(time.Second) })
	client := connectGatewayTestClient(t, url)
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = client.Close() }()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close() blocked")
	}
}

func TestGatewayClientRejectsPlaintextRemoteURL(t *testing.T) {
	for _, tc := range []struct {
		raw string
		ok  bool
	}{
		{"ws://127.0.0.1:18790/ws", true},
		{"http://[::1]:18790/base", true},
		{"ws://192.0.2.1/ws", false},
		{"http://localhost:18790", false},
		{"ws://gateway.example/ws", false},
		{"wss://gateway.example/ws", true},
		{"https://gateway.example/base", true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			_, err := normalizeAndValidateGatewayURL(tc.raw)
			if (err == nil) != tc.ok {
				t.Fatalf("error = %v, ok = %v", err, tc.ok)
			}
		})
	}
}

func TestGatewayClientPreservesResolvedWebSocketPath(t *testing.T) {
	got, err := normalizeAndValidateGatewayURL("wss://gateway.example/base/ws")
	if err != nil {
		t.Fatal(err)
	}
	if got != "wss://gateway.example/base/ws" {
		t.Fatalf("URL = %q", got)
	}
}

func TestGatewayClientFrameBudget(t *testing.T) {
	var appFrames atomic.Int64
	url := gatewayTestServer(t, func(conn *websocket.Conn) {
		authenticateGatewayTestClient(t, conn)
		for {
			var req protocol.RequestFrame
			if conn.ReadJSON(&req) != nil {
				return
			}
			appFrames.Add(1)
		}
	})
	client := connectGatewayTestClient(t, url)
	defer client.Close()
	err := client.Call(context.Background(), "large", map[string]string{"value": strings.Repeat("x", MaxGatewayFrameBytes)}, nil)
	if !errors.Is(err, ErrGatewayFrameTooLarge) {
		t.Fatalf("Call() error = %v", err)
	}
	if got := appFrames.Load(); got != 0 {
		t.Fatalf("application frames = %d", got)
	}
}

func gatewayTestServer(t *testing.T, script func(*websocket.Conn)) string {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		script(conn)
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func authenticateGatewayTestClient(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	var req protocol.RequestFrame
	mustReadJSON(t, conn, &req)
	if req.Method != protocol.MethodConnect {
		t.Errorf("first method = %q", req.Method)
	}
	mustWriteJSON(t, conn, protocol.NewOKResponse(req.ID, map[string]any{"user_id": "user-1", "tenant_id": "tenant-1"}))
}

func connectGatewayTestClient(t *testing.T, url string) *GatewayClient {
	t.Helper()
	client := NewGatewayClient(GatewayClientConfig{URL: url, Token: "secret"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	return client
}

func mustReadJSON(t *testing.T, conn *websocket.Conn, value any) {
	t.Helper()
	if err := conn.ReadJSON(value); err != nil {
		t.Errorf("ReadJSON() error = %v", err)
	}
}

func mustWriteJSON(t *testing.T, conn *websocket.Conn, value any) {
	t.Helper()
	if err := conn.WriteJSON(value); err != nil {
		t.Errorf("WriteJSON() error = %v", err)
	}
}
