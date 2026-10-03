package acpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransportBuzzInitializeNegotiatesACPv1(t *testing.T) {
	t.Parallel()

	input := `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":2,"clientCapabilities":{"auth":{"terminal":true}},"clientInfo":{"name":"buzz-acp","version":"1.2.3"}}}` + "\n"
	lines, err := serveTranscript(input, Handler{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if len(lines) != 1 {
		t.Fatalf("response count = %d, want 1", len(lines))
	}

	var got struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  struct {
			ProtocolVersion   int `json:"protocolVersion"`
			AgentCapabilities struct {
				LoadSession        bool `json:"loadSession"`
				PromptCapabilities struct {
					Image           bool `json:"image"`
					Audio           bool `json:"audio"`
					EmbeddedContext bool `json:"embeddedContext"`
				} `json:"promptCapabilities"`
				MCPCapabilities struct {
					HTTP bool `json:"http"`
					SSE  bool `json:"sse"`
				} `json:"mcpCapabilities"`
			} `json:"agentCapabilities"`
			AgentInfo   AgentInfo `json:"agentInfo"`
			AuthMethods []any     `json:"authMethods"`
		} `json:"result"`
	}
	if err := json.Unmarshal(lines[0], &got); err != nil {
		t.Fatal(err)
	}
	if got.JSONRPC != "2.0" || string(got.ID) != "0" || got.Result.ProtocolVersion != ProtocolVersion {
		t.Fatalf("initialize response = %s", lines[0])
	}
	if got.Result.AgentInfo != (AgentInfo{Name: "goclaw", Title: "GoClaw", Version: "test-version"}) {
		t.Fatalf("agentInfo = %#v", got.Result.AgentInfo)
	}
	if got.Result.AgentCapabilities.LoadSession || got.Result.AgentCapabilities.PromptCapabilities.Image || got.Result.AgentCapabilities.PromptCapabilities.Audio || got.Result.AgentCapabilities.PromptCapabilities.EmbeddedContext || got.Result.AgentCapabilities.MCPCapabilities.HTTP || got.Result.AgentCapabilities.MCPCapabilities.SSE {
		t.Fatalf("unsupported capability advertised: %s", lines[0])
	}
	if got.Result.AuthMethods == nil || len(got.Result.AuthMethods) != 0 {
		t.Fatalf("authMethods = %#v, want []", got.Result.AuthMethods)
	}
}

func TestTransportRejectsRequestBeforeInitialize(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	handler := Handler{NewSession: func(context.Context, NewSessionParams) (NewSessionResult, error) {
		calls.Add(1)
		return NewSessionResult{SessionID: "unexpected"}, nil
	}}
	input := `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}` + "\n"
	lines, err := serveTranscript(input, handler)
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	assertErrorCode(t, lines, 0, CodeInvalidRequest)
	if calls.Load() != 0 {
		t.Fatalf("handler calls = %d, want 0", calls.Load())
	}
}

func TestTransportRejectsSecondInitialize(t *testing.T) {
	t.Parallel()

	input := initializeLine(`1`) + initializeLine(`2`)
	lines, err := serveTranscript(input, Handler{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("response count = %d, want 2", len(lines))
	}
	assertErrorCode(t, lines, 1, CodeInvalidRequest)
}

func TestTransportPreservesStringAndIntegerIDs(t *testing.T) {
	t.Parallel()

	handler := Handler{NewSession: func(context.Context, NewSessionParams) (NewSessionResult, error) {
		return NewSessionResult{SessionID: "session-1"}, nil
	}}
	input := initializeLine(`"init-1"`) + `{"jsonrpc":"2.0","id":42,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}` + "\n"
	lines, err := serveTranscript(input, handler)
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("response count = %d, want 2", len(lines))
	}
	if got := responseID(t, lines[0]); got != `"init-1"` {
		t.Fatalf("string response id = %s", got)
	}
	if got := responseID(t, lines[1]); got != `42` {
		t.Fatalf("integer response id = %s", got)
	}
}

func TestTransportParseErrorRecovers(t *testing.T) {
	t.Parallel()

	lines, err := serveTranscript("{bad json}\n"+initializeLine(`1`), Handler{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("response count = %d, want 2", len(lines))
	}
	assertErrorCode(t, lines, 0, CodeParseError)
	if got := responseID(t, lines[0]); got != "null" {
		t.Fatalf("parse error id = %s, want null", got)
	}
	if got := responseID(t, lines[1]); got != "1" {
		t.Fatalf("recovery response id = %s", got)
	}
}

func TestTransportNotificationHasNoResponse(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	handler := Handler{Cancel: func(context.Context, CancelParams) { calls.Add(1) }}
	input := initializeLine(`1`) + `{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"missing"}}` + "\n"
	lines, err := serveTranscript(input, handler)
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if len(lines) != 1 {
		t.Fatalf("response count = %d, want only initialize response", len(lines))
	}
	if calls.Load() != 0 {
		t.Fatalf("cancel calls = %d, want 0 for unknown session", calls.Load())
	}
}

func TestTransportSessionLifecycle(t *testing.T) {
	t.Parallel()

	var promptCalls atomic.Int32
	var cancelCalls atomic.Int32
	handler := Handler{
		NewSession: func(context.Context, NewSessionParams) (NewSessionResult, error) {
			return NewSessionResult{SessionID: "session-1"}, nil
		},
		Prompt: func(_ context.Context, params PromptParams) (PromptResult, error) {
			promptCalls.Add(1)
			if len(params.Prompt) != 1 || params.Prompt[0].Text != "hello" {
				return PromptResult{}, errors.New("unexpected prompt")
			}
			return PromptResult{StopReason: "end_turn"}, nil
		},
		Cancel: func(context.Context, CancelParams) { cancelCalls.Add(1) },
	}
	reader, input := io.Pipe()
	output := &lockedBuffer{}
	transport := NewTransport(reader, output, handler, "test-version")
	done := make(chan error, 1)
	go func() { done <- transport.Serve(context.Background()) }()

	if _, err := io.WriteString(input, initializeLine(`1`)); err != nil {
		t.Fatal(err)
	}
	waitForLines(t, output, 1)
	if _, err := io.WriteString(input, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	waitForLines(t, output, 2)
	if _, err := io.WriteString(input, `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[{"type":"text","text":"hello"}]}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	waitForLines(t, output, 3)
	if _, err := io.WriteString(input, `{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"session-1"}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if promptCalls.Load() != 1 || cancelCalls.Load() != 1 {
		t.Fatalf("handler calls: prompt=%d cancel=%d", promptCalls.Load(), cancelCalls.Load())
	}
	if lines := splitLines(output.Bytes()); len(lines) != 3 {
		t.Fatalf("response count = %d, want 3", len(lines))
	}
}

func TestTransportRejectsOversizeLine(t *testing.T) {
	t.Parallel()

	input := strings.Repeat("x", MaxInputLineBytes+1) + "\n"
	var output bytes.Buffer
	transport := NewTransport(strings.NewReader(input), &output, Handler{}, "test-version")
	err := transport.Serve(context.Background())
	if !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("Serve() error = %v, want ErrInputTooLarge", err)
	}
	if output.Len() != 0 {
		t.Fatalf("oversize input produced output: %q", output.String())
	}
}

func TestTransportSerializesConcurrentWrites(t *testing.T) {
	t.Parallel()

	reader, input := io.Pipe()
	output := &lockedBuffer{}
	transport := NewTransport(reader, output, Handler{}, "test-version")
	done := make(chan error, 1)
	go func() { done <- transport.Serve(context.Background()) }()

	if _, err := io.WriteString(input, initializeLine(`1`)); err != nil {
		t.Fatal(err)
	}
	waitForLines(t, output, 1)

	const count = 64
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := transport.SessionUpdate(context.Background(), SessionUpdateParams{
				SessionID: "session-1",
				Update:    SessionUpdate{SessionUpdate: "agent_message_chunk", Content: TextContent{Type: "text", Text: strings.Repeat("x", i+1)}},
			})
			if err != nil {
				t.Errorf("SessionUpdate() error = %v", err)
			}
		}(i)
	}
	wg.Wait()
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}

	lines := splitLines(output.Bytes())
	if len(lines) != count+1 {
		t.Fatalf("frame count = %d, want %d", len(lines), count+1)
	}
	for i, line := range lines {
		if !json.Valid(line) {
			t.Fatalf("line %d is interleaved or invalid: %q", i, line)
		}
	}
}

func TestTransportBackpressureTerminates(t *testing.T) {
	t.Parallel()

	writer := newBlockingWriter()
	input := strings.Repeat("{bad}\n", MaxOutputQueue+2)
	transport := NewTransport(strings.NewReader(input), writer, Handler{}, "test-version")
	done := make(chan error, 1)
	go func() { done <- transport.Serve(context.Background()) }()

	<-writer.entered
	select {
	case err := <-done:
		if !errors.Is(err, ErrOutputBackpressure) {
			t.Fatalf("Serve() error = %v, want ErrOutputBackpressure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not terminate after output backpressure")
	}
	close(writer.release)
}

func TestTransportBrokenWriterTerminates(t *testing.T) {
	t.Parallel()

	transport := NewTransport(strings.NewReader(initializeLine(`1`)), errorWriter{}, Handler{}, "test-version")
	err := transport.Serve(context.Background())
	if !errors.Is(err, ErrTransportWrite) {
		t.Fatalf("Serve() error = %v, want ErrTransportWrite", err)
	}
}

func TestTransportNeverLogsPayloadOrMCPEnv(t *testing.T) {
	const canary = "DO_NOT_EXPOSE_MCP_ENV"
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	handler := Handler{NewSession: func(context.Context, NewSessionParams) (NewSessionResult, error) {
		return NewSessionResult{}, errors.New(canary)
	}}
	input := initializeLine(`1`) + `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[{"name":"tools","command":"/bin/tools","args":[],"env":[{"name":"TOKEN","value":"` + canary + `"}]}]}}` + "\n"
	lines, err := serveTranscript(input, handler)
	if err != nil && strings.Contains(err.Error(), canary) {
		t.Fatalf("Serve() exposed payload in error: %v", err)
	}
	for _, line := range lines {
		if bytes.Contains(line, []byte(canary)) {
			t.Fatalf("response exposed payload: %s", line)
		}
	}
	if strings.Contains(logs.String(), canary) {
		t.Fatalf("log exposed payload: %s", logs.String())
	}
}

func TestTransportRejectsInvalidEnvelopeUnknownMethodAndParams(t *testing.T) {
	t.Parallel()

	input := `[]` + "\n" +
		`{"jsonrpc":"1.0","id":1,"method":"initialize","params":{}}` + "\n" +
		initializeLine(`2`) +
		`{"jsonrpc":"2.0","id":3,"method":"session/load","params":{}}` + "\n" +
		`{"jsonrpc":"2.0","id":4,"method":"session/new","params":{"cwd":"relative","mcpServers":[]}}` + "\n" +
		`{"jsonrpc":"2.0","id":1.5,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}` + "\n"
	lines, err := serveTranscript(input, Handler{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	assertErrorCode(t, lines, 0, CodeInvalidRequest)
	assertErrorCode(t, lines, 1, CodeInvalidRequest)
	assertErrorCode(t, lines, 3, CodeMethodNotFound)
	assertErrorCode(t, lines, 4, CodeInvalidParams)
	assertErrorCode(t, lines, 5, CodeInvalidRequest)
}

func serveTranscript(input string, handler Handler) ([][]byte, error) {
	var output bytes.Buffer
	transport := NewTransport(strings.NewReader(input), &output, handler, "test-version")
	err := transport.Serve(context.Background())
	return splitLines(output.Bytes()), err
}

func initializeLine(id string) string {
	return `{"jsonrpc":"2.0","id":` + id + `,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}` + "\n"
}

func splitLines(data []byte) [][]byte {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil
	}
	return bytes.Split(data, []byte("\n"))
}

func responseID(t *testing.T, line []byte) string {
	t.Helper()
	var envelope struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		t.Fatal(err)
	}
	return string(envelope.ID)
}

func assertErrorCode(t *testing.T, lines [][]byte, index, want int) {
	t.Helper()
	if len(lines) <= index {
		t.Fatalf("missing response %d in %#q", index, lines)
	}
	var envelope struct {
		Error *RPCError `json:"error"`
	}
	if err := json.Unmarshal(lines[index], &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error == nil || envelope.Error.Code != want {
		t.Fatalf("response %d error = %#v, want code %d; frame=%s", index, envelope.Error, want, lines[index])
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.buf.Bytes())
}

func waitForLines(t *testing.T, output *lockedBuffer, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(splitLines(output.Bytes())) >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d output lines", count)
}

type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestTransportCancellationInterruptsIdleInput(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- NewTransport(reader, io.Discard, Handler{}, "test").Serve(ctx)
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not stop while stdin was idle")
	}
}
