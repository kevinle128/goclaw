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

type pipeTransport struct {
	transport *Transport
	input     *io.PipeWriter
	output    *lockedBuffer
	done      chan error
}

func startPipeTransport(t *testing.T, handler Handler) *pipeTransport {
	t.Helper()
	reader, input := io.Pipe()
	output := &lockedBuffer{}
	transport := NewTransport(reader, output, handler, "test-version")
	done := make(chan error, 1)
	go func() { done <- transport.Serve(context.Background()) }()
	t.Cleanup(func() { _ = input.Close() })
	p := &pipeTransport{transport: transport, input: input, output: output, done: done}
	p.write(t, initializeLine(`1`))
	waitForLines(t, output, 1)
	return p
}

func (p *pipeTransport) write(t *testing.T, line string) {
	t.Helper()
	if _, err := io.WriteString(p.input, line); err != nil {
		t.Fatal(err)
	}
}

func (p *pipeTransport) closeAndWait(t *testing.T) {
	t.Helper()
	if err := p.input.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-p.done:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not stop after input close")
	}
}

type outboundFrame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func readOutboundFrame(t *testing.T, output *lockedBuffer, index int) outboundFrame {
	t.Helper()
	waitForLines(t, output, index+1)
	var frame outboundFrame
	if err := json.Unmarshal(splitLines(output.Bytes())[index], &frame); err != nil {
		t.Fatal(err)
	}
	var id string
	if frame.JSONRPC != "2.0" || json.Unmarshal(frame.ID, &id) != nil || !strings.HasPrefix(id, "goclaw-") || frame.Method == "" {
		t.Fatalf("outbound frame = %s", splitLines(output.Bytes())[index])
	}
	return frame
}

type requestOutcome struct {
	result json.RawMessage
	rpcErr *RPCError
	err    error
}

func waitOutcome(t *testing.T, outcomes <-chan requestOutcome) requestOutcome {
	t.Helper()
	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(2 * time.Second):
		t.Fatal("Request() did not return")
		return requestOutcome{}
	}
}

func TestTransportRoutesClientResponseToPendingRequest(t *testing.T) {
	t.Parallel()

	var transport atomic.Pointer[Transport]
	outcomes := make(chan requestOutcome, 1)
	handler := Handler{
		NewSession: func(context.Context, NewSessionParams) (NewSessionResult, error) {
			return NewSessionResult{SessionID: "session-1"}, nil
		},
		Prompt: func(ctx context.Context, _ PromptParams) (PromptResult, error) {
			result, rpcErr, err := transport.Load().Request(ctx, "_buzz/wake", map[string]string{"wakeId": "w_1"})
			outcomes <- requestOutcome{result, rpcErr, err}
			return PromptResult{StopReason: "end_turn"}, nil
		},
	}
	p := startPipeTransport(t, handler)
	transport.Store(p.transport)
	p.write(t, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}`+"\n")
	waitForLines(t, p.output, 2)
	p.write(t, `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[{"type":"text","text":"hello"}]}}`+"\n")

	frame := readOutboundFrame(t, p.output, 2)
	if frame.Method != "_buzz/wake" || string(frame.Params) != `{"wakeId":"w_1"}` {
		t.Fatalf("outbound request = %#v", frame)
	}
	p.write(t, `{"jsonrpc":"2.0","id":`+string(frame.ID)+`,"result":{"outcome":"accepted"}}`+"\n")
	outcome := waitOutcome(t, outcomes)
	if outcome.err != nil || outcome.rpcErr != nil || string(outcome.result) != `{"outcome":"accepted"}` {
		t.Fatalf("Request() = %s, %v, %v", outcome.result, outcome.rpcErr, outcome.err)
	}
	waitForLines(t, p.output, 4)
	p.closeAndWait(t)

	lines := splitLines(p.output.Bytes())
	if len(lines) != 4 {
		t.Fatalf("frame count = %d, want 4 (no error frame for the response): %s", len(lines), bytes.Join(lines, []byte("\n")))
	}
	if got := responseID(t, lines[3]); got != "3" {
		t.Fatalf("prompt response id = %s", got)
	}
}

func TestTransportDropsUnknownResponseWithoutErrorFrame(t *testing.T) {
	t.Parallel()

	p := startPipeTransport(t, Handler{})
	p.write(t, `{"jsonrpc":"2.0","id":"goclaw-999","result":{}}`+"\n")
	p.write(t, `{"jsonrpc":"2.0","id":7,"result":null}`+"\n")
	p.write(t, `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}`+"\n")
	p.write(t, `{"jsonrpc":"2.0","id":"other","error":{"code":-32601,"message":"Method not found"}}`+"\n")
	p.closeAndWait(t)
	if lines := splitLines(p.output.Bytes()); len(lines) != 1 {
		t.Fatalf("frame count = %d, want only initialize response: %s", len(lines), bytes.Join(lines, []byte("\n")))
	}
}

func TestTransportKeepsMalformedResponseBehaviour(t *testing.T) {
	t.Parallel()

	input := initializeLine(`1`) +
		`{"jsonrpc":"2.0","id":"goclaw-1","result":{},"error":{"code":1,"message":"x"}}` + "\n" +
		`{"jsonrpc":"2.0","id":"goclaw-1"}` + "\n" +
		`{"jsonrpc":"1.0","id":"goclaw-1","result":{}}` + "\n" +
		`{"jsonrpc":"2.0","id":"goclaw-1","method":"","result":{}}` + "\n"
	lines, err := serveTranscript(input, Handler{})
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	for index := 1; index <= 4; index++ {
		assertErrorCode(t, lines, index, CodeInvalidRequest)
	}
}

func TestTransportRequestReturnsClientError(t *testing.T) {
	t.Parallel()

	p := startPipeTransport(t, Handler{})
	outcomes := make(chan requestOutcome, 1)
	go func() {
		result, rpcErr, err := p.transport.Request(context.Background(), "_buzz/wake", nil)
		outcomes <- requestOutcome{result, rpcErr, err}
	}()
	frame := readOutboundFrame(t, p.output, 1)
	if frame.Params != nil {
		t.Fatalf("nil params were serialized: %s", frame.Params)
	}
	p.write(t, `{"jsonrpc":"2.0","id":`+string(frame.ID)+`,"error":{"code":-32601,"message":"Method not found","data":{"x":1}}}`+"\n")
	outcome := waitOutcome(t, outcomes)
	if outcome.err != nil || outcome.result != nil || outcome.rpcErr == nil || outcome.rpcErr.Code != CodeMethodNotFound || outcome.rpcErr.Message != "Method not found" {
		t.Fatalf("Request() = %s, %#v, %v", outcome.result, outcome.rpcErr, outcome.err)
	}
	p.closeAndWait(t)
	if lines := splitLines(p.output.Bytes()); len(lines) != 2 {
		t.Fatalf("frame count = %d, want 2", len(lines))
	}
}

func TestTransportRequestOutsidePrompt(t *testing.T) {
	t.Parallel()

	p := startPipeTransport(t, Handler{})
	outcomes := make(chan requestOutcome, 2)
	for range 2 {
		go func() {
			result, rpcErr, err := p.transport.Request(context.Background(), "_buzz/wake", map[string]int{"n": 1})
			outcomes <- requestOutcome{result, rpcErr, err}
		}()
	}
	first := readOutboundFrame(t, p.output, 1)
	second := readOutboundFrame(t, p.output, 2)
	if bytes.Equal(first.ID, second.ID) {
		t.Fatalf("outbound ids collide: %s", first.ID)
	}
	p.write(t, `{"jsonrpc":"2.0","id":`+string(second.ID)+`,"result":{"outcome":"busy"}}`+"\n")
	p.write(t, `{"jsonrpc":"2.0","id":`+string(first.ID)+`,"result":{"outcome":"duplicate"}}`+"\n")
	results := map[string]bool{}
	for range 2 {
		outcome := waitOutcome(t, outcomes)
		if outcome.err != nil || outcome.rpcErr != nil {
			t.Fatalf("Request() error = %v, %v", outcome.rpcErr, outcome.err)
		}
		results[string(outcome.result)] = true
	}
	if !results[`{"outcome":"busy"}`] || !results[`{"outcome":"duplicate"}`] {
		t.Fatalf("results = %v", results)
	}
	p.closeAndWait(t)
}

func TestTransportFailsPendingRequestsOnClose(t *testing.T) {
	t.Parallel()

	p := startPipeTransport(t, Handler{})
	outcomes := make(chan requestOutcome, 1)
	go func() {
		result, rpcErr, err := p.transport.Request(context.Background(), "_buzz/wake", nil)
		outcomes <- requestOutcome{result, rpcErr, err}
	}()
	readOutboundFrame(t, p.output, 1)
	p.closeAndWait(t)
	if outcome := waitOutcome(t, outcomes); !errors.Is(outcome.err, ErrTransportClosed) {
		t.Fatalf("pending Request() error = %v, want ErrTransportClosed", outcome.err)
	}
	if _, _, err := p.transport.Request(context.Background(), "_buzz/wake", nil); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("Request() after close error = %v, want ErrTransportClosed", err)
	}
}

func TestTransportFailsPendingRequestInsidePromptOnClose(t *testing.T) {
	t.Parallel()

	var transport atomic.Pointer[Transport]
	outcomes := make(chan requestOutcome, 1)
	handler := Handler{
		NewSession: func(context.Context, NewSessionParams) (NewSessionResult, error) {
			return NewSessionResult{SessionID: "session-1"}, nil
		},
		Prompt: func(ctx context.Context, _ PromptParams) (PromptResult, error) {
			result, rpcErr, err := transport.Load().Request(ctx, "_buzz/wake", nil)
			outcomes <- requestOutcome{result, rpcErr, err}
			return PromptResult{StopReason: "end_turn"}, nil
		},
	}
	p := startPipeTransport(t, handler)
	transport.Store(p.transport)
	p.write(t, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}`+"\n")
	waitForLines(t, p.output, 2)
	p.write(t, `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[{"type":"text","text":"hello"}]}}`+"\n")
	readOutboundFrame(t, p.output, 2)
	p.closeAndWait(t)
	if outcome := waitOutcome(t, outcomes); !errors.Is(outcome.err, ErrTransportClosed) {
		t.Fatalf("pending Request() error = %v, want ErrTransportClosed", outcome.err)
	}
}

func TestTransportRequestBeforeServeFails(t *testing.T) {
	t.Parallel()

	transport := NewTransport(strings.NewReader(""), io.Discard, Handler{}, "test-version")
	if _, _, err := transport.Request(context.Background(), "_buzz/wake", nil); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("Request() error = %v, want ErrTransportClosed", err)
	}
	if _, _, err := transport.Request(context.Background(), "", nil); err == nil {
		t.Fatal("Request() accepted an empty method")
	}
}

func TestTransportRequestHonoursContextCancel(t *testing.T) {
	t.Parallel()

	p := startPipeTransport(t, Handler{})
	ctx, cancel := context.WithCancel(context.Background())
	outcomes := make(chan requestOutcome, 1)
	go func() {
		result, rpcErr, err := p.transport.Request(ctx, "_buzz/wake", nil)
		outcomes <- requestOutcome{result, rpcErr, err}
	}()
	frame := readOutboundFrame(t, p.output, 1)
	cancel()
	if outcome := waitOutcome(t, outcomes); !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("Request() error = %v, want context.Canceled", outcome.err)
	}
	p.write(t, `{"jsonrpc":"2.0","id":`+string(frame.ID)+`,"result":{"outcome":"accepted"}}`+"\n")
	p.closeAndWait(t)
	if lines := splitLines(p.output.Bytes()); len(lines) != 2 {
		t.Fatalf("frame count = %d, want 2 (late response dropped)", len(lines))
	}
}

func TestTransportLimitsPendingOutboundRequests(t *testing.T) {
	t.Parallel()

	p := startPipeTransport(t, Handler{})
	ctx, cancel := context.WithCancel(context.Background())
	outcomes := make(chan requestOutcome, MaxPendingOutboundRequests)
	for range MaxPendingOutboundRequests {
		go func() {
			result, rpcErr, err := p.transport.Request(ctx, "_buzz/wake", nil)
			outcomes <- requestOutcome{result, rpcErr, err}
		}()
	}
	waitForLines(t, p.output, MaxPendingOutboundRequests+1)
	if _, _, err := p.transport.Request(context.Background(), "_buzz/wake", nil); !errors.Is(err, ErrOutboundRequestBackpressure) {
		t.Fatalf("Request() over limit error = %v, want ErrOutboundRequestBackpressure", err)
	}
	cancel()
	for range MaxPendingOutboundRequests {
		if outcome := waitOutcome(t, outcomes); !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("Request() error = %v, want context.Canceled", outcome.err)
		}
	}
	freed := make(chan requestOutcome, 1)
	go func() {
		result, rpcErr, err := p.transport.Request(context.Background(), "_buzz/wake", nil)
		freed <- requestOutcome{result, rpcErr, err}
	}()
	frame := readOutboundFrame(t, p.output, MaxPendingOutboundRequests+1)
	p.write(t, `{"jsonrpc":"2.0","id":`+string(frame.ID)+`,"result":{}}`+"\n")
	if outcome := waitOutcome(t, freed); outcome.err != nil {
		t.Fatalf("Request() after release error = %v", outcome.err)
	}
	p.closeAndWait(t)
}

func initializeWithCapabilities(id, capabilities string) string {
	return `{"jsonrpc":"2.0","id":` + id + `,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":` + capabilities + `,"clientInfo":{"name":"test","version":"1"}}}` + "\n"
}

func initializeCapabilities(t *testing.T, capabilities string) (ClientCapabilities, []byte) {
	t.Helper()
	var output bytes.Buffer
	line := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientInfo":{"name":"test","version":"1"}}}` + "\n"
	if capabilities != "" {
		line = initializeWithCapabilities(`1`, capabilities)
	}
	transport := NewTransport(strings.NewReader(line), &output, Handler{}, "test-version")
	if err := transport.Serve(context.Background()); err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	lines := splitLines(output.Bytes())
	if len(lines) != 1 {
		t.Fatalf("response count = %d, want 1", len(lines))
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *RPCError       `json:"error"`
	}
	if err := json.Unmarshal(lines[0], &envelope); err != nil || envelope.Error != nil || envelope.Result == nil {
		t.Fatalf("initialize response = %s", lines[0])
	}
	return transport.ClientCapabilities(), lines[0]
}

func TestInitializeCapturesElicitationFormOnlyWhenExplicit(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		capabilities string
		want         ClientCapabilities
	}{
		"absent":                {"", ClientCapabilities{}},
		"empty":                 {`{}`, ClientCapabilities{}},
		"empty elicitation":     {`{"elicitation":{}}`, ClientCapabilities{}},
		"null form":             {`{"elicitation":{"form":null}}`, ClientCapabilities{}},
		"empty form":            {`{"elicitation":{"form":{}}}`, ClientCapabilities{ElicitationForm: true}},
		"form and url":          {`{"elicitation":{"form":{},"url":{}}}`, ClientCapabilities{ElicitationForm: true, ElicitationURL: true}},
		"null url":              {`{"elicitation":{"url":null}}`, ClientCapabilities{}},
		"non-object":            {`"text"`, ClientCapabilities{}},
		"bad elicitation shape": {`{"elicitation":true,"_meta":{"buzz.wake":{"version":1}}}`, ClientCapabilities{Wake: true}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, _ := initializeCapabilities(t, test.capabilities)
			if got != test.want {
				t.Fatalf("ClientCapabilities() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestInitializeCapturesWakeCapabilityAndAdvertisesOwn(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		capabilities string
		want         bool
	}{
		"version one":         {`{"_meta":{"buzz.wake":{"version":1}}}`, true},
		"version one, extra":  {`{"_meta":{"buzz.wake":{"version":1,"note":"x"}},"elicitation":{}}`, true},
		"version two":         {`{"_meta":{"buzz.wake":{"version":2}}}`, false},
		"string version":      {`{"_meta":{"buzz.wake":{"version":"1"}}}`, false},
		"missing version":     {`{"_meta":{"buzz.wake":{}}}`, false},
		"boolean":             {`{"_meta":{"buzz.wake":true}}`, false},
		"null":                {`{"_meta":{"buzz.wake":null}}`, false},
		"array":               {`{"_meta":{"buzz.wake":[{"version":1}]}}`, false},
		"other key":           {`{"_meta":{"buzz.other":{"version":1}}}`, false},
		"top level, not meta": {`{"buzz.wake":{"version":1}}`, false},
		"meta not an object":  {`{"_meta":"buzz.wake"}`, false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, line := initializeCapabilities(t, test.capabilities)
			if got.Wake != test.want {
				t.Fatalf("Wake = %v, want %v", got.Wake, test.want)
			}
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
			if string(response.Result.AgentCapabilities.Meta["buzz.wake"]) != `{"version":1}` {
				t.Fatalf("advertised agent capabilities = %s", line)
			}
		})
	}
}

func TestTransportPassesThreadContextToHandlers(t *testing.T) {
	t.Parallel()

	var sessionThread, promptThread atomic.Value
	handler := Handler{
		NewSession: func(_ context.Context, params NewSessionParams) (NewSessionResult, error) {
			sessionThread.Store(string(params.Thread))
			return NewSessionResult{SessionID: "session-1"}, nil
		},
		Prompt: func(_ context.Context, params PromptParams) (PromptResult, error) {
			promptThread.Store(string(params.Thread))
			return PromptResult{StopReason: "end_turn"}, nil
		},
	}
	p := startPipeTransport(t, handler)
	p.write(t, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[],"_meta":{"buzz.thread":{"channelId":"c1"}}}}`+"\n")
	waitForLines(t, p.output, 2)
	p.write(t, `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[{"type":"text","text":"hi"}],"_meta":{"buzz.thread":{"channelId":"c2"}}}}`+"\n")
	waitForLines(t, p.output, 3)
	p.write(t, `{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{"sessionId":"session-1","prompt":[{"type":"text","text":"hi"}],"_meta":{"buzz.thread":"bad"}}}`+"\n")
	waitForLines(t, p.output, 4)
	p.closeAndWait(t)
	if sessionThread.Load() != `{"channelId":"c1"}` || promptThread.Load() != `{"channelId":"c2"}` {
		t.Fatalf("handler threads = %v, %v", sessionThread.Load(), promptThread.Load())
	}
	assertErrorCode(t, splitLines(p.output.Bytes()), 3, CodeInvalidParams)
}
