package acpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"

	mcptransport "github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
)

type boundedStdioTransport struct {
	command            string
	args               []string
	environment        []string
	cwd                string
	executableIdentity os.FileInfo

	mu             sync.Mutex
	writeMu        sync.Mutex
	cmd            *exec.Cmd
	stdin          io.WriteCloser
	responses      map[string]chan *mcptransport.JSONRPCResponse
	onNotification func(mcp.JSONRPCNotification)
	done           chan struct{}
	closeOnce      sync.Once
	closeErr       error
}

func newBoundedStdioTransport(command string, args, environment []string, cwd string, identity os.FileInfo) mcptransport.Interface {
	return &boundedStdioTransport{
		command: command, args: append([]string(nil), args...),
		environment: append([]string(nil), environment...), cwd: cwd,
		executableIdentity: identity,
		responses:          make(map[string]chan *mcptransport.JSONRPCResponse), done: make(chan struct{}),
	}
}

func (t *boundedStdioTransport) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cmd != nil {
		return nil
	}
	if t.executableIdentity != nil {
		current, err := os.Stat(t.command)
		if err != nil || !os.SameFile(t.executableIdentity, current) || !current.Mode().IsRegular() || current.Mode().Perm()&0o022 != 0 {
			return errors.New("MCP executable changed before start")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cmd := exec.Command(t.command, t.args...)
	cmd.Env = append([]string(nil), t.environment...)
	cmd.Dir = t.cwd
	prepareMCPCommand(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		return err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return err
	}
	if err := attachMCPProcess(cmd); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = stdin.Close()
		return err
	}
	t.cmd, t.stdin = cmd, stdin
	go func() { _, _ = io.Copy(io.Discard, stderr) }()
	go t.readLoop(stdout)
	return nil
}

func (t *boundedStdioTransport) SendRequest(ctx context.Context, request mcptransport.JSONRPCRequest) (*mcptransport.JSONRPCResponse, error) {
	response := make(chan *mcptransport.JSONRPCResponse, 1)
	key := request.ID.String()
	t.mu.Lock()
	if t.stdin == nil {
		t.mu.Unlock()
		return nil, errors.New("MCP transport is not started")
	}
	t.responses[key] = response
	t.mu.Unlock()
	if err := t.writeJSON(request); err != nil {
		t.deleteResponse(key)
		return nil, err
	}
	select {
	case result := <-response:
		return result, nil
	case <-ctx.Done():
		t.deleteResponse(key)
		return nil, ctx.Err()
	case <-t.done:
		t.deleteResponse(key)
		return nil, errors.New("MCP transport closed")
	}
}

func (t *boundedStdioTransport) SendNotification(ctx context.Context, notification mcp.JSONRPCNotification) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.done:
		return errors.New("MCP transport closed")
	default:
		return t.writeJSON(notification)
	}
}

func (t *boundedStdioTransport) SetNotificationHandler(handler func(mcp.JSONRPCNotification)) {
	t.mu.Lock()
	t.onNotification = handler
	t.mu.Unlock()
}

func (t *boundedStdioTransport) GetSessionId() string { return "" }

func (t *boundedStdioTransport) Close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		stdin, cmd := t.stdin, t.cmd
		t.stdin = nil
		t.mu.Unlock()
		if stdin != nil {
			_ = stdin.Close()
		}
		if cmd != nil {
			processErr := errors.Join(killMCPProcessTree(cmd), cmd.Wait())
			if t.closeErr == nil {
				t.closeErr = processErr
			} else if processErr != nil {
				t.closeErr = errors.Join(t.closeErr, processErr)
			}
		}
		close(t.done)
	})
	return t.closeErr
}

func (t *boundedStdioTransport) writeJSON(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) >= maxMCPFrameBytes {
		return ErrMCPFrameTooLarge
	}
	payload = append(payload, '\n')
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	t.mu.Lock()
	stdin := t.stdin
	t.mu.Unlock()
	if stdin == nil {
		return errors.New("MCP transport closed")
	}
	_, err = stdin.Write(payload)
	return err
}

func (t *boundedStdioTransport) readLoop(stdout io.Reader) {
	reader := bufio.NewReaderSize(stdout, maxMCPFrameBytes+1)
	for {
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) || len(line) > maxMCPFrameBytes {
			t.closeErr = ErrMCPFrameTooLarge
			_ = t.Close()
			return
		}
		if len(line) > 0 {
			t.routeLine(strings.TrimSpace(string(line)))
		}
		if err != nil {
			_ = t.Close()
			return
		}
	}
}

func (t *boundedStdioTransport) routeLine(line string) {
	var header struct {
		ID     *mcp.RequestId `json:"id,omitempty"`
		Method string         `json:"method,omitempty"`
	}
	if line == "" || json.Unmarshal([]byte(line), &header) != nil {
		return
	}
	if header.Method != "" && header.ID == nil {
		var notification mcp.JSONRPCNotification
		if json.Unmarshal([]byte(line), &notification) != nil {
			return
		}
		t.mu.Lock()
		handler := t.onNotification
		t.mu.Unlock()
		if handler != nil {
			handler(notification)
		}
		return
	}
	var response mcptransport.JSONRPCResponse
	if json.Unmarshal([]byte(line), &response) != nil {
		return
	}
	key := response.ID.String()
	t.mu.Lock()
	channel := t.responses[key]
	delete(t.responses, key)
	t.mu.Unlock()
	if channel != nil {
		channel <- &response
	}
}

func (t *boundedStdioTransport) deleteResponse(key string) {
	t.mu.Lock()
	delete(t.responses, key)
	t.mu.Unlock()
}
