package acpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nextlevelbuilder/goclaw/internal/sessions"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

type ServerConfig struct {
	Gateway       *GatewayClient
	AgentID       string
	WorkspaceRoot string
	MCPExecRoots  []string
	Version       string
}

type Server struct {
	gateway       *GatewayClient
	agentID       string
	workspaceRoot string
	version       string
	mcpExecRoots  []string
	sessions      *SessionRegistry
	mu            sync.Mutex
	transport     *Transport
	closed        bool
}

func NewServer(config ServerConfig) (*Server, error) {
	if config.Gateway == nil || config.AgentID == "" || config.WorkspaceRoot == "" {
		return nil, errors.New("ACP server requires gateway, agent, and workspace root")
	}
	root, err := canonicalDirectory(config.WorkspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve ACP workspace root: %w", err)
	}
	return &Server{gateway: config.Gateway, agentID: config.AgentID, workspaceRoot: root, version: config.Version, mcpExecRoots: append([]string(nil), config.MCPExecRoots...), sessions: NewSessionRegistry()}, nil
}

func (s *Server) Run(ctx context.Context, input io.Reader, output io.Writer) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("ACP server is closed")
	}
	if s.transport != nil {
		s.mu.Unlock()
		return errors.New("ACP server is already running")
	}
	transport := NewTransport(input, output, Handler{NewSession: s.handleNewSession, Prompt: s.handlePrompt, Cancel: s.handleCancel}, s.version)
	s.transport = transport
	s.mu.Unlock()
	reapCtx, stopReaper := context.WithCancel(ctx)
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		ticker := time.NewTicker(SessionReapInterval)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				s.sessions.ReapIdle(now, SessionIdleTTL)
			case <-reapCtx.Done():
				return
			}
		}
	}()
	err := transport.Serve(ctx)
	stopReaper()
	<-reaperDone
	s.sessions.Close()
	s.mu.Lock()
	s.transport = nil
	s.mu.Unlock()
	return err
}

func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.sessions.Close()
	return s.gateway.Close()
}

func (s *Server) handleNewSession(ctx context.Context, params NewSessionParams) (NewSessionResult, error) {
	cwd, err := s.validateWorkspace(params.CWD)
	if err != nil {
		return NewSessionResult{}, &RPCError{Code: CodeInvalidParams, Message: "Invalid workspace"}
	}
	id := uuid.NewString()
	record := &sessionRecord{ID: id, SessionKey: sessions.BuildWSSessionKey(s.agentID, id), CWD: cwd}
	if len(params.MCPServers) > 0 {
		manager, managerErr := NewSessionMCPManager(s.workspaceRoot, s.mcpExecRoots)
		if managerErr != nil {
			return NewSessionResult{}, managerErr
		}
		startCtx, cancelStart := context.WithTimeout(ctx, 30*time.Second)
		schemas, startErr := manager.Start(startCtx, cwd, params.MCPServers)
		cancelStart()
		if startErr != nil {
			if closeErr := manager.Close(); closeErr != nil {
				slog.Warn("close failed ACP MCP startup", "error", closeErr)
			}
			return NewSessionResult{}, startErr
		}
		record.mcp, record.tools = manager, schemas
	}
	if err := s.sessions.Add(record); err != nil {
		return NewSessionResult{}, err
	}
	return NewSessionResult{SessionID: id}, nil
}

func (s *Server) handlePrompt(ctx context.Context, params PromptParams) (PromptResult, error) {
	record, generation, promptCtx, err := s.sessions.BeginPrompt(params.SessionID, ctx)
	if err != nil {
		return PromptResult{}, err
	}
	var capabilityID string
	defer func() {
		s.sessions.FinishPrompt(params.SessionID, generation)
		if capabilityID == "" {
			return
		}
		releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelRelease()
		if releaseErr := s.gateway.Call(releaseCtx, protocol.MethodACPToolsRelease, map[string]string{"capabilityId": capabilityID}, nil); releaseErr != nil {
			slog.Warn("release ACP capability", "error", releaseErr)
		}
	}()
	subscription, err := s.gateway.Subscribe(params.SessionID, record.SessionKey, generation)
	if err != nil {
		return PromptResult{}, err
	}
	defer subscription.Close()
	if len(record.tools) > 0 {
		var registered struct {
			CapabilityID string `json:"capabilityId"`
		}
		registerCtx, cancelRegister := context.WithTimeout(promptCtx, 10*time.Second)
		err = s.gateway.Call(registerCtx, protocol.MethodACPToolsRegister, map[string]any{"agentId": s.agentID, "sessionKey": record.SessionKey, "generation": generation, "tools": record.tools}, &registered)
		cancelRegister()
		if err != nil {
			return PromptResult{}, err
		}
		capabilityID = registered.CapabilityID
	}
	events := make(chan GatewayEvent, 1)
	eventErrors := make(chan error, 1)
	go func() {
		for {
			event, nextErr := subscription.Next(promptCtx)
			if nextErr != nil {
				eventErrors <- nextErr
				return
			}
			select {
			case events <- event:
			case <-promptCtx.Done():
				return
			}
		}
	}()
	type outcome struct {
		response ChatSendResponse
		err      error
	}
	responses := make(chan outcome, 1)
	go func() {
		response, sendErr := s.gateway.ChatSend(promptCtx, ChatSendRequest{AgentID: s.agentID, SessionKey: record.SessionKey, Message: joinPrompt(params.Prompt), Stream: true, Generation: generation, TerminalMode: "metadata"})
		responses <- outcome{response, sendErr}
	}()
	used := 0
	for {
		select {
		case event := <-events:
			if event.Generation != generation {
				continue
			}
			if event.Event == protocol.EventACPToolCall && record.mcp != nil {
				go s.handleMCPToolCall(promptCtx, record, event, capabilityID, generation)
				continue
			}
			if event.Type == protocol.AgentEventRunStarted && event.RunID != "" {
				s.sessions.SetRunID(params.SessionID, generation, event.RunID)
			}
			s.mu.Lock()
			transport := s.transport
			s.mu.Unlock()
			if transport != nil {
				if err := emitGatewayTool(promptCtx, transport, params.SessionID, event); err != nil {
					return PromptResult{}, err
				}
				if err := emitGatewayText(promptCtx, transport, params.SessionID, event, &used); err != nil {
					return PromptResult{}, err
				}
			}
		case result := <-responses:
			if result.err != nil {
				if errors.Is(result.err, context.Canceled) {
					return PromptResult{StopReason: "cancelled"}, nil
				}
				return PromptResult{}, &RPCError{Code: CodeGatewayError, Message: "Gateway request failed"}
			}
			if result.response.Cancelled {
				return PromptResult{StopReason: "cancelled"}, nil
			}
			return PromptResult{StopReason: "end_turn"}, nil
		case <-eventErrors:
			return PromptResult{}, &RPCError{Code: CodeGatewayError, Message: "Gateway connection outcome is indeterminate"}
		case <-promptCtx.Done():
			return PromptResult{StopReason: "cancelled"}, nil
		}
	}
}

func (s *Server) handleMCPToolCall(ctx context.Context, record *sessionRecord, event GatewayEvent, expectedCapabilityID string, expectedGeneration uint64) {
	var payload struct {
		CapabilityID string         `json:"capabilityId"`
		CallID       string         `json:"callId"`
		SessionKey   string         `json:"sessionKey"`
		Generation   uint64         `json:"generation"`
		ToolName     string         `json:"toolName"`
		Arguments    map[string]any `json:"arguments"`
	}
	if json.Unmarshal(event.Payload, &payload) != nil || payload.CapabilityID != expectedCapabilityID || payload.SessionKey != record.SessionKey || payload.Generation != expectedGeneration || payload.CallID == "" || payload.ToolName == "" {
		return
	}
	callCtx, cancelCall := context.WithTimeout(ctx, 20*time.Minute)
	defer cancelCall()
	content, isError, err := record.mcp.Call(callCtx, payload.ToolName, payload.Arguments)
	if err != nil {
		content, isError = "MCP tool call failed", true
	}
	resultCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	callErr := s.gateway.Call(resultCtx, protocol.MethodACPToolsResult, map[string]any{"capabilityId": payload.CapabilityID, "callId": payload.CallID, "generation": payload.Generation, "result": map[string]any{"content": content, "isError": isError}}, nil)
	if callErr != nil {
		slog.Warn("return ACP MCP tool result", "error", callErr)
	}
}

func (s *Server) handleCancel(ctx context.Context, params CancelParams) {
	sessionKey, runID, ok := s.sessions.Cancel(params.SessionID)
	if !ok {
		return
	}
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_ = s.gateway.ChatAbort(abortCtx, ChatAbortRequest{SessionKey: sessionKey, RunID: runID})
}

func (s *Server) validateWorkspace(requested string) (string, error) {
	if !filepath.IsAbs(requested) {
		return "", errors.New("workspace must be absolute")
	}
	cwd, err := canonicalDirectory(requested)
	if err != nil {
		return "", err
	}
	if cwd == s.workspaceRoot {
		return "", errors.New("workspace root cannot be a session workspace")
	}
	rel, err := filepath.Rel(s.workspaceRoot, cwd)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", errors.New("workspace escapes configured root")
	}
	return cwd, nil
}

func canonicalDirectory(path string) (string, error) {
	clean, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	info, err := os.Stat(clean)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("path is not a directory")
	}
	return clean, nil
}
