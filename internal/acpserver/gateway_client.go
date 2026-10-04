package acpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

var (
	ErrGatewayClosed          = errors.New("gateway connection closed")
	ErrGatewayPendingLimit    = errors.New("gateway pending request limit reached")
	ErrGatewayFrameTooLarge   = errors.New("gateway frame exceeds size limit")
	ErrGatewayIndeterminate   = errors.New("gateway request outcome is indeterminate")
	ErrGatewaySubscription    = errors.New("gateway subscription closed")
	ErrGatewayEventOverflow   = errors.New("gateway event queue overflow")
	ErrUnboundGatewayIdentity = errors.New("gateway returned an unbound identity")
)

// GatewayClientConfig contains the credentials and endpoint for one persistent
// authenticated Gateway connection.
type GatewayClientConfig struct {
	URL    string
	Token  string
	UserID string
}

// GatewayIdentity is the authenticated identity returned by connect.
type GatewayIdentity struct {
	UserID        string
	TenantID      string
	Role          string
	IsOwner       bool
	IsMasterScope bool
	ServerName    string
	ServerVersion string
}

type ChatSendRequest struct {
	AgentID      string `json:"agentId"`
	SessionKey   string `json:"sessionKey"`
	Message      string `json:"message"`
	Stream       bool   `json:"stream"`
	Generation   uint64 `json:"acpGeneration"`
	TerminalMode string `json:"terminalMode"`
}

type ChatSendResponse struct {
	Content   string `json:"content"`
	Thinking  string `json:"thinking,omitempty"`
	RunID     string `json:"runId,omitempty"`
	Cancelled bool   `json:"cancelled,omitempty"`
}

type ChatAbortRequest struct {
	SessionKey string `json:"sessionKey"`
	RunID      string `json:"runId,omitempty"`
}

func (c *GatewayClient) ChatSend(ctx context.Context, request ChatSendRequest) (ChatSendResponse, error) {
	var response ChatSendResponse
	err := c.Call(ctx, protocol.MethodChatSend, request, &response)
	return response, err
}

func (c *GatewayClient) ChatAbort(ctx context.Context, request ChatAbortRequest) error {
	return c.Call(ctx, protocol.MethodChatAbort, request, nil)
}

// GatewayEvent retains the Gateway event payload without converting JSON
// numbers or nested values through interface{}.
type GatewayEvent struct {
	Event      string
	Type       string
	SessionKey string
	RunID      string
	Generation uint64
	Payload    json.RawMessage
}

type gatewayCallResult struct {
	payload json.RawMessage
	err     error
}

type gatewayPendingCall struct {
	method string
	result chan gatewayCallResult
}

type gatewayRoute struct {
	sessionKey string
	generation uint64
}

// GatewayClient multiplexes requests and session event subscriptions over one
// WebSocket. It never replays requests.
type GatewayClient struct {
	config GatewayClientConfig

	connMu sync.RWMutex
	conn   *websocket.Conn

	writeMu sync.Mutex
	nextID  atomic.Uint64

	pendingMu sync.Mutex
	pending   map[string]*gatewayPendingCall

	subMu sync.RWMutex
	subs  map[gatewayRoute]*GatewaySubscription

	identityMu sync.RWMutex
	identity   GatewayIdentity

	closed      chan struct{}
	readDone    chan struct{}
	closeOnce   sync.Once
	readStarted atomic.Bool
}

// NewGatewayClient creates a disconnected client.
func NewGatewayClient(config GatewayClientConfig) *GatewayClient {
	return &GatewayClient{
		config:   config,
		pending:  make(map[string]*gatewayPendingCall),
		subs:     make(map[gatewayRoute]*GatewaySubscription),
		closed:   make(chan struct{}),
		readDone: make(chan struct{}),
	}
}

// Connect opens the socket and completes Gateway authentication before any
// application request can be sent.
func (c *GatewayClient) Connect(ctx context.Context) error {
	wsURL, err := normalizeAndValidateGatewayURL(c.config.URL)
	if err != nil {
		return err
	}
	dialer := *websocket.DefaultDialer
	if strings.HasPrefix(wsURL, "ws://") {
		// Plaintext is loopback-only and must not expose credentials to a proxy.
		dialer.Proxy = nil
	}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("connect gateway: %w", err)
	}
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	params, err := json.Marshal(map[string]string{"token": c.config.Token, "user_id": c.config.UserID})
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("marshal gateway connect: %w", err)
	}
	frame := protocol.RequestFrame{Type: protocol.FrameTypeRequest, ID: c.requestID(), Method: protocol.MethodConnect, Params: params}
	if err := c.writeFrame(frame); err != nil {
		_ = conn.Close()
		return fmt.Errorf("send gateway connect: %w", err)
	}
	conn.SetReadLimit(MaxGatewayFrameBytes)
	if err := conn.SetReadDeadline(time.Now().Add(GatewayReadTimeout)); err != nil {
		_ = conn.Close()
		return fmt.Errorf("set gateway read deadline: %w", err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("read gateway connect: %w", err)
	}
	if len(data) > MaxGatewayFrameBytes {
		_ = conn.Close()
		return ErrGatewayFrameTooLarge
	}
	var response gatewayWireResponse
	if err := json.Unmarshal(data, &response); err != nil {
		_ = conn.Close()
		return fmt.Errorf("decode gateway connect: %w", err)
	}
	if response.Type != protocol.FrameTypeResponse || response.ID != frame.ID {
		_ = conn.Close()
		return errors.New("unexpected gateway connect response")
	}
	if !response.OK {
		_ = conn.Close()
		return gatewayResponseError(response.Error)
	}
	var payload struct {
		UserID        string `json:"user_id"`
		TenantID      string `json:"tenant_id"`
		Role          string `json:"role"`
		IsOwner       bool   `json:"is_owner"`
		IsMasterScope bool   `json:"is_master_scope"`
		Server        struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"server"`
	}
	if err := json.Unmarshal(response.Payload, &payload); err != nil {
		_ = conn.Close()
		return fmt.Errorf("decode gateway identity: %w", err)
	}
	if strings.TrimSpace(payload.UserID) == "" || strings.TrimSpace(payload.TenantID) == "" {
		_ = conn.Close()
		return ErrUnboundGatewayIdentity
	}
	c.identityMu.Lock()
	c.identity = GatewayIdentity{
		UserID: payload.UserID, TenantID: payload.TenantID, Role: payload.Role,
		IsOwner: payload.IsOwner, IsMasterScope: payload.IsMasterScope,
		ServerName: payload.Server.Name, ServerVersion: payload.Server.Version,
	}
	c.identityMu.Unlock()
	_ = conn.SetReadDeadline(time.Now().Add(GatewayReadTimeout))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(GatewayReadTimeout)) })
	c.readStarted.Store(true)
	go c.readLoop(conn)
	go c.pingLoop(conn)
	return nil
}

// Identity returns the identity established by Connect.
func (c *GatewayClient) Identity() GatewayIdentity {
	c.identityMu.RLock()
	defer c.identityMu.RUnlock()
	return c.identity
}

// Call sends one request and waits for its correlated response.
func (c *GatewayClient) Call(ctx context.Context, method string, params, result any) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		timeout := GatewayControlTimeout
		if method == protocol.MethodChatSend {
			timeout = GatewayChatTimeout
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	paramsData, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal gateway params: %w", err)
	}
	id := c.requestID()
	frame := protocol.RequestFrame{Type: protocol.FrameTypeRequest, ID: id, Method: method, Params: paramsData}
	data, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal gateway request: %w", err)
	}
	if len(data) > MaxGatewayFrameBytes {
		return ErrGatewayFrameTooLarge
	}
	pending := &gatewayPendingCall{method: method, result: make(chan gatewayCallResult, 1)}
	if err := c.reservePending(id, pending); err != nil {
		return err
	}
	if err := c.writeData(data); err != nil {
		c.removePending(id, pending)
		c.shutdown(err)
		if method == protocol.MethodChatSend {
			return errors.Join(ErrGatewayIndeterminate, err)
		}
		return err
	}

	select {
	case response := <-pending.result:
		if response.err != nil {
			if method == protocol.MethodChatSend {
				return errors.Join(ErrGatewayIndeterminate, response.err)
			}
			return response.err
		}
		if result != nil && len(response.payload) > 0 {
			if err := json.Unmarshal(response.payload, result); err != nil {
				return fmt.Errorf("decode gateway response: %w", err)
			}
		}
		return nil
	case <-ctx.Done():
		c.removePending(id, pending)
		return ctx.Err()
	}
}

func (c *GatewayClient) reservePending(id string, pending *gatewayPendingCall) error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	select {
	case <-c.closed:
		return ErrGatewayClosed
	default:
	}
	if len(c.pending) >= MaxPendingGatewayRequests {
		return ErrGatewayPendingLimit
	}
	c.pending[id] = pending
	return nil
}

func (c *GatewayClient) removePending(id string, pending *gatewayPendingCall) {
	c.pendingMu.Lock()
	if c.pending[id] == pending {
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
}

func (c *GatewayClient) pendingCount() int {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	return len(c.pending)
}

// Subscribe registers one bounded event route before chat.send is issued.
func (c *GatewayClient) Subscribe(sessionID, sessionKey string, generation uint64) (*GatewaySubscription, error) {
	if sessionID == "" || sessionKey == "" || generation == 0 {
		return nil, errors.New("gateway subscription requires session, key, and generation")
	}
	route := gatewayRoute{sessionKey: sessionKey, generation: generation}
	sub := &GatewaySubscription{client: c, route: route, sessionID: sessionID, events: make(chan GatewayEvent, MaxGatewayEventQueue), done: make(chan struct{})}
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if len(c.subs) >= MaxActiveSessions {
		return nil, errors.New("gateway subscription limit reached")
	}
	if _, exists := c.subs[route]; exists {
		return nil, errors.New("gateway subscription already exists")
	}
	c.subs[route] = sub
	return sub, nil
}

func (c *GatewayClient) readLoop(conn *websocket.Conn) {
	defer close(c.readDone)
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			c.shutdown(err)
			return
		}
		if len(data) > MaxGatewayFrameBytes {
			c.shutdown(ErrGatewayFrameTooLarge)
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(GatewayReadTimeout))
		frameType, err := protocol.ParseFrameType(data)
		if err != nil {
			c.shutdown(errors.New("invalid gateway frame"))
			return
		}
		switch frameType {
		case protocol.FrameTypeResponse:
			c.dispatchResponse(data)
		case protocol.FrameTypeEvent:
			c.dispatchEvent(data)
		default:
			c.shutdown(errors.New("unexpected gateway frame type"))
			return
		}
	}
}

func (c *GatewayClient) pingLoop(conn *websocket.Conn) {
	ticker := time.NewTicker(GatewayReadTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			c.writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(GatewayWriteTimeout))
			c.writeMu.Unlock()
			if err != nil {
				c.shutdown(err)
				return
			}
		}
	}
}

type gatewayWireResponse struct {
	Type    string               `json:"type"`
	ID      string               `json:"id"`
	OK      bool                 `json:"ok"`
	Payload json.RawMessage      `json:"payload"`
	Error   *protocol.ErrorShape `json:"error"`
}

func (c *GatewayClient) dispatchResponse(data []byte) {
	var response gatewayWireResponse
	if json.Unmarshal(data, &response) != nil {
		c.shutdown(errors.New("invalid gateway response"))
		return
	}
	c.pendingMu.Lock()
	pending := c.pending[response.ID]
	delete(c.pending, response.ID)
	c.pendingMu.Unlock()
	if pending == nil {
		return
	}
	if !response.OK {
		pending.result <- gatewayCallResult{err: gatewayResponseError(response.Error)}
		return
	}
	pending.result <- gatewayCallResult{payload: response.Payload}
}

func gatewayResponseError(shape *protocol.ErrorShape) error {
	if shape == nil {
		return errors.New("gateway request failed")
	}
	return fmt.Errorf("gateway request failed: %s", shape.Code)
}

func (c *GatewayClient) dispatchEvent(data []byte) {
	var frame struct {
		Event   string          `json:"event"`
		Payload json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(data, &frame) != nil {
		return
	}
	var route struct {
		Type              string `json:"type"`
		SessionKey        string `json:"sessionKey"`
		RunID             string `json:"runId"`
		Generation        uint64 `json:"acpGeneration"`
		CapabilityVersion uint64 `json:"generation"`
	}
	if json.Unmarshal(frame.Payload, &route) != nil || route.SessionKey == "" {
		return
	}
	if route.Generation == 0 {
		route.Generation = route.CapabilityVersion
	}
	if route.Generation == 0 {
		return
	}
	key := gatewayRoute{sessionKey: route.SessionKey, generation: route.Generation}
	c.subMu.RLock()
	sub := c.subs[key]
	c.subMu.RUnlock()
	if sub == nil {
		return
	}
	event := GatewayEvent{Event: frame.Event, Type: route.Type, SessionKey: route.SessionKey, RunID: route.RunID, Generation: route.Generation, Payload: frame.Payload}
	if !sub.accept(event) {
		return
	}
	select {
	case sub.events <- event:
	default:
		c.removeSubscription(sub)
		sub.fail(ErrGatewayEventOverflow)
	}
}

func (c *GatewayClient) requestID() string {
	return fmt.Sprintf("acp-%d", c.nextID.Add(1))
}

func (c *GatewayClient) writeFrame(frame protocol.RequestFrame) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if len(data) > MaxGatewayFrameBytes {
		return ErrGatewayFrameTooLarge
	}
	return c.writeData(data)
}

func (c *GatewayClient) writeData(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return ErrGatewayClosed
	default:
	}
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()
	if conn == nil {
		return ErrGatewayClosed
	}
	if err := conn.SetWriteDeadline(time.Now().Add(GatewayWriteTimeout)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

func (c *GatewayClient) shutdown(cause error) {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.connMu.RLock()
		conn := c.conn
		c.connMu.RUnlock()
		if conn != nil {
			_ = conn.Close()
		}
		if cause == nil {
			cause = ErrGatewayClosed
		}
		c.pendingMu.Lock()
		pending := c.pending
		c.pending = make(map[string]*gatewayPendingCall)
		c.pendingMu.Unlock()
		for _, call := range pending {
			call.result <- gatewayCallResult{err: cause}
		}
		c.subMu.Lock()
		subs := c.subs
		c.subs = make(map[gatewayRoute]*GatewaySubscription)
		c.subMu.Unlock()
		for _, sub := range subs {
			sub.fail(cause)
		}
	})
}

// Close stops the connection and waits a bounded time for the read loop.
func (c *GatewayClient) Close() error {
	c.shutdown(ErrGatewayClosed)
	if !c.readStarted.Load() {
		return nil
	}
	select {
	case <-c.readDone:
		return nil
	case <-time.After(GatewayCloseDrainTimeout):
		return errors.New("gateway close timed out")
	}
}

func (c *GatewayClient) removeSubscription(sub *GatewaySubscription) {
	c.subMu.Lock()
	if c.subs[sub.route] == sub {
		delete(c.subs, sub.route)
	}
	c.subMu.Unlock()
}

// GatewaySubscription owns one session and generation event route.
type GatewaySubscription struct {
	client    *GatewayClient
	route     gatewayRoute
	sessionID string
	events    chan GatewayEvent
	done      chan struct{}
	failOnce  sync.Once
	errMu     sync.RWMutex
	err       error
	runMu     sync.RWMutex
	runID     string
}

func (s *GatewaySubscription) accept(event GatewayEvent) bool {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if event.Type == protocol.AgentEventRunStarted {
		if event.RunID == "" || (s.runID != "" && s.runID != event.RunID) {
			return false
		}
		s.runID = event.RunID
		return true
	}
	// MCP calls are routed by session and generation, then checked against the capability.
	if event.Event == protocol.EventACPToolCall {
		return true
	}
	return s.runID == "" || (event.RunID != "" && event.RunID == s.runID)
}

// Next waits for the next matching event or the subscription failure.
func (s *GatewaySubscription) Next(ctx context.Context) (GatewayEvent, error) {
	select {
	case <-s.done:
		return GatewayEvent{}, s.terminalError()
	default:
	}
	select {
	case <-s.done:
		return GatewayEvent{}, s.terminalError()
	case event := <-s.events:
		return event, nil
	case <-ctx.Done():
		return GatewayEvent{}, ctx.Err()
	}
}

// Close removes the subscription without closing the shared connection.
func (s *GatewaySubscription) Close() {
	s.client.removeSubscription(s)
	s.fail(ErrGatewaySubscription)
}

func (s *GatewaySubscription) fail(err error) {
	s.failOnce.Do(func() {
		s.errMu.Lock()
		s.err = err
		s.errMu.Unlock()
		close(s.done)
	})
}

func (s *GatewaySubscription) terminalError() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	if s.err == nil {
		return ErrGatewaySubscription
	}
	return s.err
}

func normalizeAndValidateGatewayURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("parse gateway URL: %w", err)
	}
	baseURL := false
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
		baseURL = true
	case "https":
		parsed.Scheme = "wss"
		baseURL = true
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported gateway URL scheme %q", parsed.Scheme)
	}
	if parsed.Hostname() == "" {
		return "", errors.New("gateway URL requires a host")
	}
	if parsed.Scheme == "ws" {
		ip := net.ParseIP(parsed.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return "", errors.New("plaintext Gateway connections require an exact loopback IP address")
		}
	}
	if baseURL || parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = strings.TrimRight(parsed.Path, "/") + "/ws"
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
