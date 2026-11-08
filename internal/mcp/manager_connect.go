package mcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	mcpclient "github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// connectAndDiscover creates a client, initializes the MCP handshake, and
// discovers tools. Returns a connected serverState with discovered tool
// definitions. The caller is responsible for registering tools and starting
// the health loop. This function is shared by both Manager and Pool.
// protocol is the server setting "protocol"; stdio is always legacy-only.
func connectAndDiscover(ctx context.Context, name, transportType, command string, args []string, env map[string]string, url string, headers map[string]string, timeoutSec int, protocol ProtocolMode) (*serverState, []mcpgo.Tool, error) {
	client, err := createClient(transportType, command, args, env, url, headers)
	if err != nil {
		return nil, nil, fmt.Errorf("create client: %w", err)
	}
	// Attach before Start so that no stream frame can arrive unrouted.
	router := attachNotificationRouter(client)

	if transportType != "stdio" {
		if err := client.Start(ctx); err != nil {
			_ = client.Close()
			return nil, nil, fmt.Errorf("start transport: %w", err)
		}
	}

	// Retry initialization with exponential backoff for slow-starting stdio servers.
	// Heavy MCP servers (FastMCP with 80+ tools, OAuth servers) can take 3-5s to start
	// their stdin read loop. Without retries, Initialize sends JSON-RPC before the
	// server is ready, gets EOF, and permanently fails. SSE/HTTP transports don't need
	// this because the HTTP server rejects connections until ready (connection refused).
	const maxInitAttempts = 4 // backoff: 2s + 4s + 8s = ~14s total before giving up
	var initErr error
	var initResult *mcpgo.InitializeResult
	for attempt := range maxInitAttempts {
		if attempt > 0 {
			backoff := time.Duration(1<<attempt) * time.Second // 2s, 4s, 8s
			slog.Debug("mcp.init.retry", "server", name, "attempt", attempt+1, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				_ = client.Close()
				return nil, nil, fmt.Errorf("initialize: context cancelled during retry: %w", ctx.Err())
			}
		}
		res, err := client.Initialize(ctx, newInitializeRequest(transportType, protocol, clientInfoName))
		if err == nil {
			initErr = nil
			initResult = res
			break
		}
		initErr = err
		// Non-stdio transports: connection errors are definitive, don't retry.
		if transportType != "stdio" {
			break
		}
	}
	if initErr != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("initialize: %w", markUnauthorized(initErr))
	}

	toolsResult, err := client.ListTools(ctx, mcpgo.ListToolsRequest{})
	if err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("list tools: %w", markUnauthorized(err))
	}

	if timeoutSec <= 0 {
		timeoutSec = 60
	}

	ss := &serverState{
		name:       name,
		transport:  transportType,
		client:     client,
		timeoutSec: timeoutSec,
		conn: connParams{
			command:  command,
			args:     args,
			env:      env,
			url:      url,
			headers:  headers,
			protocol: protocol,
		},
	}
	ss.session.Store(newConnSession(client, initResult, router))
	ss.clientPtr.Store(client)
	ss.connected.Store(true)

	slog.Debug("mcp.server.protocol",
		"server", name,
		"transport", transportType,
		"protocol_version", client.ProtocolVersion(),
		"era", ss.era().String(),
		"setting", string(protocol))

	return ss, toolsResult.Tools, nil
}

// newInitializeRequest builds the Initialize request. A legacy-only
// connection asks for 2025-11-25: mcp-go then skips the server/discover
// probe and runs the initialize handshake at once. Otherwise the client asks
// for the newest version, probes server/discover, and falls back to
// initialize for a legacy server.
//
// No extensions are declared here. mcp-go copies the Initialize capabilities
// into the _meta of every typed request, and a typed CallTool cannot decode a
// task result. goclaw declares tasks and events only on raw requests.
func newInitializeRequest(transportType string, protocol ProtocolMode, clientName string) mcpgo.InitializeRequest {
	req := mcpgo.InitializeRequest{}
	req.Params.ProtocolVersion = mcpgo.LATEST_PROTOCOL_VERSION
	if legacyOnly(transportType, protocol) {
		req.Params.ProtocolVersion = mcpgo.LATEST_LEGACY_PROTOCOL_VERSION
	}
	req.Params.ClientInfo = mcpgo.Implementation{Name: clientName, Version: clientInfoVersion}
	return req
}

// newConnSession records the era and the discover essentials of a client
// after Initialize.
func newConnSession(client *mcpclient.Client, res *mcpgo.InitializeResult, router *notificationRouter) *connSession {
	cs := &connSession{client: client, era: eraFromVersion(client.ProtocolVersion()), router: router}
	if cs.era == EraModern {
		info := &DiscoverInfo{ProtocolVersion: client.ProtocolVersion(), Capabilities: client.GetServerCapabilities()}
		if res != nil {
			info.ServerInfo = res.ServerInfo
			info.Instructions = res.Instructions
		}
		cs.discover = info
	}
	return cs
}

// markUnauthorized keeps the text "unauthorized (401)" in errors from an HTTP
// 401. mcp-go reports a 401 as "authorization required", and callers (for
// example the per-user credential purge) match the older text.
func markUnauthorized(err error) error {
	if err == nil || strings.Contains(strings.ToLower(err.Error()), "unauthorized (401)") {
		return err
	}
	if errors.Is(err, transport.ErrAuthorizationRequired) || errors.Is(err, transport.ErrUnauthorized) {
		return fmt.Errorf("unauthorized (401): %w", err)
	}
	return err
}

// livenessTimeout bounds one modern liveness probe.
const livenessTimeout = 5 * time.Second

// checkLiveness asks the server whether it is alive. Legacy connections send
// ping. On a modern connection mcp-go's Ping sends nothing (the method was
// removed in 2026-07-28), so goclaw sends server/discover with a 5 s deadline.
func checkLiveness(ctx context.Context, client *mcpclient.Client) error {
	if client == nil {
		return errors.New("no client")
	}
	if eraFromVersion(client.ProtocolVersion()) != EraModern {
		return client.Ping(ctx)
	}
	probeCtx, cancel := context.WithTimeout(ctx, livenessTimeout)
	defer cancel()
	_, err := client.Discover(probeCtx, mcpgo.DiscoverRequest{})
	return err
}

// connectServer creates a client, initializes the connection, discovers tools, and registers them.
// serverID is the MCP server UUID from DB (uuid.Nil for config-path servers).
// hints carries admin-authored description hints from MCPServerData.Settings.tool_hints;
// pass a zero-value ToolHints{} for config-path servers or when no hints are configured.
// toolAllow/toolDeny come from the agent's grant — non-allowed tools are filtered out
// upfront so the LLM never sees tools it cannot call. Pass nil for config-path servers.
// taskEvents is the server setting task_events (nil when not set).
func (m *Manager) connectServer(ctx context.Context, name, transportType, command string, args []string, env map[string]string, url string, headers map[string]string, toolPrefix string, timeoutSec int, serverID uuid.UUID, hints ToolHints, toolAllow, toolDeny []string, protocol ProtocolMode, taskEvents *TaskEventsBinding) error {
	ss, mcpTools, err := connectAndDiscover(ctx, name, transportType, command, args, env, url, headers, timeoutSec, protocol)
	if err != nil {
		return err
	}

	// Register tools (filtered by grant's tool_allow/tool_deny upfront)
	registeredNames := m.registerBridgeTools(ss, mcpTools, name, toolPrefix, timeoutSec, serverID, hints, toolAllow, toolDeny, taskEvents)
	ss.toolNames = registeredNames

	// Create health monitoring context
	hctx, hcancel := context.WithCancel(context.Background())
	ss.cancel = hcancel

	// Store server state BEFORE updating MCP group
	m.mu.Lock()
	m.servers[name] = ss
	m.mu.Unlock()

	if len(registeredNames) > 0 {
		m.registry.RegisterToolGroup("mcp:"+name, registeredNames)
		m.updateMCPGroup()
	}

	go m.healthLoop(hctx, ss)

	slog.Info("mcp.server.connected",
		"server", name,
		"transport", transportType,
		"tools", len(registeredNames),
	)

	return nil
}

// registerBridgeTools creates BridgeTools from MCP tool definitions and
// registers them in the Manager's registry. Returns registered tool names.
// serverID is the MCP server UUID (uuid.Nil for config-path servers).
// hints.Global applies to all tools; hints.Tools[name] adds a per-tool hint.
// toolAllow/toolDeny are evaluated via IsToolAllowed so filtered-out tools
// never get a BridgeTool created — the LLM never sees them, eliminating the
// "registered then runtime-denied" loop that produced repeated grant-revoked
// errors. Pass nil/nil to register every discovered tool.
func (m *Manager) registerBridgeTools(ss *serverState, mcpTools []mcpgo.Tool, serverName, toolPrefix string, timeoutSec int, serverID uuid.UUID, hints ToolHints, toolAllow, toolDeny []string, taskEvents *TaskEventsBinding) []string {
	var registeredNames []string
	var filteredOut []string
	for _, mcpTool := range mcpTools {
		if !IsToolAllowed(mcpTool.Name, toolAllow, toolDeny) {
			filteredOut = append(filteredOut, mcpTool.Name)
			continue
		}

		bt := NewBridgeTool(serverName, mcpTool, &ss.clientPtr, toolPrefix, timeoutSec, &ss.connected, serverID, m.grantChecker).
			WithHints(hints.Global, hints.HintFor(mcpTool.Name)).
			WithForceReconnect(func(reason string) { ss.requestForceReconnect(reason) }).
			WithTaskStarter(m.starter()).
			WithTaskEvents(taskEvents)

		if _, exists := m.registry.Get(bt.Name()); exists {
			slog.Warn("mcp.tool.name_collision",
				"server", serverName,
				"tool", bt.Name(),
				"action", "skipped",
			)
			continue
		}

		m.registry.Register(bt)
		registeredNames = append(registeredNames, bt.Name())
	}
	if len(filteredOut) > 0 {
		slog.Info("mcp.tools.filtered_at_register",
			"server", serverName,
			"server_id", serverID,
			"path", "manager",
			"filtered_count", len(filteredOut),
			"filtered_tools", filteredOut,
			"allow_size", len(toolAllow),
			"deny_size", len(toolDeny),
		)
	}
	return registeredNames
}

// connectViaPool acquires a shared connection from the pool and creates
// per-agent BridgeTools pointing to the shared client/connected pointers.
// serverID is the MCP server UUID from DB. hints carries admin-authored
// description hints from MCPServerData.Settings.tool_hints.
// toolAllow/toolDeny come from the agent's grant — non-allowed tools are filtered
// out upfront so the LLM never sees tools it cannot call.
func (m *Manager) connectViaPool(ctx context.Context, tenantID uuid.UUID, name, transportType, command string, args []string, env map[string]string, url string, headers map[string]string, toolPrefix string, timeoutSec int, serverID uuid.UUID, hints ToolHints, toolAllow, toolDeny []string, protocol ProtocolMode, taskEvents *TaskEventsBinding) error {
	entry, err := m.pool.Acquire(ctx, tenantID, name, transportType, command, args, env, url, headers, timeoutSec, WithProtocolMode(protocol))
	if err != nil {
		return err
	}

	// Create per-agent BridgeTools from the pool's shared connection
	registeredNames := m.registerPoolBridgeTools(entry, name, toolPrefix, timeoutSec, serverID, hints, toolAllow, toolDeny, taskEvents)

	// Track server state and per-agent tool names.
	// poolServers/poolToolNames keyed by plain name for Close() iteration.
	// poolKeys maps plain name → pool compound key for Release().
	m.mu.Lock()
	m.servers[name] = entry.state
	if m.poolServers == nil {
		m.poolServers = make(map[string]struct{})
	}
	m.poolServers[name] = struct{}{}
	if m.poolToolNames == nil {
		m.poolToolNames = make(map[string][]string)
	}
	m.poolToolNames[name] = registeredNames
	if m.poolKeys == nil {
		m.poolKeys = make(map[string]string)
	}
	m.poolKeys[name] = poolKey(tenantID, name)
	m.mu.Unlock()

	if len(registeredNames) > 0 {
		m.registry.RegisterToolGroup("mcp:"+name, registeredNames)
		m.updateMCPGroup()
	}

	slog.Info("mcp.server.connected_via_pool",
		"server", name,
		"transport", transportType,
		"tools", len(registeredNames),
	)

	return nil
}

// registerPoolBridgeTools creates BridgeTools from pool entry's discovered tools,
// pointing to the shared client/connected pointers. Returns registered tool names.
// serverID is the MCP server UUID from DB.
// hints.Global applies to all tools; hints.Tools[name] adds a per-tool hint.
// toolAllow/toolDeny are evaluated via IsToolAllowed so filtered-out tools never
// get a BridgeTool created (LLM never sees them).
func (m *Manager) registerPoolBridgeTools(entry *poolEntry, serverName, toolPrefix string, timeoutSec int, serverID uuid.UUID, hints ToolHints, toolAllow, toolDeny []string, taskEvents *TaskEventsBinding) []string {
	var registeredNames []string
	var filteredOut []string
	for _, mcpTool := range entry.tools {
		if !IsToolAllowed(mcpTool.Name, toolAllow, toolDeny) {
			filteredOut = append(filteredOut, mcpTool.Name)
			continue
		}

		bt := NewBridgeTool(serverName, mcpTool, &entry.state.clientPtr, toolPrefix, timeoutSec, &entry.state.connected, serverID, m.grantChecker).
			WithHints(hints.Global, hints.HintFor(mcpTool.Name)).
			WithForceReconnect(entry.RequestForceReconnect()).
			WithTaskStarter(m.starter()).
			WithTaskEvents(taskEvents)

		if _, exists := m.registry.Get(bt.Name()); exists {
			slog.Warn("mcp.tool.name_collision",
				"server", serverName,
				"tool", bt.Name(),
				"action", "skipped",
			)
			continue
		}

		m.registry.Register(bt)
		registeredNames = append(registeredNames, bt.Name())
	}
	if len(filteredOut) > 0 {
		slog.Info("mcp.tools.filtered_at_register",
			"server", serverName,
			"server_id", serverID,
			"path", "pool",
			"filtered_count", len(filteredOut),
			"filtered_tools", filteredOut,
			"allow_size", len(toolAllow),
			"deny_size", len(toolDeny),
		)
	}

	return registeredNames
}

// createClient creates the appropriate MCP client based on transport type.
func createClient(transportType, command string, args []string, env map[string]string, url string, headers map[string]string) (*mcpclient.Client, error) {
	switch transportType {
	case "stdio":
		envSlice := mapToEnvSlice(env)
		return mcpclient.NewStdioMCPClient(command, envSlice, args...)

	case "sse":
		var opts []transport.ClientOption
		if len(headers) > 0 {
			opts = append(opts, mcpclient.WithHeaders(headers))
		}
		return mcpclient.NewSSEMCPClient(url, opts...)

	case "streamable-http":
		var opts []transport.StreamableHTTPCOption
		if len(headers) > 0 {
			opts = append(opts, transport.WithHTTPHeaders(headers))
		}
		return mcpclient.NewStreamableHttpClient(url, opts...)

	default:
		return nil, fmt.Errorf("unsupported transport: %q", transportType)
	}
}

// newHealthTicker creates a ticker for health check intervals.
func newHealthTicker() *time.Ticker {
	return time.NewTicker(healthCheckInterval)
}

// isMethodNotFound returns true if the error indicates the server
// doesn't implement the "ping" method (still considered healthy).
func isMethodNotFound(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "method not found")
}

// healthLoop periodically checks the MCP server (see checkLiveness) and
// attempts reconnection on failure.
func (m *Manager) healthLoop(ctx context.Context, ss *serverState) {
	ticker := newHealthTicker()
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Skip ping while a force-reconnect is in flight. Some servers
			// answer `ping` in the post-reset "initializing" state; pinging
			// here would race the recovery goroutine and clobber
			// connected=true before the fresh Initialize completes.
			if ss.reconnPending.Load() {
				slog.Debug("mcp.server.health_skip", "server", ss.name, "reason", "reconnect_pending")
				continue
			}
			if err := checkLiveness(ctx, ss.client); err != nil {
				if isMethodNotFound(err) {
					ss.connected.Store(true)
					ss.mu.Lock()
					ss.reconnAttempts = 0
					ss.healthFailures = 0
					ss.lastErr = ""
					ss.mu.Unlock()
					continue
				}
				ss.mu.Lock()
				ss.healthFailures++
				failures := ss.healthFailures
				ss.lastErr = err.Error()
				ss.mu.Unlock()

				slog.Warn("mcp.server.health_failed", "server", ss.name, "error", err, "consecutive", failures)

				// Only mark disconnected and attempt reconnect after consecutive failures
				// to tolerate transient errors (e.g. 504 from upstream proxy).
				if failures >= healthFailThreshold {
					ss.connected.Store(false)
					m.tryReconnect(ctx, ss)
				}
			} else {
				ss.connected.Store(true)
				ss.mu.Lock()
				ss.reconnAttempts = 0
				ss.healthFailures = 0
				ss.lastErr = ""
				ss.mu.Unlock()
			}
		}
	}
}

// tryReconnect attempts to reconnect with exponential backoff.
func (m *Manager) tryReconnect(ctx context.Context, ss *serverState) {
	reconnectWithBackoff(ctx, ss, "mcp.server")
}

// reconnectWithBackoff implements the two-phase reconnect strategy shared by
// Manager.healthLoop and poolHealthLoop. Handles cooldown after exhausting
// max attempts, exponential backoff, fast-path ping (transient blips), and
// slow-path full reconnect (dead server-side session).
// logPrefix distinguishes log entries (e.g. "mcp.server" vs "mcp.pool").
func reconnectWithBackoff(ctx context.Context, ss *serverState, logPrefix string) {
	ss.mu.Lock()
	if ss.reconnAttempts >= maxReconnectAttempts {
		ss.lastErr = fmt.Sprintf("max reconnect attempts (%d) reached, entering cooldown", maxReconnectAttempts)
		ss.mu.Unlock()
		slog.Warn(logPrefix+".reconnect_cooldown", "server", ss.name, "cooldown", reconnectCooldown)
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectCooldown):
		}
		ss.mu.Lock()
		ss.reconnAttempts = 0
		ss.mu.Unlock()
		return // will retry on next health tick
	}
	ss.reconnAttempts++
	attempt := ss.reconnAttempts
	ss.mu.Unlock()

	backoff := min(initialBackoff*time.Duration(1<<(attempt-1)), maxBackoff)
	slog.Info(logPrefix+".reconnecting", "server", ss.name, "attempt", attempt, "backoff", backoff)

	select {
	case <-ctx.Done():
		return
	case <-time.After(backoff):
	}

	// Fast path: check the existing client — works for transient network
	// blips where the server-side session is still alive. checkLiveness
	// contacts the server in both eras (Ping alone is a no-op on modern).
	if err := checkLiveness(ctx, ss.client); err == nil {
		ss.connected.Store(true)
		ss.mu.Lock()
		ss.reconnAttempts = 0
		ss.healthFailures = 0
		ss.lastErr = ""
		ss.mu.Unlock()
		slog.Info(logPrefix+".reconnected", "server", ss.name)
		return
	}

	// Slow path: server-side session is dead (container restart, OOM, etc.).
	if fullReconnect(ctx, ss) {
		slog.Info(logPrefix+".reconnected", "server", ss.name, "method", "full_reconnect")
	}
}

// fullReconnect creates a fresh MCP client, atomically swaps it into serverState,
// and closes the old one. Returns true on success. Used by reconnectWithBackoff
// as the slow path when pinging the old client fails.
//
// The new client is created and validated FIRST, then swapped via clientPtr.Store()
// so BridgeTools see the new client immediately. The old client is closed AFTER
// the swap to avoid a window where ss.client points to a closed client.
//
// NOTE: Does not re-discover tools (ListTools). If the MCP server restarts with
// a different tool set, changes won't be reflected until the Manager reconnects.
func fullReconnect(ctx context.Context, ss *serverState) bool {
	slog.Info("mcp.full_reconnect", "server", ss.name, "transport", ss.transport)

	newClient, err := createClient(ss.transport, ss.conn.command, ss.conn.args, ss.conn.env, ss.conn.url, ss.conn.headers)
	if err != nil {
		slog.Warn("mcp.reconnect_create_failed", "server", ss.name, "error", err)
		return false
	}
	router := attachNotificationRouter(newClient)

	if ss.transport != "stdio" {
		if err := newClient.Start(ctx); err != nil {
			_ = newClient.Close()
			slog.Warn("mcp.reconnect_start_failed", "server", ss.name, "error", err)
			return false
		}
	}

	initRes, err := newClient.Initialize(ctx, newInitializeRequest(ss.transport, ss.conn.protocol, clientInfoName))
	if err != nil {
		_ = newClient.Close()
		slog.Warn("mcp.reconnect_init_failed", "server", ss.name, "error", err)
		return false
	}

	// Swap atomically: store new client, then close old.
	// BridgeTools use clientPtr.Load() so they see the new client immediately.
	// The era is read again: the server may have moved to or from 2026-07-28.
	oldClient := ss.client
	ss.client = newClient
	ss.session.Store(newConnSession(newClient, initRes, router))
	ss.clientPtr.Store(newClient)
	ss.connected.Store(true)
	ss.mu.Lock()
	ss.reconnAttempts = 0
	ss.healthFailures = 0
	ss.lastErr = ""
	ss.mu.Unlock()

	_ = oldClient.Close()
	return true
}
