package acpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	mcpclient "github.com/mark3labs/mcp-go/client"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
)

const maxMCPFrameBytes = 480 << 10

var (
	ErrMCPFrameTooLarge = errors.New("MCP frame exceeds size limit")
	validEnvName        = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type localMCPServer struct {
	mu         sync.Mutex
	name       string
	client     *mcpclient.Client
	tools      map[string]string
	secrets    []string
	definition MCPServer
	cwd        string
}

// SessionMCPManager owns all Buzz-supplied MCP processes for one ACP session.
type SessionMCPManager struct {
	workspaceRoot string
	execRoots     []string
	mu            sync.RWMutex
	servers       map[string]*localMCPServer
	closed        bool
}

func NewSessionMCPManager(workspaceRoot string, execRoots []string) (*SessionMCPManager, error) {
	root, err := canonicalDirectory(workspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve MCP workspace root: %w", err)
	}
	if len(execRoots) == 0 {
		return nil, errors.New("MCP executable root is required")
	}
	canonicalRoots := make([]string, 0, len(execRoots))
	for _, candidate := range execRoots {
		resolved, resolveErr := canonicalDirectory(candidate)
		if resolveErr != nil {
			return nil, fmt.Errorf("resolve MCP executable root: %w", resolveErr)
		}
		canonicalRoots = append(canonicalRoots, resolved)
	}
	return &SessionMCPManager{workspaceRoot: root, execRoots: canonicalRoots, servers: make(map[string]*localMCPServer)}, nil
}

func (m *SessionMCPManager) Start(ctx context.Context, cwd string, definitions []MCPServer) ([]acpbridge.ToolSchema, error) {
	workingDirectory, err := m.resolveWorkspace(cwd)
	if err != nil {
		return nil, err
	}
	if len(definitions) > MaxMCPServersPerSession {
		return nil, errors.New("too many MCP servers")
	}
	started := make([]*localMCPServer, 0, len(definitions))
	var schemas []acpbridge.ToolSchema
	for _, definition := range definitions {
		server, discovered, startErr := m.startServer(ctx, workingDirectory, definition)
		if startErr != nil {
			closeLocalMCPServers(started)
			return nil, startErr
		}
		started = append(started, server)
		schemas = append(schemas, discovered...)
		if len(schemas) > MaxTools {
			closeLocalMCPServers(started)
			return nil, errors.New("too many MCP tools")
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		closeLocalMCPServers(started)
		return nil, errors.New("MCP manager is closed")
	}
	for _, server := range started {
		if _, exists := m.servers[server.name]; exists {
			closeLocalMCPServers(started)
			return nil, errors.New("duplicate MCP server name")
		}
		m.servers[server.name] = server
	}
	return schemas, nil
}

func (m *SessionMCPManager) Call(ctx context.Context, registeredName string, arguments map[string]any) (string, bool, error) {
	m.mu.RLock()
	var server *localMCPServer
	var upstream string
	for _, candidate := range m.servers {
		if name, ok := candidate.tools[registeredName]; ok {
			server, upstream = candidate, name
			break
		}
	}
	m.mu.RUnlock()
	if server == nil {
		return "", true, errors.New("MCP tool not found")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.client == nil {
		if err := m.restartServer(server); err != nil {
			return "", true, err
		}
	}
	result, err := server.client.CallTool(ctx, mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{Name: upstream, Arguments: arguments}})
	if err != nil {
		if ctx.Err() != nil {
			_ = server.client.Close()
			server.client = nil
			_ = m.restartServer(server)
		}
		return "", true, errors.New("MCP tool call failed")
	}
	payload, err := json.Marshal(result.Content)
	if err != nil {
		return "", true, errors.New("MCP tool result is invalid")
	}
	var wire any
	if json.Unmarshal(payload, &wire) != nil {
		return "", true, errors.New("MCP tool result is invalid")
	}
	payload, err = json.Marshal(redactSecretValues(wire, server.secrets))
	if err != nil {
		return "", true, errors.New("MCP tool result is invalid")
	}
	if len(payload) > maxMCPFrameBytes {
		return "", true, ErrMCPFrameTooLarge
	}
	return string(payload), result.IsError, nil
}

func (m *SessionMCPManager) restartServer(server *localMCPServer) error {
	restartCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	replacement, _, err := m.startServer(restartCtx, server.cwd, server.definition)
	if err != nil {
		return errors.New("restart MCP server failed")
	}
	server.client = replacement.client
	server.tools = replacement.tools
	server.secrets = replacement.secrets
	return nil
}

func (m *SessionMCPManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	servers := m.servers
	m.servers = make(map[string]*localMCPServer)
	m.mu.Unlock()
	var joined error
	for _, server := range servers {
		server.mu.Lock()
		if server.client != nil {
			joined = errors.Join(joined, server.client.Close())
			server.client = nil
		}
		server.mu.Unlock()
	}
	return joined
}

func (m *SessionMCPManager) startServer(ctx context.Context, cwd string, definition MCPServer) (*localMCPServer, []acpbridge.ToolSchema, error) {
	command, err := m.resolveExecutable(definition.Command)
	if err != nil {
		return nil, nil, err
	}
	executableIdentity, err := os.Stat(command)
	if err != nil {
		return nil, nil, errors.New("MCP command is unavailable")
	}
	if interpreterHasInlineCode(command, definition.Args) {
		return nil, nil, errors.New("MCP interpreter inline code is not allowed")
	}
	environment, secrets, err := minimalMCPEnvironment(definition.Environment())
	if err != nil {
		return nil, nil, err
	}
	transport := newBoundedStdioTransport(command, definition.Args, environment, cwd, executableIdentity)
	client := mcpclient.NewClient(transport)
	if err := client.Start(ctx); err != nil {
		return nil, nil, errors.New("start MCP server failed")
	}
	initRequest := mcpgo.InitializeRequest{}
	initRequest.Params.ProtocolVersion = mcpgo.LATEST_PROTOCOL_VERSION
	initRequest.Params.ClientInfo = mcpgo.Implementation{Name: "goclaw-acp", Version: "1"}
	if _, err := client.Initialize(ctx, initRequest); err != nil {
		_ = client.Close()
		return nil, nil, errors.New("initialize MCP server failed")
	}
	toolsResult, err := client.ListTools(ctx, mcpgo.ListToolsRequest{})
	if err != nil {
		_ = client.Close()
		return nil, nil, errors.New("discover MCP tools failed")
	}
	if stringContainsSecret(definition.Name, secrets) {
		_ = client.Close()
		return nil, nil, errors.New("MCP server identifier contains a secret")
	}
	server := &localMCPServer{name: definition.Name, client: client, tools: make(map[string]string), secrets: secrets, definition: definition, cwd: cwd}
	schemas := make([]acpbridge.ToolSchema, 0, len(toolsResult.Tools))
	for _, tool := range toolsResult.Tools {
		if stringContainsSecret(tool.Name, secrets) {
			_ = client.Close()
			return nil, nil, errors.New("MCP tool identifier contains a secret")
		}
		registered := registeredMCPToolName(definition.Name, tool.Name)
		if registered == "" {
			_ = client.Close()
			return nil, nil, errors.New("MCP tool has an invalid name")
		}
		if _, exists := server.tools[registered]; exists {
			_ = client.Close()
			return nil, nil, errors.New("MCP tool name collision")
		}
		schemaBytes, marshalErr := json.Marshal(tool)
		if marshalErr != nil {
			_ = client.Close()
			return nil, nil, errors.New("MCP tool schema is invalid")
		}
		var wire map[string]any
		if json.Unmarshal(schemaBytes, &wire) != nil {
			_ = client.Close()
			return nil, nil, errors.New("MCP tool schema is invalid")
		}
		filteredValue, filterErr := redactMCPDiscovery(wire, secrets)
		if filterErr != nil {
			_ = client.Close()
			return nil, nil, filterErr
		}
		filtered := filteredValue.(map[string]any)
		input, _ := filtered["inputSchema"].(map[string]any)
		description, _ := filtered["description"].(string)
		server.tools[registered] = tool.Name
		schemas = append(schemas, acpbridge.ToolSchema{Name: registered, ServerName: definition.Name, ToolName: tool.Name, Description: description, InputSchema: input})
	}
	return server, schemas, nil
}

func (m *SessionMCPManager) resolveWorkspace(candidate string) (string, error) {
	resolved, err := canonicalDirectory(candidate)
	if err != nil {
		return "", errors.New("invalid MCP working directory")
	}
	if !pathWithin(m.workspaceRoot, resolved) {
		return "", errors.New("MCP working directory escapes workspace root")
	}
	return resolved, nil
}

func (m *SessionMCPManager) resolveExecutable(candidate string) (string, error) {
	if !filepath.IsAbs(candidate) {
		return "", errors.New("MCP command must be an absolute path")
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(candidate))
	if err != nil {
		return "", errors.New("MCP command is unavailable")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return "", errors.New("MCP command is not executable")
	}
	if !executableOwnedByCurrentUser(info) {
		return "", errors.New("MCP command has an untrusted owner")
	}
	for _, root := range m.execRoots {
		if pathWithin(root, resolved) {
			return resolved, nil
		}
	}
	return "", errors.New("MCP command is outside approved executable roots")
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)) && !filepath.IsAbs(relative)
}

func minimalMCPEnvironment(entries []EnvVariable) ([]string, []string, error) {
	values := make(map[string]string)
	for _, name := range []string{"HOME", "LANG", "LC_ALL", "PATH", "TMPDIR", "TZ", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "TEMP", "TMP"} {
		if value := os.Getenv(name); value != "" {
			values[name] = value
		}
	}
	secrets := make([]string, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		upperName := strings.ToUpper(entry.Name)
		if !validEnvName.MatchString(entry.Name) || strings.ContainsRune(entry.Value, '\x00') || strings.HasPrefix(upperName, "GOCLAW_") {
			return nil, nil, errors.New("invalid MCP environment entry")
		}
		if _, exists := seen[entry.Name]; exists {
			return nil, nil, errors.New("duplicate or protected MCP environment entry")
		}
		seen[entry.Name] = struct{}{}
		values[entry.Name] = entry.Value
		if entry.Value != "" {
			secrets = append(secrets, entry.Value)
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	environment := make([]string, 0, len(keys))
	for _, key := range keys {
		environment = append(environment, key+"="+values[key])
	}
	return environment, secrets, nil
}

func redactSecretValues(value any, secrets []string) any {
	switch current := value.(type) {
	case map[string]any:
		filtered := make(map[string]any, len(current))
		for key, child := range current {
			filtered[key] = redactSecretValues(child, secrets)
		}
		return filtered
	case []any:
		filtered := make([]any, len(current))
		for index, child := range current {
			filtered[index] = redactSecretValues(child, secrets)
		}
		return filtered
	case string:
		for _, secret := range secrets {
			current = strings.ReplaceAll(current, secret, "[REDACTED]")
		}
		return current
	default:
		return value
	}
}

func redactMCPDiscovery(value any, secrets []string) (any, error) {
	switch current := value.(type) {
	case map[string]any:
		filtered := make(map[string]any, len(current))
		for key, child := range current {
			if stringContainsSecret(key, secrets) {
				return nil, errors.New("MCP discovery schema key contains a secret")
			}
			clean, err := redactMCPDiscovery(child, secrets)
			if err != nil {
				return nil, err
			}
			filtered[key] = clean
		}
		return filtered, nil
	case []any:
		filtered := make([]any, len(current))
		for index, child := range current {
			clean, err := redactMCPDiscovery(child, secrets)
			if err != nil {
				return nil, err
			}
			filtered[index] = clean
		}
		return filtered, nil
	case string:
		return redactSecretValues(current, secrets), nil
	default:
		return value, nil
	}
}

func stringContainsSecret(value string, secrets []string) bool {
	for _, secret := range secrets {
		if secret != "" && strings.Contains(value, secret) {
			return true
		}
	}
	return false
}

func registeredMCPToolName(server, tool string) string {
	normalize := func(value string) string {
		var result strings.Builder
		for _, char := range strings.ToLower(value) {
			switch {
			case char >= 'a' && char <= 'z', char >= '0' && char <= '9', char == '_':
				result.WriteRune(char)
			case char == '-', char == '.', char == ' ':
				result.WriteByte('_')
			default:
				return ""
			}
		}
		return strings.Trim(result.String(), "_")
	}
	server, tool = normalize(server), normalize(tool)
	if server == "" || tool == "" {
		return ""
	}
	return "mcp_" + server + "__" + tool
}

func interpreterHasInlineCode(command string, args []string) bool {
	name := strings.ToLower(filepath.Base(command))
	isInterpreter := strings.HasPrefix(name, "python") || strings.HasPrefix(name, "node") || name == "sh" || name == "bash" || name == "zsh" || name == "cmd.exe" || name == "powershell.exe" || name == "pwsh.exe"
	if !isInterpreter {
		return false
	}
	for _, arg := range args {
		switch strings.ToLower(arg) {
		case "-c", "-e", "/c", "-command", "-encodedcommand":
			return true
		}
	}
	return false
}

func closeLocalMCPServers(servers []*localMCPServer) {
	for _, server := range servers {
		server.mu.Lock()
		if server.client != nil {
			_ = server.client.Close()
			server.client = nil
		}
		server.mu.Unlock()
	}
}
