package mcp

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

// searchModeFixture registers the tools of several servers in a fresh manager
// (no MCP connection) and then runs the search-mode split. Servers in
// taskServers get a task_events binding on each of their tools.
func searchModeFixture(t *testing.T, servers map[string]int, taskServers ...string) (*Manager, *tools.Registry) {
	t.Helper()
	reg := tools.NewRegistry()
	m := NewManager(reg)
	m.mu.Lock()
	for server, count := range servers {
		ss := &serverState{name: server}
		for i := range count {
			bt := makeBridgeTool(server, fmt.Sprintf("tool_%02d", i))
			if slices.Contains(taskServers, server) {
				bt.WithTaskEvents(&TaskEventsBinding{Event: "run.status_changed", TaskArgument: "run_id"})
			}
			reg.Register(bt)
			ss.toolNames = append(ss.toolNames, bt.Name())
		}
		m.servers[server] = ss
	}
	m.mu.Unlock()
	m.maybeEnterSearchMode()
	return m, reg
}

func inlineToolNames(reg *tools.Registry, m *Manager) []string {
	var names []string
	for _, name := range m.ToolNames() {
		if _, ok := reg.Get(name); ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

func TestSearchModeKeepsTaskEventServerToolsInline(t *testing.T) {
	servers := map[string]int{"gitlab": 39, "graylog": 9, "wiki": 7, "game-db": 7, "archon-mcp": 3}
	m, reg := searchModeFixture(t, servers, "archon-mcp")

	if !m.IsSearchMode() {
		t.Fatal("expected search mode above the inline limit")
	}
	for i := range 3 {
		name := makeBridgeTool("archon-mcp", fmt.Sprintf("tool_%02d", i)).Name()
		if _, ok := reg.Get(name); !ok {
			t.Errorf("tool %q of the task_events server must stay inline", name)
		}
		m.mu.RLock()
		_, deferred := m.deferredTools[name]
		m.mu.RUnlock()
		if deferred {
			t.Errorf("tool %q of the task_events server must not be deferred", name)
		}
	}
	if got := len(inlineToolNames(reg, m)); got != mcpToolInlineMaxCount {
		t.Errorf("inline tools = %d, want the limit %d", got, mcpToolInlineMaxCount)
	}
	if got, want := deferredCount(m), 65-mcpToolInlineMaxCount; got != want {
		t.Errorf("deferred tools = %d, want %d", got, want)
	}
}

func TestSearchModeInlineSetIsStableAcrossLoads(t *testing.T) {
	servers := map[string]int{"gitlab": 39, "graylog": 9, "wiki": 7, "game-db": 7, "archon-mcp": 3}
	m, reg := searchModeFixture(t, servers, "archon-mcp")
	first := inlineToolNames(reg, m)
	for range 25 {
		m2, reg2 := searchModeFixture(t, servers, "archon-mcp")
		if got := inlineToolNames(reg2, m2); !slices.Equal(got, first) {
			t.Fatalf("inline set changed between loads:\nfirst %v\nnow   %v", first, got)
		}
	}
	// The rest is picked in server name order, then tool name order.
	if !slices.Contains(first, makeBridgeTool("game-db", "tool_06").Name()) {
		t.Error("game-db sorts before gitlab and must be inline in full")
	}
	if slices.Contains(first, makeBridgeTool("wiki", "tool_00").Name()) {
		t.Error("wiki sorts last and must be deferred")
	}
}

func TestSearchModeKeepsAllTaskEventToolsEvenOverLimit(t *testing.T) {
	servers := map[string]int{"big-task-server": mcpToolInlineMaxCount + 5, "other": 10}
	m, reg := searchModeFixture(t, servers, "big-task-server")
	for i := range mcpToolInlineMaxCount + 5 {
		name := makeBridgeTool("big-task-server", fmt.Sprintf("tool_%02d", i)).Name()
		if _, ok := reg.Get(name); !ok {
			t.Fatalf("tool %q of the task_events server must stay inline", name)
		}
	}
	if got := deferredCount(m); got != 10 {
		t.Errorf("deferred tools = %d, want the 10 tools of the other server", got)
	}
}
