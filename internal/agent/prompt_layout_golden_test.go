package agent

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

var updatePromptGolden = flag.Bool("update-prompt-golden", false, "rewrite prompt layout golden files")

// layoutTestTool is a minimal tool with a fixed schema for layout tests.
type layoutTestTool struct{ name string }

func (t layoutTestTool) Name() string        { return t.name }
func (t layoutTestTool) Description() string { return "layout test tool " + t.name }
func (t layoutTestTool) Parameters() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}}
}
func (t layoutTestTool) Execute(context.Context, map[string]any) *tools.Result {
	return &tools.Result{ForLLM: "ok"}
}

// newPromptLayoutLoop builds a Loop with a real registry that is enough for
// buildMessages and buildFilteredTools.
func newPromptLayoutLoop(toolNames ...string) *Loop {
	reg := tools.NewRegistry()
	for _, n := range toolNames {
		reg.Register(layoutTestTool{name: n})
	}
	return &Loop{
		id:          "layout-agent",
		displayName: "Layout Agent",
		model:       "stub-model",
		provider:    &stubProvider{},
		tools:       reg,
		registry:    reg,
		workspace:   "/tmp/layout-ws",
	}
}

var promptDateLine = regexp.MustCompile(`Current date: [^\n]*\(UTC\)`)

func normalizePromptDate(s string) string {
	return promptDateLine.ReplaceAllString(s, "Current date: <date> (UTC)")
}

func checkPromptGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *updatePromptGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v (run with -update-prompt-golden)", err)
	}
	if string(want) != got {
		t.Fatalf("prompt layout changed for %s\n--- want ---\n%s\n--- got ---\n%s", name, want, got)
	}
}

// With the stable-prefix flag off, buildMessages must keep today's layout:
// one system message with the cache boundary marker and per-turn sections,
// then the history, then the raw user message.
func TestBuildMessages_DefaultLayoutGolden(t *testing.T) {
	l := newPromptLayoutLoop("read_file", "web_search")
	history := []providers.Message{
		{Role: "user", Content: "earlier question"},
		{Role: "assistant", Content: "earlier answer"},
	}
	msgs, _ := l.buildMessages(context.Background(), history, "", "current question", "extra instructions",
		"sess-1", "ws", "", "", "Title", "chat-1", "direct", "user-1", "Alice", 0, nil, false)

	var sb strings.Builder
	for _, m := range msgs {
		sb.WriteString("=== " + m.Role + "\n" + m.Content + "\n")
	}
	got := normalizePromptDate(sb.String())
	checkPromptGolden(t, "prompt_layout_default.golden", got)

	if !strings.Contains(msgs[0].Content, CacheBoundaryMarker) {
		t.Fatal("default layout must keep the cache boundary marker in the system message")
	}
	if last := msgs[len(msgs)-1]; last.Role != "user" || last.Content != "current question" {
		t.Fatalf("last message = %+v, want the raw user message", last)
	}
}
