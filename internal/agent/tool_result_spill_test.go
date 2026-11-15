package agent

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

// quarterCounter counts one token per four bytes of content.
type quarterCounter struct{}

func (quarterCounter) Count(_ string, text string) int { return len(text) / 4 }
func (quarterCounter) CountMessages(_ string, msgs []providers.Message) int {
	n := 0
	for _, m := range msgs {
		n += len(m.Content) / 4
	}
	return n
}
func (quarterCounter) CountToolSchemas(_ string, _ []providers.ToolDefinition) int { return 0 }
func (quarterCounter) ModelContextWindow(_ string) int                             { return 200_000 }

// newSpillLoop returns a Loop wired like production for the spill guard: the
// registry holds read_file with an alias, and the policy has a call prefix.
func newSpillLoop(t *testing.T, dataDir string, maxTokens int) *Loop {
	t.Helper()
	reg := tools.NewRegistry()
	reg.Register(tools.NewReadFileTool(t.TempDir(), true))
	reg.RegisterAlias("Read", "read_file")
	return &Loop{
		registry:            reg,
		agentToolPolicy:     &config.ToolPolicySpec{ToolCallPrefix: "proxy_"},
		toolResultMaxTokens: maxTokens,
		toolResultSpill:     tools.NewToolResultSpillStore(dataDir),
	}
}

func spillState(sessionKey string) *pipeline.RunState {
	return pipeline.NewRunState(&pipeline.RunInput{SessionKey: sessionKey, RunID: "run-1"}, nil, "test-model", nil)
}

func spillCtx(workspace string, tenant uuid.UUID) context.Context {
	ctx := tools.WithToolWorkspace(context.Background(), workspace)
	return store.WithTenantID(ctx, tenant)
}

func toolMsg(content string) providers.Message {
	return providers.Message{Role: "tool", ToolCallID: "tc-1", Content: content}
}

// vietnameseBlob is multibyte text of exactly n runes, with a line break every 38 runes.
func vietnameseBlob(n int) string {
	var b strings.Builder
	unit := "Tiểu Hổ đọc kết quả tìm kiếm mã nguồn.\n"
	for utf8.RuneCountInString(b.String()) < n {
		b.WriteString(unit)
	}
	return string([]rune(b.String())[:n])
}

func TestBoundToolResult_SpillsAboveThreshold(t *testing.T) {
	workspace, dataDir := t.TempDir(), t.TempDir()
	l := newSpillLoop(t, dataDir, 1000)
	bound := l.makeBoundToolResult(quarterCounter{})
	tenant := uuid.New()

	content := vietnameseBlob(30000) // far above 1000 tokens at 4 bytes per token
	tc := providers.ToolCall{ID: "tc-1", Name: "mcp_gitlab___search_project_code"}
	got := bound(spillCtx(workspace, tenant), spillState("sess-a"), tc, toolMsg(content))

	if got.Role != "tool" || got.ToolCallID != "tc-1" {
		t.Fatalf("Role/ToolCallID changed: %+v", got)
	}

	hash := tools.ToolResultSessionHash(tenant, "sess-a")
	dir := filepath.Join(workspace, ".tool-results", "session-"+hash)
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("spill dir %s: entries=%v err=%v", dir, entries, err)
	}
	name := entries[0].Name()
	if !strings.HasSuffix(name, "-mcp_gitlab___search_project_code.txt") || len(name) != 12+1+len("mcp_gitlab___search_project_code")+4 {
		t.Errorf("file name = %q, want 12 random hex + tool name", name)
	}
	info, _ := entries[0].Info()
	if info.Mode().Perm() != 0o600 {
		t.Errorf("file mode = %v, want 0600", info.Mode().Perm())
	}
	if di, _ := os.Stat(dir); di.Mode().Perm() != 0o700 {
		t.Errorf("session dir mode = %v, want 0700", di.Mode().Perm())
	}
	saved, _ := os.ReadFile(filepath.Join(dir, name))
	if string(saved) != content {
		t.Errorf("spill file differs from the full result (%d vs %d bytes)", len(saved), len(content))
	}

	rel := ".tool-results/session-" + hash + "/" + name
	runes := []rune(content)
	want := fmt.Sprintf("%s\n\n[...]\n\n%s\n\n[Tool result too large: about %d tokens (%d chars). Showing the first 6000 and last 2000 chars. Full result saved to: %s. Use read_file with offset/limit on this path, or exec grep on it, to see the rest.]",
		string(runes[:6000]), string(runes[len(runes)-2000:]), len(content)/4, len(runes), rel)
	if got.Content != want {
		t.Errorf("notice mismatch:\n got: %.300s...\nwant: %.300s...", got.Content, want)
	}
}

func TestBoundToolResult_ReadFileAndAliasExempt(t *testing.T) {
	workspace := t.TempDir()
	l := newSpillLoop(t, t.TempDir(), 1000)
	bound := l.makeBoundToolResult(quarterCounter{})
	content := strings.Repeat("x", 40000)

	for _, name := range []string{"read_file", "Read", "proxy_read_file", "proxy_Read"} {
		got := bound(spillCtx(workspace, uuid.New()), spillState("s"), providers.ToolCall{ID: "tc-1", Name: name}, toolMsg(content))
		if got.Content != content {
			t.Errorf("%s: result was changed, want read_file output exempt", name)
		}
	}
	if _, err := os.Stat(filepath.Join(workspace, ".tool-results")); !os.IsNotExist(err) {
		t.Errorf("exempt calls created a spill dir (err=%v)", err)
	}

	// A different tool with the same size still spills.
	got := bound(spillCtx(workspace, uuid.New()), spillState("s"), providers.ToolCall{ID: "tc-2", Name: "exec"}, toolMsg(content))
	if got.Content == content {
		t.Error("exec result of the same size was not spilled")
	}
}

func TestBoundToolResult_BelowThresholdAndOffUnchanged(t *testing.T) {
	workspace := t.TempDir()
	content := strings.Repeat("y", 3000) // 750 tokens
	tc := providers.ToolCall{ID: "tc-1", Name: "exec"}

	l := newSpillLoop(t, t.TempDir(), 1000)
	got := l.makeBoundToolResult(quarterCounter{})(spillCtx(workspace, uuid.New()), spillState("s"), tc, toolMsg(content))
	if got.Content != content {
		t.Error("result below the threshold was changed")
	}

	// Bytes above the threshold but tokens below it: the token count decides.
	l = newSpillLoop(t, t.TempDir(), 1000)
	l.toolResultMaxTokens = 800
	short := providers.Message{Role: "tool", ToolCallID: "tc-1", Content: strings.Repeat("y", 3000)}
	if got := l.makeBoundToolResult(quarterCounter{})(spillCtx(workspace, uuid.New()), spillState("s"), tc, short); got.Content != short.Content {
		t.Error("result of 750 tokens was spilled at threshold 800")
	}

	l = newSpillLoop(t, t.TempDir(), 0)
	if l.makeBoundToolResult(quarterCounter{}) != nil {
		t.Error("threshold 0 must install no guard")
	}

	l = newSpillLoop(t, t.TempDir(), 1000)
	l.toolResultSpill = nil
	if l.makeBoundToolResult(quarterCounter{}) != nil {
		t.Error("a loop without a spill store must install no guard")
	}
}

func TestBoundToolResult_TenantWorkspacesIsolated(t *testing.T) {
	base, dataDir := t.TempDir(), t.TempDir()
	wsA := filepath.Join(base, "tenants", "a")
	wsB := filepath.Join(base, "tenants", "b")
	for _, ws := range []string{wsA, wsB} {
		if err := os.MkdirAll(ws, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	l := newSpillLoop(t, dataDir, 1000)
	bound := l.makeBoundToolResult(quarterCounter{})
	tenantA, tenantB := uuid.New(), uuid.New()
	content := strings.Repeat("z", 20000)
	tc := providers.ToolCall{ID: "tc-1", Name: "exec"}

	gotA := bound(spillCtx(wsA, tenantA), spillState("same-key"), tc, toolMsg(content))
	gotB := bound(spillCtx(wsB, tenantB), spillState("same-key"), tc, toolMsg(content))

	dirA, _ := filepath.Glob(filepath.Join(wsA, ".tool-results", "session-*"))
	dirB, _ := filepath.Glob(filepath.Join(wsB, ".tool-results", "session-*"))
	if len(dirA) != 1 || len(dirB) != 1 {
		t.Fatalf("spill dirs: A=%v B=%v, want one each in their own workspace", dirA, dirB)
	}
	if filepath.Base(dirA[0]) == filepath.Base(dirB[0]) {
		t.Errorf("tenants A and B share the session dir name %s", filepath.Base(dirA[0]))
	}
	for _, g := range []struct{ name, text, other string }{
		{"A", gotA.Content, "tenants/b"}, {"B", gotB.Content, "tenants/a"},
	} {
		if !strings.Contains(g.text, "saved to: .tool-results/session-") {
			t.Errorf("tenant %s: notice path is not workspace-relative: %.200s", g.name, g.text[len(g.text)-250:])
		}
		if strings.Contains(g.text, g.other) || strings.Contains(g.text, base) {
			t.Errorf("tenant %s: notice leaks an absolute or foreign path", g.name)
		}
	}
}

func TestBoundToolResult_SaveFailureKeepsInlineResult(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	l := newSpillLoop(t, t.TempDir(), 1000)
	bound := l.makeBoundToolResult(quarterCounter{})
	content := strings.Repeat("q", 20000)
	tc := providers.ToolCall{ID: "tc-1", Name: "exec"}

	// The workspace is a regular file, so the spill directory cannot be created.
	notDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notDir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, ws := range map[string]string{"workspace is a file": notDir, "no workspace": ""} {
		in := toolMsg(content)
		got := bound(spillCtx(ws, uuid.New()), spillState("s"), tc, in)
		if got.Content != content || got.IsError {
			t.Errorf("%s: result changed or flagged as error after a save failure", name)
		}
	}
	if !strings.Contains(logs.String(), "tool_result.spill_failed") {
		t.Errorf("no spill_failed warning logged: %q", logs.String())
	}
}

func TestBoundToolResult_WrapsLongLines(t *testing.T) {
	workspace := t.TempDir()
	l := newSpillLoop(t, t.TempDir(), 1000)
	bound := l.makeBoundToolResult(quarterCounter{})
	content := `{"results":[` + strings.Repeat(`{"file":"a.go","line":1,"text":"Tiểu Hổ"},`, 7500) + `]}` // one line
	if strings.Contains(content, "\n") {
		t.Fatal("fixture must be one line")
	}
	got := bound(spillCtx(workspace, uuid.New()), spillState("s"), providers.ToolCall{ID: "tc-1", Name: "mcp_search"}, toolMsg(content))

	files, _ := filepath.Glob(filepath.Join(workspace, ".tool-results", "session-*", "*.txt"))
	if len(files) != 1 {
		t.Fatalf("spill files = %v", files)
	}
	saved, _ := os.ReadFile(files[0])
	lines := strings.Split(string(saved), "\n")
	if len(lines) < 150 {
		t.Errorf("saved file has %d lines, want at least 150 for a 300k-rune line", len(lines))
	}
	for i, line := range lines {
		if utf8.RuneCountInString(line) > 2000 {
			t.Fatalf("line %d has %d runes, want at most 2000", i, utf8.RuneCountInString(line))
		}
	}
	if strings.ReplaceAll(string(saved), "\n", "") != content {
		t.Error("wrapping changed characters other than inserting newlines")
	}
	wantNote := fmt.Sprintf("The file has %d lines; lines longer than 2000 chars were wrapped.", len(lines))
	if !strings.Contains(got.Content, wantNote) {
		t.Errorf("notice lacks the line-count note %q", wantNote)
	}

	// read_file pages the wrapped file under its 50,000 char cap.
	rf := tools.NewReadFileTool(workspace, true)
	rel := got.Content[strings.Index(got.Content, ".tool-results/"):]
	rel = rel[:strings.Index(rel, ".txt")+4]
	res := rf.Execute(context.Background(), map[string]any{"path": rel, "offset": float64(0), "limit": float64(20)})
	if res.IsError {
		t.Fatalf("read_file failed: %s", res.ForLLM)
	}
	if n := utf8.RuneCountInString(res.ForLLM); n > 50000+300 {
		t.Errorf("read_file page has %d chars, want it capped near 50000", n)
	}
}

// TestPipeline_SpillGuardKeepsProductionCaseAlive replays the 2026-11-15 case
// with the real guard and the real history fit: a tool result of about 1.47M
// tokens against a history budget of 206,621. Without the guard the next LLM
// call is rejected and the run aborts.
func TestPipeline_SpillGuardKeepsProductionCaseAlive(t *testing.T) {
	const budget = 206621
	for _, guarded := range []bool{false, true} {
		t.Run(fmt.Sprintf("guard=%v", guarded), func(t *testing.T) {
			workspace := t.TempDir()
			l := newSpillLoop(t, t.TempDir(), 20000)
			counter := quarterCounter{}
			huge := strings.Repeat("a", 5_893_076)
			llmCalls := 0
			deps := pipeline.PipelineDeps{
				TokenCounter: counter,
				Config:       pipeline.PipelineConfig{MaxIterations: 6, CheckpointInterval: 5, ContextWindow: budget},
				PruneMessages: func(msgs []providers.Message, _ int) ([]providers.Message, pipeline.PruneStats) {
					return msgs, pipeline.PruneStats{}
				},
				CompactMessages: func(context.Context, []providers.Message, string) ([]providers.Message, error) {
					return []providers.Message{{Role: "user", Content: "[compacted summary]"}}, nil
				},
				OverBudgetMessage: func(context.Context) string { return "context too long" },
				CallLLM: func(_ context.Context, _ *pipeline.RunState, req providers.ChatRequest) (*providers.ChatResponse, error) {
					llmCalls++
					if counter.CountMessages("", req.Messages) > budget {
						return nil, fmt.Errorf("context length exceeded for this request")
					}
					if llmCalls == 1 {
						return &providers.ChatResponse{FinishReason: "tool_calls", ToolCalls: []providers.ToolCall{{ID: "tc-1", Name: "mcp_gitlab___search_project_code"}}}, nil
					}
					return &providers.ChatResponse{Content: "done", FinishReason: "stop"}, nil
				},
				ExecuteToolCall: func(_ context.Context, _ *pipeline.RunState, tc providers.ToolCall) ([]providers.Message, error) {
					return []providers.Message{toolMsg(huge)}, nil
				},
			}
			if guarded {
				deps.BoundToolResult = l.makeBoundToolResult(counter)
			}
			d := &deps
			p := pipeline.NewPipeline(nil, []pipeline.Stage{
				pipeline.NewThinkStage(d), pipeline.NewPruneStage(d, nil), pipeline.NewToolStage(d),
				pipeline.NewObserveStage(d), pipeline.NewCheckpointStage(d),
			}, nil, deps)
			state := spillState("prod-case")
			state.Ctx = spillCtx(workspace, uuid.New())
			state.Messages.SetHistory([]providers.Message{{Role: "user", Content: "find the code"}})

			if _, err := p.Run(state.Ctx, state); err != nil {
				t.Fatalf("Run() error: %v", err)
			}
			if guarded {
				if state.ExitCode != pipeline.BreakLoop || state.Observe.FinalContent != "done" {
					t.Errorf("guarded run: exit=%v final=%q, want a final answer", state.ExitCode, state.Observe.FinalContent)
				}
				files, _ := filepath.Glob(filepath.Join(workspace, ".tool-results", "session-*", "*.txt"))
				if len(files) != 1 {
					t.Errorf("spill files = %v, want one", files)
				}
			} else if state.ExitCode != pipeline.AbortRun {
				t.Errorf("unguarded run: exit=%v, want AbortRun (documents the failure)", state.ExitCode)
			}
		})
	}
}
