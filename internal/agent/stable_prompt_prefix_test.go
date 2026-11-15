package agent

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

func buildLayoutMessages(t *testing.T, l *Loop, extra, title, sender, user string) []providers.Message {
	t.Helper()
	msgs, _ := l.buildMessages(context.Background(), nil, "", user, extra,
		"sess-1", "ws", "", "", title, "chat-1", "direct", "user-1", sender, 0, nil, false)
	return msgs
}

func TestBuildMessages_StableLayout_MovesDynamicSectionsToUserMessage(t *testing.T) {
	off := newPromptLayoutLoop("read_file")
	on := newPromptLayoutLoop("read_file")
	on.stablePromptPrefix = true

	offMsgs := buildLayoutMessages(t, off, "extra instructions", "Title", "Alice", "question")
	onMsgs := buildLayoutMessages(t, on, "extra instructions", "Title", "Alice", "question")

	// The stable part is the default text above the boundary minus the
	// chat-specific identity lines, which move below it.
	stable, _, _ := strings.Cut(offMsgs[0].Content, CacheBoundaryMarker)
	_, stable, _ = strings.Cut(stable, "## Tooling")
	want := "## Tooling" + strings.TrimRight(stable, "\n")
	if got := onMsgs[0].Content; got != want {
		t.Fatalf("system must equal the stable part of the default prompt\n got: %q\nwant: %q", got, want)
	}
	if strings.Contains(onMsgs[0].Content, CacheBoundaryMarker) {
		t.Fatal("stable system message must not carry the boundary marker")
	}
	last := onMsgs[len(onMsgs)-1]
	if last.Role != "user" || !strings.HasPrefix(last.Content, "<runtime_context>\n") {
		t.Fatalf("current-turn user message must start with the runtime block: %q", last.Content)
	}
	for _, want := range []string{"extra instructions", "Current date:", "## Runtime", "chat_id: chat-1", "running in ws"} {
		if !strings.Contains(last.Content, want) {
			t.Errorf("runtime block missing %q", want)
		}
	}
	if !strings.HasSuffix(last.Content, "</runtime_context>\n\nquestion") {
		t.Fatalf("user text must follow the block: %q", last.Content)
	}
	if len(onMsgs) != len(offMsgs) {
		t.Fatalf("message count changed: %d vs %d", len(onMsgs), len(offMsgs))
	}
}

func TestBuildMessages_StableLayout_SystemIdenticalAcrossTurns(t *testing.T) {
	for _, stable := range []bool{true, false} {
		l := newPromptLayoutLoop("read_file")
		l.stablePromptPrefix = stable
		a := buildLayoutMessages(t, l, "extra one", "Title A", "Alice", "first question")
		b := buildLayoutMessages(t, l, "extra two", "Title B", "Bob", "second question")
		same := a[0].Content == b[0].Content
		if stable && !same {
			t.Error("flag on: system messages must be byte-equal across turns with different dynamic content")
		}
		if !stable && same {
			t.Error("flag off: system messages are expected to differ across turns")
		}
	}
}

func TestBuildMessages_StableLayout_SystemIdenticalAcrossChats(t *testing.T) {
	l := newPromptLayoutLoop("read_file")
	l.stablePromptPrefix = true
	build := func(chatID string) string {
		msgs, _ := l.buildMessages(context.Background(), nil, "", "q", "",
			"sess", "ws", "", "", "", chatID, "direct", "user-1", "Alice", 0, nil, false)
		return msgs[0].Content
	}
	if build("chat-1") != build("chat-2") {
		t.Fatal("flag on: system message must not depend on the chat id")
	}
}

func layoutRunState(l *Loop) *pipeline.RunState {
	return pipeline.NewRunState(&pipeline.RunInput{}, nil, "m", l.provider)
}

func toolNamesOf(defs []providers.ToolDefinition) []string {
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		out = append(out, toolDefKey(d))
	}
	return out
}

func TestToolOrder_StableAppendsActivatedTool(t *testing.T) {
	l := newPromptLayoutLoop("alpha", "charlie")
	l.stablePromptPrefix = true
	l.maxIterations = 10
	build := l.makeBuildFilteredTools(&RunRequest{})
	state := layoutRunState(l)

	first, err := build(state)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolNamesOf(first); !slices.Equal(got, []string{"alpha", "charlie"}) {
		t.Fatalf("first build = %v", got)
	}

	// A deferred tool activated mid-run sorts between the others.
	l.registry.Register(layoutTestTool{name: "bravo"})
	state.Iteration = 2
	second, _ := build(state)
	if got := toolNamesOf(second); !slices.Equal(got, []string{"alpha", "charlie", "bravo"}) {
		t.Fatalf("activated tool must be appended: %v", got)
	}

	// Final iteration strips tools; the order must survive for later builds.
	state.Iteration = 10
	if stripped, _ := build(state); len(stripped) != 0 {
		t.Fatalf("final iteration must strip tools: %v", toolNamesOf(stripped))
	}
	state.Iteration = 3
	third, _ := build(state)
	if got := toolNamesOf(third); !slices.Equal(got, []string{"alpha", "charlie", "bravo"}) {
		t.Fatalf("order lost after final-iteration strip: %v", got)
	}

	// A new run starts from a fresh order.
	fresh, _ := build(layoutRunState(l))
	if got := toolNamesOf(fresh); !slices.Equal(got, []string{"alpha", "bravo", "charlie"}) {
		t.Fatalf("new run must start sorted: %v", got)
	}
}

func TestToolOrder_DefaultStillResorts(t *testing.T) {
	l := newPromptLayoutLoop("alpha", "charlie")
	l.maxIterations = 10
	build := l.makeBuildFilteredTools(&RunRequest{})
	state := layoutRunState(l)
	build(state)
	l.registry.Register(layoutTestTool{name: "bravo"})
	state.Iteration = 2
	second, _ := build(state)
	if got := toolNamesOf(second); !slices.Equal(got, []string{"alpha", "bravo", "charlie"}) {
		t.Fatalf("flag off must keep sorted order: %v", got)
	}
}

func TestToolOrder_StableRemovedToolKeepsOthersInPlace(t *testing.T) {
	l := newPromptLayoutLoop("alpha", "bravo", "charlie")
	l.stablePromptPrefix = true
	l.maxIterations = 10
	build := l.makeBuildFilteredTools(&RunRequest{})
	state := layoutRunState(l)
	build(state)
	l.registry.Disable("bravo")
	state.Iteration = 1
	got, _ := build(state)
	if names := toolNamesOf(got); !slices.Equal(names, []string{"alpha", "charlie"}) {
		t.Fatalf("got %v", names)
	}
	l.registry.Enable("bravo")
	state.Iteration = 2
	got, _ = build(state)
	if names := toolNamesOf(got); !slices.Equal(names, []string{"alpha", "bravo", "charlie"}) {
		t.Fatalf("returning tool must take its old place: %v", names)
	}
}
