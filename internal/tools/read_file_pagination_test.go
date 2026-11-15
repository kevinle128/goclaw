package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func readFileResult(t *testing.T, content string, args map[string]any) string {
	t.Helper()
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "data.txt"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	args["path"] = "data.txt"
	res := NewReadFileTool(workspace, true).Execute(WithToolWorkspace(context.Background(), workspace), args)
	if res.IsError {
		t.Fatalf("read_file failed: %s", res.ForLLM)
	}
	return res.ForLLM
}

func TestReadFile_SingleLineOverCapIsTruncated(t *testing.T) {
	t.Parallel()
	line := strings.Repeat("é", 120_000) // multibyte, so the cap counts runes not bytes
	got := readFileResult(t, line, map[string]any{})

	wantNote := "[Line 0 is 120000 chars; showed the first 50000. Use exec with a byte range (for example: cut -c 50001-100000) to read more.]"
	if !strings.HasSuffix(got, wantNote) {
		t.Errorf("result does not end with the note %q; tail: %q", wantNote, got[max(0, len(got)-200):])
	}
	body := strings.TrimSuffix(got, "\n\n"+wantNote)
	if n := utf8.RuneCountInString(body); n != 50_000 {
		t.Errorf("body has %d runes, want 50000", n)
	}
}

func TestReadFile_LongFirstLineWithMoreLinesPointsToNextOffset(t *testing.T) {
	t.Parallel()
	content := strings.Repeat("x", 80_000) + "\nsecond\nthird"
	got := readFileResult(t, content, map[string]any{})
	if !strings.Contains(got, "Use offset=1 to continue with the next line.]") {
		t.Errorf("note lacks the next-line hint: %q", got[len(got)-250:])
	}
	if strings.Contains(got, "second") {
		t.Error("later lines were returned after a truncated first line")
	}
}

func TestReadFile_LineThatFitsIsNotTruncated(t *testing.T) {
	t.Parallel()
	// 49,999 runes plus the newline reach the cap exactly; the line must come back whole.
	content := strings.Repeat("y", 49_999) + "\n" + strings.Repeat("z", 10)
	got := readFileResult(t, content, map[string]any{})
	if !strings.HasPrefix(got, strings.Repeat("y", 49_999)) || strings.Contains(got, "Line 0 is") {
		t.Errorf("a first line within the cap was truncated: %q", got[:60])
	}

	exact := strings.Repeat("y", 50_000) + "\nnext"
	got = readFileResult(t, exact, map[string]any{})
	if strings.Contains(got, "Line 0 is") || !strings.HasPrefix(got, strings.Repeat("y", 50_000)) {
		t.Errorf("a first line of exactly the cap was truncated")
	}
}

func TestReadFile_WrappedLinesPageUnderTheCap(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	for range 300 {
		b.WriteString(strings.Repeat("w", 1999) + "\n")
	}
	got := readFileResult(t, b.String(), map[string]any{"offset": float64(0), "limit": float64(40)})
	if n := utf8.RuneCountInString(got); n > 50_000+300 {
		t.Errorf("page has %d chars, want it capped near 50000", n)
	}
	if !strings.Contains(got, "Use offset=") {
		t.Error("capped page lacks the continue hint")
	}
}
