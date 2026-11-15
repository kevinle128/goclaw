package tools

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSpillStore_ExclusiveCreateRefusesPlantedSymlink(t *testing.T) {
	workspace, dataDir, outside := t.TempDir(), t.TempDir(), t.TempDir()
	s := NewToolResultSpillStore(dataDir)
	tenant := uuid.New()
	s.randName = func() string { return "aaaaaaaaaaaa" }

	// The first save creates the session dir. Then a symlink to a file outside
	// the workspace takes the place of the name the next save will try.
	if _, err := s.Save(workspace, tenant, "sess", "exec", "first"); err != nil {
		t.Fatalf("first Save() error: %v", err)
	}
	sessionDir := filepath.Join(workspace, ToolResultSpillDirName, "session-"+ToolResultSessionHash(tenant, "sess"))
	victim := filepath.Join(outside, "victim.txt")
	planted := filepath.Join(sessionDir, "aaaaaaaaaaaa-exec.txt")
	if err := os.Remove(planted); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, planted); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Save(workspace, tenant, "sess", "exec", "second"); err == nil {
		t.Fatal("Save() through a planted symlink succeeded, want an error")
	}
	if _, err := os.Stat(victim); !os.IsNotExist(err) {
		t.Errorf("the symlink target was written (stat err=%v)", err)
	}
}

func TestSpillStore_RefusesSymlinkedDirectories(t *testing.T) {
	tenant := uuid.New()
	hash := ToolResultSessionHash(tenant, "sess")

	t.Run("session dir links outside", func(t *testing.T) {
		workspace, dataDir, outside := t.TempDir(), t.TempDir(), t.TempDir()
		root := filepath.Join(workspace, ToolResultSpillDirName)
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "session-"+hash)); err != nil {
			t.Fatal(err)
		}
		if _, err := NewToolResultSpillStore(dataDir).Save(workspace, tenant, "sess", "exec", "data"); err == nil {
			t.Fatal("Save() into a symlinked session dir succeeded")
		}
		if entries, _ := os.ReadDir(outside); len(entries) != 0 {
			t.Errorf("files were written outside the workspace: %v", entries)
		}
	})

	t.Run("spill root links outside", func(t *testing.T) {
		workspace, dataDir, outside := t.TempDir(), t.TempDir(), t.TempDir()
		if err := os.Symlink(outside, filepath.Join(workspace, ToolResultSpillDirName)); err != nil {
			t.Fatal(err)
		}
		if _, err := NewToolResultSpillStore(dataDir).Save(workspace, tenant, "sess", "exec", "data"); err == nil {
			t.Fatal("Save() under a symlinked spill root succeeded")
		}
		if entries, _ := os.ReadDir(outside); len(entries) != 0 {
			t.Errorf("files were written outside the workspace: %v", entries)
		}
	})
}

func TestSpillStore_SameSessionKeyDiffersByTenant(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	if ToolResultSessionHash(a, "k") == ToolResultSessionHash(b, "k") {
		t.Error("two tenants share a session hash")
	}
	// A missing tenant counts as the master tenant, as in the session stores.
	if ToolResultSessionHash(uuid.Nil, "k") == ToolResultSessionHash(a, "k") {
		t.Error("nil tenant hashes like a real tenant")
	}
}

func TestSpillStore_IndexRecordsEachDirectoryOnce(t *testing.T) {
	workspace, dataDir := t.TempDir(), t.TempDir()
	s := NewToolResultSpillStore(dataDir)
	tenant := uuid.New()
	for range 3 {
		if _, err := s.Save(workspace, tenant, "sess", "exec", "data"); err != nil {
			t.Fatalf("Save() error: %v", err)
		}
	}
	lines, err := readIndexLines(s.indexPath(ToolResultSessionHash(tenant, "sess")))
	if err != nil || len(lines) != 1 {
		t.Fatalf("index lines = %v err=%v, want one directory", lines, err)
	}
	if !strings.HasSuffix(lines[0], "/.tool-results/session-"+ToolResultSessionHash(tenant, "sess")) {
		t.Errorf("index entry = %q", lines[0])
	}
}

func TestWrapLongLines_KeepsShortContentAndInsertsOnlyNewlines(t *testing.T) {
	short := "a\nb\n" + strings.Repeat("c", 100)
	if got, wrapped := wrapLongLines(short, 2000); got != short || wrapped {
		t.Error("short content was changed")
	}
	long := strings.Repeat("é", 4500) + "\nshort\n" + strings.Repeat("x", 2000)
	got, wrapped := wrapLongLines(long, 2000)
	if !wrapped {
		t.Fatal("long line was not wrapped")
	}
	if strings.ReplaceAll(got, "\n", "") != strings.ReplaceAll(long, "\n", "") {
		t.Error("wrapping changed characters")
	}
	if want := 3 + 1 + 1; len(strings.Split(got, "\n")) != want {
		t.Errorf("lines = %d, want %d (4500 runes -> 3 lines; 2000 runes stays 1 line)", len(strings.Split(got, "\n")), want)
	}
}
