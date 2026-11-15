package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
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

func spillFile(t *testing.T, s *ToolResultSpillStore, workspace string, tenant uuid.UUID, key string) string {
	t.Helper()
	res, err := s.Save(workspace, tenant, key, "exec", "data of "+key)
	if err != nil {
		t.Fatalf("Save(%s): %v", key, err)
	}
	return filepath.Join(workspace, filepath.FromSlash(res.RelPath))
}

func TestSpillStore_DeleteSessionRemovesDirAndIndex(t *testing.T) {
	workspace, dataDir := t.TempDir(), t.TempDir()
	s := NewToolResultSpillStore(dataDir)
	tenantA, tenantB := uuid.New(), uuid.New()

	a1 := spillFile(t, s, workspace, tenantA, "k")
	a2 := spillFile(t, s, workspace, tenantA, "k")
	b := spillFile(t, s, workspace, tenantB, "k")

	ctx := store.WithTenantID(context.Background(), tenantA)
	if got := s.DeleteSession(ctx, "k"); got != 1 {
		t.Errorf("DeleteSession() = %d directories, want 1", got)
	}
	for _, f := range []string{a1, a2} {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("%s still exists (err=%v)", f, err)
		}
	}
	if _, err := os.Stat(s.indexPath(ToolResultSessionHash(tenantA, "k"))); !os.IsNotExist(err) {
		t.Errorf("index file still exists (err=%v)", err)
	}
	if _, err := os.Stat(b); err != nil {
		t.Errorf("the same key in another tenant lost its file: %v", err)
	}
	if got := s.DeleteSession(ctx, "k"); got != 0 {
		t.Errorf("second DeleteSession() = %d, want 0", got)
	}
}

func TestSpillStore_DeleteSessionIgnoresForeignIndexEntries(t *testing.T) {
	workspace, dataDir := t.TempDir(), t.TempDir()
	s := NewToolResultSpillStore(dataDir)
	tenant := uuid.New()
	hash := ToolResultSessionHash(tenant, "k")
	victim := filepath.Join(t.TempDir(), "important")
	if err := os.MkdirAll(victim, 0o700); err != nil {
		t.Fatal(err)
	}
	// A tampered index lists a directory that is not a session spill directory.
	if err := os.MkdirAll(filepath.Join(dataDir, toolResultIndexDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.indexPath(hash), []byte(victim+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	spillFile(t, s, workspace, tenant, "k")

	s.DeleteSession(store.WithTenantID(context.Background(), tenant), "k")
	if _, err := os.Stat(victim); err != nil {
		t.Errorf("a foreign directory from the index was removed: %v", err)
	}
}

func TestSpillStore_SweepRemovesOnlyOldFiles(t *testing.T) {
	workspace, dataDir := t.TempDir(), t.TempDir()
	s := NewToolResultSpillStore(dataDir)
	tenant := uuid.New()
	now := time.Now()
	age := func(path string, d time.Duration) {
		t.Helper()
		if err := os.Chtimes(path, now.Add(-d), now.Add(-d)); err != nil {
			t.Fatal(err)
		}
	}
	const day = 24 * time.Hour

	// Session "mixed": one old file and one recent file. Session "old": only old files.
	oldMixed := spillFile(t, s, workspace, tenant, "mixed")
	newMixed := spillFile(t, s, workspace, tenant, "mixed")
	old1 := spillFile(t, s, workspace, tenant, "old")
	old2 := spillFile(t, s, workspace, tenant, "old")
	age(oldMixed, 8*day)
	age(newMixed, 6*day)
	age(old1, 9*day)
	age(old2, 8*day)

	if st := s.Sweep(0, now); st.FilesRemoved != 0 || st.DirsRemoved != 0 {
		t.Errorf("Sweep(0) removed %+v, want nothing", st)
	}
	st := s.Sweep(7*day, now)
	if st.FilesRemoved != 3 || st.DirsRemoved != 1 {
		t.Errorf("Sweep(7d) = %+v, want 3 files and 1 directory removed", st)
	}
	if _, err := os.Stat(newMixed); err != nil {
		t.Errorf("6-day-old file was removed: %v", err)
	}
	for _, f := range []string{oldMixed, old1, old2} {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("%s older than 7 days still exists", f)
		}
	}
	if _, err := os.Stat(filepath.Dir(old1)); !os.IsNotExist(err) {
		t.Error("empty session directory was not removed")
	}
	if _, err := os.Stat(s.indexPath(ToolResultSessionHash(tenant, "old"))); !os.IsNotExist(err) {
		t.Error("index entry of the removed session directory is still there")
	}
	if lines, _ := readIndexLines(s.indexPath(ToolResultSessionHash(tenant, "mixed"))); len(lines) != 1 {
		t.Errorf("index of the kept session = %v, want one entry", lines)
	}

	// A new save in a swept session records its directory again.
	again := spillFile(t, s, workspace, tenant, "old")
	if _, err := os.Stat(again); err != nil {
		t.Fatal(err)
	}
	if lines, _ := readIndexLines(s.indexPath(ToolResultSessionHash(tenant, "old"))); len(lines) != 1 {
		t.Errorf("index after a new save = %v, want one entry", lines)
	}
}
