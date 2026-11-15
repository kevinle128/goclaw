package tools

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

const (
	// ToolResultSpillDirName is the hidden directory, inside the tool workspace,
	// that holds spilled tool results. A hidden name keeps the vault walker out.
	ToolResultSpillDirName = ".tool-results"

	// toolResultIndexDirName lives under the data directory (which exec cannot
	// read) and maps a session hash to the spill directories it owns, so that
	// session delete and the age sweep do not have to walk every workspace.
	toolResultIndexDirName = "tool-results-index"

	// spillWrapColumn is the longest line a spill file keeps. read_file pages by
	// line, so a one-line result would otherwise come back as one huge page.
	spillWrapColumn = 2000

	spillSessionDirPrefix = "session-"
	spillMaxToolNameLen   = 40
	spillCreateAttempts   = 3
)

// ToolResultSpillStore writes oversized tool results to session-owned files in
// the tool workspace. It holds only the data directory; every path it touches is
// derived from the tenant, session and workspace of the call, so one instance is
// shared by all agents.
type ToolResultSpillStore struct {
	dataDir string

	// indexMu serializes index appends so two saves of one session record the
	// spill directory once.
	indexMu sync.Mutex

	// randName returns the random part of a spill file name. Tests replace it
	// to aim at a planted path.
	randName func() string
}

// NewToolResultSpillStore returns a store whose ownership index lives under
// dataDir. An empty dataDir is an error at Save time, not here, so a gateway
// without a data directory still starts.
func NewToolResultSpillStore(dataDir string) *ToolResultSpillStore {
	return &ToolResultSpillStore{dataDir: dataDir, randName: randomSpillName}
}

// SpillResult describes a saved spill file.
type SpillResult struct {
	// RelPath is the file path relative to the tool workspace, with forward slashes.
	RelPath string
	// Lines is the line count of the saved file.
	Lines int
	// Wrapped reports that lines longer than the wrap column were split.
	Wrapped bool
}

// ToolResultSessionHash returns the hex SHA-256 of the tenant and session key.
// A missing tenant counts as the master tenant, matching how the session
// stores key their rows, so save and delete agree on the directory name.
func ToolResultSessionHash(tenantID uuid.UUID, sessionKey string) string {
	if tenantID == uuid.Nil {
		tenantID = store.MasterTenantID
	}
	sum := sha256.Sum256([]byte(tenantID.String() + "\x00" + sessionKey))
	return hex.EncodeToString(sum[:])
}

// Save writes content to {workspace}/.tool-results/session-{hash}/{random}-{tool}.txt.
// Directories are 0700 and the file is created exclusively with mode 0600, so a
// planted symlink cannot redirect the write. Any failure is returned to the
// caller, which keeps the inline result.
func (s *ToolResultSpillStore) Save(workspace string, tenantID uuid.UUID, sessionKey, toolName, content string) (SpillResult, error) {
	if workspace == "" {
		return SpillResult{}, errors.New("no tool workspace")
	}
	if s.dataDir == "" {
		return SpillResult{}, errors.New("no data directory for the spill index")
	}
	workspace, err := filepath.Abs(workspace)
	if err != nil {
		return SpillResult{}, fmt.Errorf("resolve workspace: %w", err)
	}
	realWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return SpillResult{}, fmt.Errorf("resolve workspace: %w", err)
	}

	hash := ToolResultSessionHash(tenantID, sessionKey)
	rootDir := filepath.Join(workspace, ToolResultSpillDirName)
	sessionDir := filepath.Join(rootDir, spillSessionDirPrefix+hash)
	for _, dir := range []string{rootDir, sessionDir} {
		if err := ensureRealDir(dir); err != nil {
			return SpillResult{}, err
		}
	}
	realSession, err := filepath.EvalSymlinks(sessionDir)
	if err != nil {
		return SpillResult{}, fmt.Errorf("resolve spill dir: %w", err)
	}
	if !pathInside(realWorkspace, realSession) {
		return SpillResult{}, fmt.Errorf("spill dir %s resolves outside the workspace", sessionDir)
	}

	// The index is written before the file so a file never exists without an
	// owner record that delete and the sweep can find.
	if err := s.recordSpillDir(hash, sessionDir); err != nil {
		return SpillResult{}, err
	}

	text, wrapped := wrapLongLines(content, spillWrapColumn)
	name := sanitizeSpillToolName(toolName)
	var (
		path string
		f    *os.File
	)
	for range spillCreateAttempts {
		path = filepath.Join(sessionDir, s.randName()+"-"+name+".txt")
		f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil || !errors.Is(err, fs.ErrExist) {
			break
		}
	}
	if err != nil {
		return SpillResult{}, fmt.Errorf("create spill file: %w", err)
	}
	if _, err := f.WriteString(text); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return SpillResult{}, fmt.Errorf("write spill file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return SpillResult{}, fmt.Errorf("close spill file: %w", err)
	}

	rel, err := filepath.Rel(workspace, path)
	if err != nil {
		rel = path
	}
	return SpillResult{
		RelPath: filepath.ToSlash(rel),
		Lines:   strings.Count(text, "\n") + 1,
		Wrapped: wrapped,
	}, nil
}

// ensureRealDir creates dir with mode 0700 when absent and requires that it is
// a real directory. A symlink at dir is refused; MkdirAll alone would follow it.
func ensureRealDir(dir string) error {
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("create spill dir: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("inspect spill dir: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("spill dir %s is not a real directory", dir)
	}
	return nil
}

// pathInside reports whether child is root or lies below it.
func pathInside(root, child string) bool {
	rel, err := filepath.Rel(root, child)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

// indexPath returns the index file of a session hash.
func (s *ToolResultSpillStore) indexPath(hash string) string {
	return filepath.Join(s.dataDir, toolResultIndexDirName, hash)
}

// recordSpillDir appends sessionDir to the index file of hash once.
func (s *ToolResultSpillStore) recordSpillDir(hash, sessionDir string) error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	if err := os.MkdirAll(filepath.Join(s.dataDir, toolResultIndexDirName), 0o700); err != nil {
		return fmt.Errorf("create spill index dir: %w", err)
	}
	path := s.indexPath(hash)
	known, err := readIndexLines(path)
	if err != nil {
		return err
	}
	for _, d := range known {
		if d == sessionDir {
			return nil
		}
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open spill index: %w", err)
	}
	if _, err := f.WriteString(sessionDir + "\n"); err != nil {
		_ = f.Close()
		return fmt.Errorf("write spill index: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close spill index: %w", err)
	}
	return nil
}

// readIndexLines returns the non-empty lines of an index file. A missing file
// is an empty index.
func readIndexLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read spill index: %w", err)
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 4096), 1<<20)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			lines = append(lines, line)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read spill index: %w", err)
	}
	return lines, nil
}

// wrapLongLines splits every line longer than column runes into column-rune
// pieces joined by "\n". It only inserts newlines, so removing them from a
// wrapped line gives the original line back.
func wrapLongLines(content string, column int) (string, bool) {
	if len(content) <= column {
		return content, false
	}
	lines := strings.Split(content, "\n")
	wrapped := false
	var b strings.Builder
	b.Grow(len(content) + len(content)/column + 1)
	for i, line := range lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		if len(line) <= column { // bytes bound runes: no rune counting needed
			b.WriteString(line)
			continue
		}
		runes := []rune(line)
		if len(runes) <= column {
			b.WriteString(line)
			continue
		}
		wrapped = true
		for start := 0; start < len(runes); start += column {
			if start > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(string(runes[start:min(start+column, len(runes))]))
		}
	}
	if !wrapped {
		return content, false
	}
	return b.String(), true
}

// sanitizeSpillToolName reduces a tool name to [a-z0-9_-], at most
// spillMaxToolNameLen characters, so it is safe inside a file name.
func sanitizeSpillToolName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
		if b.Len() >= spillMaxToolNameLen {
			break
		}
	}
	if b.Len() == 0 {
		return "tool"
	}
	return b.String()
}

// randomSpillName returns 12 random hex characters.
func randomSpillName() string {
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand does not fail on supported platforms; a fixed name still
		// cannot overwrite anything because creation is exclusive.
		return "000000000000"
	}
	return hex.EncodeToString(buf[:])
}
