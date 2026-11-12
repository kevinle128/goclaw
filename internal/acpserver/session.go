package acpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
)

var ErrPromptActive = errors.New("ACP session already has an active prompt")

type sessionRecord struct {
	ID         string
	SessionKey string
	CWD        string

	mu         sync.Mutex
	generation uint64
	active     *activePrompt
	lastActive time.Time
	mcp        *SessionMCPManager
	tools      []acpbridge.ToolSchema
	// servers holds the checked MCP server definitions from session/new. An
	// idle session stops its MCP processes; the next prompt starts them again
	// from these definitions.
	servers []MCPServer
	// removed is set when the registry drops the record. A prompt that found
	// the record just before then must not start on it.
	removed bool
	// thread is the _meta["buzz.thread"] value from session/new, or nil.
	thread json.RawMessage
}

type activePrompt struct {
	generation uint64
	runID      string
	cancel     context.CancelFunc
	// thread is the _meta["buzz.thread"] value of this turn: the prompt value
	// when the prompt has one, else the session value.
	thread json.RawMessage
}

// PromptContext identifies the client context of one active prompt.
type PromptContext struct {
	SessionID  string
	SessionKey string
	// Thread is the compact JSON thread object of the turn, or nil.
	Thread json.RawMessage
}

type SessionRegistry struct {
	mu        sync.RWMutex
	sessions  map[string]*sessionRecord
	now       func() time.Time
	limit     int
	maxPrompt time.Duration
}

// NewSessionRegistry creates a registry. A prompt is cancelled after
// maxPromptDuration; zero or a negative value selects MaxPromptDurationDefault.
func NewSessionRegistry(maxPromptDuration time.Duration) *SessionRegistry {
	if maxPromptDuration <= 0 {
		maxPromptDuration = MaxPromptDurationDefault
	}
	return &SessionRegistry{sessions: make(map[string]*sessionRecord), now: time.Now, limit: MaxActiveSessions, maxPrompt: maxPromptDuration}
}

// Add stores a new session. When the registry is full, it removes the idle
// session that was active least recently and returns its id as evicted. A
// session with an active prompt is never removed.
func (r *SessionRegistry) Add(record *sessionRecord) (evicted string, err error) {
	r.mu.Lock()
	if _, exists := r.sessions[record.ID]; exists {
		r.mu.Unlock()
		return "", errors.New("ACP session already exists")
	}
	if len(r.sessions) >= r.limit {
		victim := r.evictOldestIdleLocked()
		if victim == "" {
			r.mu.Unlock()
			return "", errors.New("ACP session limit reached")
		}
		evicted = victim
	}
	record.lastActive = r.now()
	r.sessions[record.ID] = record
	r.mu.Unlock()
	return evicted, nil
}

// evictOldestIdleLocked removes the idle session with the oldest activity and
// returns its id, or "" when every session has an active prompt. The caller
// holds r.mu. A candidate is checked again under its own lock, because a
// prompt can start on it after the first look.
func (r *SessionRegistry) evictOldestIdleLocked() string {
	skipped := make(map[string]struct{})
	for {
		var oldest *sessionRecord
		var oldestAt time.Time
		for id, candidate := range r.sessions {
			if _, skip := skipped[id]; skip {
				continue
			}
			candidate.mu.Lock()
			idle, at := candidate.active == nil, candidate.lastActive
			candidate.mu.Unlock()
			if idle && (oldest == nil || at.Before(oldestAt)) {
				oldest, oldestAt = candidate, at
			}
		}
		if oldest == nil {
			return ""
		}
		oldest.mu.Lock()
		if oldest.active != nil {
			oldest.mu.Unlock()
			skipped[oldest.ID] = struct{}{}
			continue
		}
		oldest.removed = true
		closeSessionMCP(oldest)
		oldest.mu.Unlock()
		delete(r.sessions, oldest.ID)
		return oldest.ID
	}
}

func (r *SessionRegistry) Get(id string) (*sessionRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.sessions[id]
	return record, ok
}

// BeginPrompt starts a prompt. A non-nil thread replaces the session thread
// for this turn only.
func (r *SessionRegistry) BeginPrompt(id string, parent context.Context, thread json.RawMessage) (*sessionRecord, uint64, context.Context, error) {
	record, ok := r.Get(id)
	if !ok {
		return nil, 0, nil, &RPCError{Code: CodeUnknownSession, Message: "Unknown session"}
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.removed {
		return nil, 0, nil, &RPCError{Code: CodeUnknownSession, Message: "Unknown session"}
	}
	if record.active != nil {
		return nil, 0, nil, ErrPromptActive
	}
	record.generation++
	turnThread := record.thread
	if thread != nil {
		turnThread = bytes.Clone(thread)
	}
	ctx, cancel := context.WithTimeout(parent, r.maxPrompt)
	record.active = &activePrompt{generation: record.generation, cancel: cancel, thread: turnThread}
	record.lastActive = r.now()
	return record, record.generation, ctx, nil
}

// PromptContext returns the client context of the active prompt with this
// generation. The Thread value is a copy.
func (r *SessionRegistry) PromptContext(id string, generation uint64) (PromptContext, bool) {
	record, ok := r.Get(id)
	if !ok {
		return PromptContext{}, false
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.active == nil || record.active.generation != generation {
		return PromptContext{}, false
	}
	return PromptContext{SessionID: record.ID, SessionKey: record.SessionKey, Thread: bytes.Clone(record.active.thread)}, true
}

func (r *SessionRegistry) SetRunID(id string, generation uint64, runID string) bool {
	record, ok := r.Get(id)
	if !ok {
		return false
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.active == nil || record.active.generation != generation {
		return false
	}
	record.active.runID = runID
	return true
}

func (r *SessionRegistry) FinishPrompt(id string, generation uint64) bool {
	record, ok := r.Get(id)
	if !ok {
		return false
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.active == nil || record.active.generation != generation {
		return false
	}
	record.active.cancel()
	record.active = nil
	record.lastActive = r.now()
	return true
}

func (r *SessionRegistry) Cancel(id string) (sessionKey, runID string, ok bool) {
	record, exists := r.Get(id)
	if !exists {
		return "", "", false
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.active == nil {
		return record.SessionKey, "", true
	}
	record.active.cancel()
	return record.SessionKey, record.active.runID, true
}

func (r *SessionRegistry) Release(id string) {
	r.mu.Lock()
	record := r.sessions[id]
	delete(r.sessions, id)
	r.mu.Unlock()
	if record == nil {
		return
	}
	record.mu.Lock()
	record.removed = true
	if record.active != nil {
		record.active.cancel()
		record.active = nil
	}
	closeSessionMCP(record)
	record.mu.Unlock()
}

func (r *SessionRegistry) Close() {
	r.mu.Lock()
	records := r.sessions
	r.sessions = make(map[string]*sessionRecord)
	r.mu.Unlock()
	for _, record := range records {
		record.mu.Lock()
		record.removed = true
		if record.active != nil {
			record.active.cancel()
			record.active = nil
		}
		closeSessionMCP(record)
		record.mu.Unlock()
	}
}

func (r *SessionRegistry) Len() int { r.mu.RLock(); defer r.mu.RUnlock(); return len(r.sessions) }

// ReapIdle stops the MCP processes of sessions that have been idle for ttl.
// The sessions stay registered: a client can wake a thread session hours
// later, and its next prompt starts the MCP processes again. It returns the
// number of sessions whose MCP processes were stopped.
func (r *SessionRegistry) ReapIdle(now time.Time, ttl time.Duration) int {
	r.mu.RLock()
	records := make([]*sessionRecord, 0, len(r.sessions))
	for _, record := range r.sessions {
		records = append(records, record)
	}
	r.mu.RUnlock()
	stopped := 0
	for _, record := range records {
		record.mu.Lock()
		if record.active == nil && record.mcp != nil && now.Sub(record.lastActive) >= ttl {
			closeSessionMCP(record)
			stopped++
		}
		record.mu.Unlock()
	}
	return stopped
}

// resumeMCP sets the MCP manager that a prompt started again for a session
// whose MCP processes were stopped. It refuses when the prompt with this
// generation is no longer active, so a closed session gets no new processes.
func (r *SessionRegistry) resumeMCP(id string, generation uint64, manager *SessionMCPManager, tools []acpbridge.ToolSchema) bool {
	record, ok := r.Get(id)
	if !ok {
		return false
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.active == nil || record.active.generation != generation || record.mcp != nil {
		return false
	}
	record.mcp, record.tools = manager, tools
	return true
}

// mcpManager returns the session's MCP manager, or nil when its MCP
// processes are stopped.
func (record *sessionRecord) mcpManager() *SessionMCPManager {
	record.mu.Lock()
	defer record.mu.Unlock()
	return record.mcp
}

func closeSessionMCP(record *sessionRecord) {
	if record.mcp != nil {
		if err := record.mcp.Close(); err != nil {
			slog.Warn("close ACP MCP session", "error", err)
		}
		record.mcp = nil
	}
}
