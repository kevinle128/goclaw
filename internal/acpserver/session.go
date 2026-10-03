package acpserver

import (
	"context"
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
}

type activePrompt struct {
	generation uint64
	runID      string
	cancel     context.CancelFunc
}

type SessionRegistry struct {
	mu       sync.RWMutex
	sessions map[string]*sessionRecord
	now      func() time.Time
	limit    int
}

func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{sessions: make(map[string]*sessionRecord), now: time.Now, limit: MaxActiveSessions}
}

func (r *SessionRegistry) Add(record *sessionRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sessions) >= r.limit {
		return errors.New("ACP session limit reached")
	}
	if _, exists := r.sessions[record.ID]; exists {
		return errors.New("ACP session already exists")
	}
	record.lastActive = r.now()
	r.sessions[record.ID] = record
	return nil
}

func (r *SessionRegistry) Get(id string) (*sessionRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.sessions[id]
	return record, ok
}

func (r *SessionRegistry) BeginPrompt(id string, parent context.Context) (*sessionRecord, uint64, context.Context, error) {
	record, ok := r.Get(id)
	if !ok {
		return nil, 0, nil, &RPCError{Code: CodeUnknownSession, Message: "Unknown session"}
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.active != nil {
		return nil, 0, nil, ErrPromptActive
	}
	record.generation++
	ctx, cancel := context.WithTimeout(parent, 2*time.Hour)
	record.active = &activePrompt{generation: record.generation, cancel: cancel}
	record.lastActive = r.now()
	return record, record.generation, ctx, nil
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
		if record.active != nil {
			record.active.cancel()
			record.active = nil
		}
		closeSessionMCP(record)
		record.mu.Unlock()
	}
}

func (r *SessionRegistry) Len() int { r.mu.RLock(); defer r.mu.RUnlock(); return len(r.sessions) }

func (r *SessionRegistry) ReapIdle(now time.Time, ttl time.Duration) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	removed := 0
	for id, record := range r.sessions {
		record.mu.Lock()
		idle := record.active == nil && now.Sub(record.lastActive) >= ttl
		record.mu.Unlock()
		if idle {
			delete(r.sessions, id)
			closeSessionMCP(record)
			removed++
		}
	}
	return removed
}

func closeSessionMCP(record *sessionRecord) {
	if record.mcp != nil {
		if err := record.mcp.Close(); err != nil {
			slog.Warn("close ACP MCP session", "error", err)
		}
		record.mcp = nil
	}
}
