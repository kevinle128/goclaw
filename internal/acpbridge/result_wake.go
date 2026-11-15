package acpbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

const (
	// maxSessionOrigins bounds the remembered ACP sessions.
	maxSessionOrigins = 1024
	// sessionOriginTTL is how long an ACP session is remembered after its
	// last prompt.
	sessionOriginTTL = 24 * time.Hour
	// MaxWakeThreadBytes bounds the thread object that a wake echoes.
	MaxWakeThreadBytes = 4 << 10
	// MaxResultWakePromptBytes is the largest wake prompt of the ACP client.
	MaxResultWakePromptBytes = 2 << 10

	defaultResultBusyRetry = 35 * time.Second
	defaultResultBusyTries = 5
	resultWakeDeliverLimit = 90 * time.Second
)

// SessionOrigin describes the ACP client behind a session key: which
// connection identity runs it, and the thread of the client.
type SessionOrigin struct {
	TenantID     uuid.UUID
	AgentID      uuid.UUID
	UserID       string
	ACPSessionID string
	// Thread is the opaque thread object of the client, or empty.
	Thread json.RawMessage
	// Wake is true when the client accepts wake requests.
	Wake bool
}

type sessionOriginEntry struct {
	origin SessionOrigin
	seen   time.Time
}

// SessionOrigins remembers the ACP origin of each session key. A background
// run of the session, such as a sub-agent announce, has no ACP prompt of its
// own; this lets it find the thread of the client again.
type SessionOrigins struct {
	mu      sync.Mutex
	entries map[string]sessionOriginEntry
	now     func() time.Time
}

// NewSessionOrigins creates an empty registry.
func NewSessionOrigins() *SessionOrigins {
	return &SessionOrigins{entries: make(map[string]sessionOriginEntry), now: time.Now}
}

// Remember stores the origin of an ACP prompt for sessionKey.
func (s *SessionOrigins) Remember(sessionKey string, origin SessionOrigin) {
	if s == nil || sessionKey == "" {
		return
	}
	origin.Thread = bytes.Clone(origin.Thread)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if _, exists := s.entries[sessionKey]; !exists && len(s.entries) >= maxSessionOrigins {
		s.evictLocked(now)
	}
	s.entries[sessionKey] = sessionOriginEntry{origin: origin, seen: now}
}

// evictLocked drops the expired entries, and the oldest one when none expired.
func (s *SessionOrigins) evictLocked(now time.Time) {
	var oldestKey string
	var oldest time.Time
	for key, e := range s.entries {
		if now.Sub(e.seen) > sessionOriginTTL {
			delete(s.entries, key)
			continue
		}
		if oldestKey == "" || e.seen.Before(oldest) {
			oldestKey, oldest = key, e.seen
		}
	}
	if len(s.entries) >= maxSessionOrigins && oldestKey != "" {
		delete(s.entries, oldestKey)
	}
}

// Lookup returns the origin of sessionKey, or false when the session is not
// an ACP session or was not seen for a day.
func (s *SessionOrigins) Lookup(sessionKey string) (SessionOrigin, bool) {
	if s == nil {
		return SessionOrigin{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[sessionKey]
	if !ok {
		return SessionOrigin{}, false
	}
	if s.now().Sub(e.seen) > sessionOriginTTL {
		delete(s.entries, sessionKey)
		return SessionOrigin{}, false
	}
	origin := e.origin
	origin.Thread = bytes.Clone(origin.Thread)
	return origin, true
}

// WakeSender sends one wake to a live ACP adapter connection. WakeSinks
// implements it.
type WakeSender interface {
	DeliverWake(ctx context.Context, tenantID, agentID uuid.UUID, userID, wakeID string, params json.RawMessage) (outcome string, ok bool)
}

// ResultWaker wakes the thread of an ACP session with a text of goclaw, for
// example the result of a sub-agent. The wake starts a turn of the client with
// its host tools, so the agent can post the text itself.
type ResultWaker struct {
	Origins *SessionOrigins
	Sender  WakeSender
	// BusyRetry and BusyTries bound the retries after a busy answer. The
	// client allows one wake per thread every 30 seconds.
	BusyRetry time.Duration
	BusyTries int
}

// NewResultWaker creates a waker with the default retry limits.
func NewResultWaker(origins *SessionOrigins, sender WakeSender) *ResultWaker {
	return &ResultWaker{Origins: origins, Sender: sender, BusyRetry: defaultResultBusyRetry, BusyTries: defaultResultBusyTries}
}

// Origin returns the ACP origin of sessionKey. It is false for a nil waker.
func (w *ResultWaker) Origin(sessionKey string) (SessionOrigin, bool) {
	if w == nil {
		return SessionOrigin{}, false
	}
	return w.Origins.Lookup(sessionKey)
}

type resultWakeParams struct {
	WakeID    string          `json:"wakeId"`
	Reason    string          `json:"reason"`
	Thread    json.RawMessage `json:"thread"`
	SessionID string          `json:"sessionId"`
	Prompt    string          `json:"prompt"`
	CreatedAt string          `json:"createdAt"`
}

// Wake sends one wake to the thread of origin and reports whether the client
// queued it (accepted or duplicate). reason is a wake reason of the client
// (completed, failed, cancelled); prompt must fit MaxResultWakePromptBytes.
// A false result means the caller must deliver the text another way.
func (w *ResultWaker) Wake(ctx context.Context, origin SessionOrigin, wakeID, reason, prompt string) bool {
	if w == nil || w.Sender == nil || !origin.Wake || wakeID == "" {
		return false
	}
	if len(origin.Thread) == 0 || len(origin.Thread) > MaxWakeThreadBytes || len(prompt) > MaxResultWakePromptBytes {
		slog.Warn("acp.result_wake.invalid", "wake_id", wakeID, "thread_bytes", len(origin.Thread), "prompt_bytes", len(prompt))
		return false
	}
	params, err := json.Marshal(resultWakeParams{
		WakeID: wakeID, Reason: reason, Thread: origin.Thread, SessionID: origin.ACPSessionID,
		Prompt: prompt, CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return false
	}
	tries := max(w.BusyTries, 1)
	for attempt := 1; ; attempt++ {
		deliverCtx, cancel := context.WithTimeout(ctx, resultWakeDeliverLimit)
		outcome, ok := w.Sender.DeliverWake(deliverCtx, origin.TenantID, origin.AgentID, origin.UserID, wakeID, params)
		cancel()
		switch {
		case !ok:
			slog.Warn("acp.result_wake.no_sink", "wake_id", wakeID)
			return false
		case outcome == protocol.ACPWakeOutcomeAccepted || outcome == protocol.ACPWakeOutcomeDuplicate:
			slog.Info("acp.result_wake.delivered", "wake_id", wakeID, "outcome", outcome)
			return true
		case outcome == protocol.ACPWakeOutcomeBusy && attempt < tries:
			timer := time.NewTimer(w.BusyRetry)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return false
			}
		default:
			slog.Warn("acp.result_wake.not_delivered", "wake_id", wakeID, "outcome", outcome, "attempt", attempt)
			return false
		}
	}
}
