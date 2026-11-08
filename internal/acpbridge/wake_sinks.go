package acpbridge

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// Limits of wake delivery.
const (
	// MaxWakeSinksPerConnection bounds the agents that one adapter
	// connection can subscribe for.
	MaxWakeSinksPerConnection = 16
	// MaxWakeParamsBytes bounds the ACP params of one wake.
	MaxWakeParamsBytes = 16 << 10
	// DefaultWakeAckTimeout is how long the gateway waits for acp.wake.ack.
	// The adapter waits at most 30 s for the ACP client, then acks.
	DefaultWakeAckTimeout = 40 * time.Second
	// maxWakeSinkTries is the number of sinks that one delivery tries: the
	// last subscribed sink, and the next one once on unsupported or failed.
	maxWakeSinkTries = 2
)

var (
	// ErrWakeSinkLimit means the connection has too many subscriptions.
	ErrWakeSinkLimit = errors.New("ACP wake subscription limit reached")
	// ErrWakeInvalid means a subscription or an ack has a bad shape.
	ErrWakeInvalid = errors.New("invalid ACP wake subscription or ack")
)

// WakeSink is one adapter connection that receives the wakes of an agent.
// The identity comes from the authenticated connection, never from params.
type WakeSink struct {
	ConnectionID string
	TenantID     uuid.UUID
	AgentID      uuid.UUID
	UserID       string
	Sender       CriticalEventSender
}

type wakeSinkKey struct {
	connectionID string
	tenantID     uuid.UUID
	agentID      uuid.UUID
}

type wakeSinkState struct {
	WakeSink
	// seq orders the subscriptions; the highest one is the last subscribed.
	seq uint64
}

type wakeAckKey struct {
	connectionID string
	wakeID       string
}

// WakeSinks holds the live wake subscriptions of the gateway and delivers
// wakes to them. It implements the wake deliverer of the MCP wake
// dispatcher.
type WakeSinks struct {
	mu          sync.Mutex
	seq         uint64
	sinks       map[wakeSinkKey]*wakeSinkState
	acks        map[wakeAckKey]chan string
	onSubscribe func()
	ackTimeout  time.Duration
}

// NewWakeSinks creates an empty registry. A zero ackTimeout selects
// DefaultWakeAckTimeout.
func NewWakeSinks(ackTimeout time.Duration) *WakeSinks {
	if ackTimeout <= 0 {
		ackTimeout = DefaultWakeAckTimeout
	}
	return &WakeSinks{
		sinks:      make(map[wakeSinkKey]*wakeSinkState),
		acks:       make(map[wakeAckKey]chan string),
		ackTimeout: ackTimeout,
	}
}

// SetOnSubscribe sets a function that runs after each subscription, for
// example to wake the dispatcher. It must return quickly.
func (w *WakeSinks) SetOnSubscribe(fn func()) {
	w.mu.Lock()
	w.onSubscribe = fn
	w.mu.Unlock()
}

// Subscribe adds or renews a sink. A renewed sink becomes the last
// subscribed one.
func (w *WakeSinks) Subscribe(sink WakeSink) error {
	if sink.ConnectionID == "" || sink.TenantID == uuid.Nil || sink.AgentID == uuid.Nil || sink.UserID == "" || sink.Sender == nil {
		return ErrWakeInvalid
	}
	key := wakeSinkKey{connectionID: sink.ConnectionID, tenantID: sink.TenantID, agentID: sink.AgentID}
	w.mu.Lock()
	if _, exists := w.sinks[key]; !exists {
		perConnection := 0
		for k := range w.sinks {
			if k.connectionID == sink.ConnectionID {
				perConnection++
			}
		}
		if perConnection >= MaxWakeSinksPerConnection {
			w.mu.Unlock()
			return ErrWakeSinkLimit
		}
	}
	w.seq++
	w.sinks[key] = &wakeSinkState{WakeSink: sink, seq: w.seq}
	onSubscribe := w.onSubscribe
	w.mu.Unlock()
	if onSubscribe != nil {
		onSubscribe()
	}
	return nil
}

// Ack gives the outcome of a wake to the waiting delivery. It returns
// ErrNotFound when no delivery of this wake waits on this connection.
func (w *WakeSinks) Ack(connectionID, wakeID, outcome string) error {
	if wakeID == "" || !protocol.IsACPWakeOutcome(outcome) {
		return ErrWakeInvalid
	}
	key := wakeAckKey{connectionID: connectionID, wakeID: wakeID}
	w.mu.Lock()
	ch, ok := w.acks[key]
	delete(w.acks, key)
	w.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	ch <- outcome
	return nil
}

// ReleaseConnection removes the sinks of a closed connection and fails its
// waiting deliveries.
func (w *WakeSinks) ReleaseConnection(connectionID string) {
	w.mu.Lock()
	for key := range w.sinks {
		if key.connectionID == connectionID {
			delete(w.sinks, key)
		}
	}
	var waiting []chan string
	for key, ch := range w.acks {
		if key.connectionID == connectionID {
			waiting = append(waiting, ch)
			delete(w.acks, key)
		}
	}
	w.mu.Unlock()
	for _, ch := range waiting {
		ch <- protocol.ACPWakeOutcomeFailed
	}
}

// Len returns the number of live sinks.
func (w *WakeSinks) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.sinks)
}

// DeliverWake sends one wake to the last subscribed sink of the tenant,
// agent and user, and on unsupported or failed to the next sink once. A
// sink that answers unsupported is removed. ok is false when no live sink
// exists; else outcome is one of the acp.wake.ack outcomes.
func (w *WakeSinks) DeliverWake(ctx context.Context, tenantID, agentID uuid.UUID, userID, wakeID string, params json.RawMessage) (outcome string, ok bool) {
	candidates := w.candidates(tenantID, agentID, userID)
	if len(candidates) == 0 {
		return "", false
	}
	if wakeID == "" || len(params) > MaxWakeParamsBytes {
		return protocol.ACPWakeOutcomeFailed, true
	}
	outcome = protocol.ACPWakeOutcomeFailed
	for i, sink := range candidates {
		if i == maxWakeSinkTries {
			break
		}
		outcome = w.deliverTo(ctx, sink, wakeID, params)
		switch outcome {
		case protocol.ACPWakeOutcomeAccepted, protocol.ACPWakeOutcomeDuplicate, protocol.ACPWakeOutcomeBusy:
			return outcome, true
		case protocol.ACPWakeOutcomeUnsupported:
			w.remove(sink)
		}
		if ctx.Err() != nil {
			break
		}
	}
	return outcome, true
}

// candidates returns the matching sinks, last subscribed first.
func (w *WakeSinks) candidates(tenantID, agentID uuid.UUID, userID string) []*wakeSinkState {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []*wakeSinkState
	for key, sink := range w.sinks {
		if key.tenantID == tenantID && key.agentID == agentID && (userID == "" || sink.UserID == userID) {
			out = append(out, sink)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq > out[j].seq })
	return out
}

func (w *WakeSinks) remove(sink *wakeSinkState) {
	key := wakeSinkKey{connectionID: sink.ConnectionID, tenantID: sink.TenantID, agentID: sink.AgentID}
	w.mu.Lock()
	if w.sinks[key] == sink {
		delete(w.sinks, key)
	}
	w.mu.Unlock()
}

// deliverTo sends acp.wake to one sink and waits for its ack.
func (w *WakeSinks) deliverTo(ctx context.Context, sink *wakeSinkState, wakeID string, params json.RawMessage) string {
	key := wakeAckKey{connectionID: sink.ConnectionID, wakeID: wakeID}
	ch := make(chan string, 1)
	w.mu.Lock()
	if _, busy := w.acks[key]; busy {
		w.mu.Unlock()
		return protocol.ACPWakeOutcomeFailed
	}
	w.acks[key] = ch
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		if w.acks[key] == ch {
			delete(w.acks, key)
		}
		w.mu.Unlock()
	}()

	event := protocol.NewEvent(protocol.EventACPWake, map[string]any{"wakeId": wakeID, "params": params})
	if err := sink.Sender.SendCriticalEvent(*event); err != nil {
		slog.Warn("acp.wake.send_failed", "connection", sink.ConnectionID, "wake_id", wakeID, "error", err)
		return protocol.ACPWakeOutcomeFailed
	}
	timer := time.NewTimer(w.ackTimeout)
	defer timer.Stop()
	select {
	case outcome := <-ch:
		return outcome
	case <-timer.C:
		slog.Warn("acp.wake.ack_timeout", "connection", sink.ConnectionID, "wake_id", wakeID)
		return protocol.ACPWakeOutcomeFailed
	case <-ctx.Done():
		return protocol.ACPWakeOutcomeFailed
	}
}
