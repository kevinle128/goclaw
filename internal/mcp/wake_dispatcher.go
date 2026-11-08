package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// Wake size limits of the ACP request.
const (
	// maxWakePromptBytes bounds the prompt text of a wake.
	maxWakePromptBytes = 2 << 10
	// maxWakeFrameBytes bounds the whole JSON-RPC frame of a wake.
	maxWakeFrameBytes = 16 << 10
	// wakeFrameOverhead is the size of the JSON-RPC envelope around params.
	wakeFrameOverhead = 128
	// maxWakeTaskIDBytes bounds a task id that goes into a wake prompt.
	maxWakeTaskIDBytes = 256
)

// WakeDeliverer sends one wake to a live ACP adapter connection of the
// tenant, agent and user. ok is false when no live connection subscribed;
// else outcome is one of the protocol.ACPWakeOutcome values.
type WakeDeliverer interface {
	DeliverWake(ctx context.Context, tenantID, agentID uuid.UUID, userID, wakeID string, params json.RawMessage) (outcome string, ok bool)
}

// WakeDispatcherConfig configures a WakeDispatcher.
type WakeDispatcherConfig struct {
	Tasks     store.MCPTaskStore
	Servers   store.MCPServerStore
	Deliverer WakeDeliverer
}

// wakeTiming holds the intervals and limits of the dispatcher. Tests make
// them short.
type wakeTiming struct {
	// interval is the period of the scan for due wakes.
	interval time.Duration
	// noSinkRetry is the delay of a wake without a live sink.
	noSinkRetry time.Duration
	// busyRetry is the delay after a busy answer.
	busyRetry time.Duration
	// failedBackoff is the delay after the n-th failed delivery; the last
	// value repeats.
	failedBackoff []time.Duration
	// threadSpacing is the minimum time between delivered wakes of a thread.
	threadSpacing time.Duration
	// threadHourlyCap is the maximum of delivered wakes per thread per hour.
	threadHourlyCap int
	// hourlyCapRetry is the delay of a wake over the hourly cap. The store
	// gives no time of the oldest delivery in the hour, so the wake is
	// checked again after this delay.
	hourlyCapRetry time.Duration
	// deliverTimeout bounds one delivery, which can try two sinks.
	deliverTimeout time.Duration
	// batch is the maximum number of due wakes of one scan.
	batch int
	// parallel is the maximum number of deliveries at the same time.
	parallel int
}

func defaultWakeTiming() wakeTiming {
	return wakeTiming{
		interval:        45 * time.Second,
		noSinkRetry:     60 * time.Second,
		busyRetry:       30 * time.Second,
		failedBackoff:   []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute},
		threadSpacing:   30 * time.Second,
		threadHourlyCap: 20,
		hourlyCapRetry:  3 * time.Minute,
		deliverTimeout:  90 * time.Second,
		batch:           100,
		parallel:        16,
	}
}

// WakeDispatcher delivers the due rows of mcp_task_wakes to ACP adapters.
// It keeps the per-thread limits of the ACP client: one wake in flight per
// thread, at least 30 s between delivered wakes of a thread, and at most 20
// delivered wakes of a thread per hour.
type WakeDispatcher struct {
	tasks     store.MCPTaskStore
	servers   store.MCPServerStore
	deliverer WakeDeliverer
	timing    wakeTiming
	now       func() time.Time

	kick chan struct{}
	// noSinkKick asks the next scan to include no_sink wakes that are not
	// due yet (a new adapter subscribed).
	noSinkKick atomic.Bool

	mu       sync.Mutex
	inFlight map[string]struct{}
	// failures counts the consecutive failed deliveries of a wake. It is
	// not stored; after a restart the backoff starts again at 30 s.
	failures map[string]int
	slots    chan struct{}

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started atomic.Bool
}

// NewWakeDispatcher creates a dispatcher. Call Start to run it.
func NewWakeDispatcher(cfg WakeDispatcherConfig) (*WakeDispatcher, error) {
	if cfg.Tasks == nil || cfg.Servers == nil || cfg.Deliverer == nil {
		return nil, errors.New("mcp wake dispatcher: task store, server store and deliverer are required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	timing := defaultWakeTiming()
	return &WakeDispatcher{
		tasks:     cfg.Tasks,
		servers:   cfg.Servers,
		deliverer: cfg.Deliverer,
		timing:    timing,
		now:       time.Now,
		kick:      make(chan struct{}, 1),
		inFlight:  make(map[string]struct{}),
		failures:  make(map[string]int),
		slots:     make(chan struct{}, timing.parallel),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// Start runs the dispatch loop until Stop. A second call does nothing.
func (d *WakeDispatcher) Start() {
	if !d.started.CompareAndSwap(false, true) {
		return
	}
	d.wg.Go(d.run)
}

// Stop ends the loop and waits a bounded time for the deliveries in flight.
// Rows of interrupted deliveries stay due and are sent again later with the
// same wake id.
func (d *WakeDispatcher) Stop() {
	d.cancel()
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(stopWaitLimit):
		slog.Warn("mcp.wake.stop_timeout", "waited", stopWaitLimit.String())
	}
}

// Kick asks for a scan of the due wakes now (for example after a new wake
// row). It does not block.
func (d *WakeDispatcher) Kick() {
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

// KickNoSink asks for a scan now that also sends the no_sink wakes before
// their retry time, because an adapter subscribed.
func (d *WakeDispatcher) KickNoSink() {
	d.noSinkKick.Store(true)
	d.Kick()
}

func (d *WakeDispatcher) run() {
	ticker := time.NewTicker(d.timing.interval)
	defer ticker.Stop()
	for {
		d.dispatchDue()
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
		case <-d.kick:
		}
	}
}

// dispatchDue starts a delivery for each due wake whose thread has no wake
// in flight.
func (d *WakeDispatcher) dispatchDue() {
	now := d.now()
	horizon := now
	includeNoSink := d.noSinkKick.Swap(false)
	if includeNoSink {
		// no_sink rows wait at most noSinkRetry, so this horizon covers all.
		horizon = now.Add(d.timing.noSinkRetry)
	}
	due, err := d.tasks.ListDueWakes(d.ctx, horizon, d.timing.batch)
	if err != nil {
		if d.ctx.Err() == nil {
			slog.Warn("mcp.wake.list_failed", "error", err.Error())
		}
		return
	}
	for i := range due {
		item := due[i]
		if item.Wake.NextAttemptAt.After(now) && item.Wake.State != store.MCPWakeStateNoSink {
			continue
		}
		if !d.claimThread(item.Wake.ThreadKey) {
			continue
		}
		select {
		case d.slots <- struct{}{}:
		default:
			// All delivery slots are busy; a finished delivery kicks a scan.
			d.releaseThread(item.Wake.ThreadKey)
			return
		}
		d.wg.Go(func() {
			defer func() {
				<-d.slots
				d.releaseThread(item.Wake.ThreadKey)
				d.Kick()
			}()
			d.deliver(item)
		})
	}
}

func (d *WakeDispatcher) claimThread(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, busy := d.inFlight[key]; busy {
		return false
	}
	d.inFlight[key] = struct{}{}
	return true
}

func (d *WakeDispatcher) releaseThread(key string) {
	d.mu.Lock()
	delete(d.inFlight, key)
	d.mu.Unlock()
}

// deliver applies the limits to one wake, sends it, and records the outcome.
func (d *WakeDispatcher) deliver(item store.MCPTaskWakeDue) {
	wake, task := item.Wake, item.Task
	ctx := store.WithTenantID(d.ctx, wake.TenantID)
	log := slog.With("wake_id", wake.WakeID, "task_row", task.ID, "reason", wake.Reason)
	now := d.now()

	if !now.Before(task.ExpiresAt) {
		d.mark(ctx, log, wake, store.MCPWakeStateExpired, now)
		return
	}
	stats, err := d.tasks.WakeThreadStats(ctx, wake.ThreadKey, now.Add(-time.Hour))
	if err != nil {
		if ctx.Err() == nil {
			log.Warn("mcp.wake.stats_failed", "error", err.Error())
		}
		return
	}
	if stats.LastDeliveredAt != nil {
		if next := stats.LastDeliveredAt.Add(d.timing.threadSpacing); now.Before(next) {
			d.mark(ctx, log, wake, wake.State, next)
			return
		}
	}
	if stats.DeliveredSince >= d.timing.threadHourlyCap {
		log.Info("mcp.wake.thread_hourly_cap", "delivered", stats.DeliveredSince)
		d.mark(ctx, log, wake, wake.State, now.Add(d.timing.hourlyCapRetry))
		return
	}

	params, err := d.buildParams(ctx, item)
	if err != nil {
		if errors.Is(err, errWakeUnbuildable) {
			log.Warn("mcp.wake.unbuildable", "error", err.Error())
			d.mark(ctx, log, wake, store.MCPWakeStateExpired, now)
			return
		}
		log.Warn("mcp.wake.build_failed", "error", err.Error())
		d.mark(ctx, log, wake, store.MCPWakeStatePending, now.Add(d.failedDelay(wake.WakeID)))
		return
	}

	deliverCtx, cancel := context.WithTimeout(ctx, d.timing.deliverTimeout)
	outcome, ok := d.deliverer.DeliverWake(deliverCtx, wake.TenantID, task.AgentID, task.UserID, wake.WakeID, params)
	cancel()
	if d.ctx.Err() != nil {
		// Stopping: leave the row due, so the same wake id is sent again.
		return
	}
	now = d.now()
	switch {
	case !ok:
		d.clearFailures(wake.WakeID)
		d.mark(ctx, log, wake, store.MCPWakeStateNoSink, now.Add(d.timing.noSinkRetry))
	case outcome == protocol.ACPWakeOutcomeAccepted || outcome == protocol.ACPWakeOutcomeDuplicate:
		d.clearFailures(wake.WakeID)
		log.Info("mcp.wake.delivered", "outcome", outcome)
		d.mark(ctx, log, wake, store.MCPWakeStateDelivered, now)
	case outcome == protocol.ACPWakeOutcomeBusy:
		d.clearFailures(wake.WakeID)
		d.mark(ctx, log, wake, store.MCPWakeStatePending, now.Add(d.timing.busyRetry))
	case outcome == protocol.ACPWakeOutcomeUnsupported:
		d.clearFailures(wake.WakeID)
		log.Info("mcp.wake.unsupported")
		d.mark(ctx, log, wake, store.MCPWakeStateNoSink, now.Add(d.timing.noSinkRetry))
	default:
		delay := d.failedDelay(wake.WakeID)
		log.Warn("mcp.wake.failed", "outcome", outcome, "retry_in", delay.String())
		d.mark(ctx, log, wake, store.MCPWakeStatePending, now.Add(delay))
	}
}

func (d *WakeDispatcher) mark(ctx context.Context, log *slog.Logger, wake store.MCPTaskWake, state string, next time.Time) {
	if state == store.MCPWakeStateExpired || state == store.MCPWakeStateDelivered {
		d.clearFailures(wake.WakeID)
	}
	if err := d.tasks.MarkWakeOutcome(ctx, wake.WakeID, state, next); err != nil && ctx.Err() == nil {
		log.Warn("mcp.wake.store_failed", "op", "mark_outcome", "state", state, "error", err.Error())
	}
}

// failedDelay counts one more failed delivery of the wake and returns its
// backoff delay.
func (d *WakeDispatcher) failedDelay(wakeID string) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := d.failures[wakeID]
	d.failures[wakeID] = n + 1
	steps := d.timing.failedBackoff
	return steps[min(n, len(steps)-1)]
}

func (d *WakeDispatcher) clearFailures(wakeID string) {
	d.mu.Lock()
	delete(d.failures, wakeID)
	d.mu.Unlock()
}

// errWakeUnbuildable marks a wake whose params can never be built safely.
var errWakeUnbuildable = errors.New("wake params cannot be built")

// wakeParams is the params object of the ACP _buzz/wake request.
type wakeParams struct {
	WakeID string `json:"wakeId"`
	Reason string `json:"reason"`
	// Thread is the stored thread object unchanged, or null.
	Thread    json.RawMessage `json:"thread"`
	SessionID string          `json:"sessionId"`
	Task      wakeTask        `json:"task"`
	Prompt    string          `json:"prompt"`
	CreatedAt string          `json:"createdAt"`
}

type wakeTask struct {
	Server        string `json:"server"`
	TaskID        string `json:"taskId"`
	Status        string `json:"status"`
	LastUpdatedAt string `json:"lastUpdatedAt"`
}

// buildParams builds the ACP params from the wake and task rows and the
// server name only. No text of the MCP server (status message, question,
// result) goes into a wake.
func (d *WakeDispatcher) buildParams(ctx context.Context, item store.MCPTaskWakeDue) (json.RawMessage, error) {
	wake, task := item.Wake, item.Task
	if !safeWakeTaskID(task.TaskID) {
		return nil, fmt.Errorf("%w: task id is empty, too long or has unsafe characters", errWakeUnbuildable)
	}
	prompt, ok := wakePrompt(wake.Reason, task.TaskID)
	if !ok {
		return nil, fmt.Errorf("%w: unknown reason %q", errWakeUnbuildable, wake.Reason)
	}
	srv, err := d.servers.GetServer(ctx, task.ServerID)
	if err != nil {
		return nil, fmt.Errorf("load mcp server: %w", err)
	}
	if srv == nil {
		return nil, fmt.Errorf("load mcp server: %s not found", task.ServerID)
	}
	thread := task.ThreadContext
	if !json.Valid(thread) {
		thread = nil
	}
	params, err := json.Marshal(wakeParams{
		WakeID:    wake.WakeID,
		Reason:    wake.Reason,
		Thread:    thread,
		SessionID: task.ACPSessionID,
		Task: wakeTask{
			Server:        srv.Name,
			TaskID:        task.TaskID,
			Status:        wakeTaskStatus(task.Status),
			LastUpdatedAt: task.UpdatedAt.UTC().Format(time.RFC3339),
		},
		Prompt:    prompt,
		CreatedAt: wake.CreatedAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errWakeUnbuildable, err)
	}
	if len(params)+wakeFrameOverhead > maxWakeFrameBytes {
		return nil, fmt.Errorf("%w: frame of %d bytes is over the limit", errWakeUnbuildable, len(params)+wakeFrameOverhead)
	}
	return params, nil
}

// wakePrompt returns the English prompt of a wake reason. The task id is
// checked by safeWakeTaskID.
func wakePrompt(reason, taskID string) (string, bool) {
	const prefix = "[goclaw task update] Archon run "
	var prompt string
	switch reason {
	case store.MCPWakeReasonInputRequired:
		prompt = prefix + taskID + ` needs approval. Call the tool mcp_task with action "input" and task_id "` + taskID +
			`" now. The tool shows the question to the people in this thread and sends their answer to Archon.`
	case store.MCPWakeReasonPRReady:
		prompt = prefix + taskID + ` opened a pull request and waits for CI. Call mcp_task with action "status" and task_id "` + taskID +
			`", then post the PR link in this thread.`
	case store.MCPWakeReasonCompleted, store.MCPWakeReasonFailed, store.MCPWakeReasonCancelled:
		prompt = prefix + taskID + " is now " + reason + `. Call mcp_task with action "status" and task_id "` + taskID +
			`", then post the result (PR link and CI result, or the error) in this thread.`
	default:
		return "", false
	}
	if len(prompt) > maxWakePromptBytes {
		return "", false
	}
	return prompt, true
}

// safeWakeTaskID accepts a task id that is safe to put in a prompt: valid
// UTF-8, at most 256 bytes, no control characters, white space, quotes,
// back quotes or back slashes.
func safeWakeTaskID(id string) bool {
	if id == "" || len(id) > maxWakeTaskIDBytes || !utf8.ValidString(id) {
		return false
	}
	return !strings.ContainsFunc(id, func(r rune) bool {
		return unicode.IsControl(r) || unicode.IsSpace(r) || r == '"' || r == '\\' || r == '`'
	})
}

// wakeTaskStatus returns a known task status, or "unknown" for any other
// text of the server.
func wakeTaskStatus(status string) string {
	switch status {
	case TaskStatusWorking, TaskStatusInputRequired, TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled:
		return status
	}
	return "unknown"
}
