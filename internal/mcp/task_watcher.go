package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// watchTiming holds the timings of the task watchers. Zero heartbeat and
// safetyPoll mean "from the task_events setting".
type watchTiming struct {
	heartbeat       time.Duration
	safetyPoll      time.Duration
	defaultPoll     time.Duration // tasks/get interval without pollIntervalMs
	minPoll         time.Duration // clamp of pollIntervalMs
	maxPoll         time.Duration
	eventsPollFloor time.Duration // clamp of nextPollMs
	eventsPollCap   time.Duration
	backoffBase     time.Duration // reconnect backoff 1, 2, 4 ... 60 s
	backoffMax      time.Duration
	// streamFailures is how many stream attempts in a row may fail before
	// the watcher uses events/poll for streamRetryAfter.
	streamFailures   int
	streamRetryAfter time.Duration
	acquireTimeout   time.Duration
}

func defaultWatchTiming() watchTiming {
	return watchTiming{
		defaultPoll:      5 * time.Second,
		minPoll:          time.Second,
		maxPoll:          30 * time.Second,
		eventsPollFloor:  time.Second,
		eventsPollCap:    60 * time.Second,
		backoffBase:      time.Second,
		backoffMax:       60 * time.Second,
		streamFailures:   3,
		streamRetryAfter: 60 * time.Second,
		acquireTimeout:   60 * time.Second,
	}
}

// dedupWindow is how many recent eventIds a watcher remembers.
const dedupWindow = 256

// watchMode is how a watcher learns about changes of its task.
type watchMode int

const (
	modeTasksGet   watchMode = iota // tasks/get at the poll interval
	modeEventsPoll                  // events/poll with the cursor
	modeStream                      // events/stream with the cursor
)

// watchOutcome is the result of one run of a mode.
type watchOutcome int

const (
	outcomeDone   watchOutcome = iota // the task ended or the watcher stops
	outcomeRetry                      // a connection problem: back off, then try again
	outcomeSwitch                     // the mode changed: run the next mode at once
)

// requestCaps is the JSON form of mcp_tasks.request_caps.
type requestCaps struct {
	Tasks           bool `json:"tasks"`
	Events          bool `json:"events"`
	ElicitationForm bool `json:"elicitationForm"`
}

func (c requestCaps) rawOpts() RawOpts {
	return RawOpts{Tasks: c.Tasks, Events: c.Events, ElicitationForm: c.ElicitationForm}
}

func capsFromOpts(o RawOpts) requestCaps {
	return requestCaps{Tasks: o.Tasks, Events: o.Events, ElicitationForm: o.ElicitationForm}
}

func parseRequestCaps(raw json.RawMessage) requestCaps {
	var c requestCaps
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &c)
	}
	return c
}

// taskWatcher follows one mcp_tasks row until the task ends, the row
// expires, or the manager stops. It runs in its own goroutine.
type taskWatcher struct {
	mgr    *TaskWatchManager
	row    store.MCPTask
	caps   requestCaps
	timing watchTiming

	// baseCtx carries the tenant of the row and ends when the manager
	// stops. runCtx also ends when the row expires.
	baseCtx context.Context
	runCtx  context.Context
	cancel  context.CancelFunc
	done    chan struct{}

	conn         *watchConn
	reacquire    bool
	binding      *TaskEventsBinding
	serverName   string
	dedup        *eventDedup
	pollInterval time.Duration
	backoff      time.Duration

	failedStreams int
	pollUntil     time.Time
	streamOff     bool // the server has no events/stream; events/poll only
	eventsOff     bool // no events for this task; tasks/get only
}

func (w *taskWatcher) log() *slog.Logger {
	return slog.With("task_row", w.row.ID, "task_id", w.row.TaskID, "server", w.serverName)
}

// run is the watcher goroutine.
func (w *taskWatcher) run() {
	defer close(w.done)
	defer w.mgr.forget(w)
	defer w.releaseConn()

	for {
		if w.runCtx.Err() != nil {
			w.finish()
			return
		}
		if err := w.ensureConn(); err != nil {
			w.log().Warn("mcp.task.watch_connect_failed", "error", err.Error())
			if !w.sleep(w.nextBackoff()) {
				w.finish()
				return
			}
			continue
		}
		sess := w.conn.entry.state.currentSession()
		if sess == nil || sess.client == nil {
			w.reacquire = true
			if !w.sleep(w.nextBackoff()) {
				w.finish()
				return
			}
			continue
		}

		var out watchOutcome
		switch w.mode(sess) {
		case modeStream:
			out = w.runStream(sess)
		case modeEventsPoll:
			out = w.runEventsPoll(sess)
		default:
			out = w.runTasksGet(sess)
		}
		switch out {
		case outcomeDone:
			w.finish()
			return
		case outcomeRetry:
			if !w.sleep(w.nextBackoff()) {
				w.finish()
				return
			}
		}
	}
}

// finish handles the end of the run context: at expiry the open wakes of
// the row become expired. A manager stop leaves the row unchanged.
func (w *taskWatcher) finish() {
	if w.baseCtx.Err() != nil || !errors.Is(w.runCtx.Err(), context.DeadlineExceeded) {
		return
	}
	n, err := w.mgr.tasks.ExpireWakesForTask(w.baseCtx, w.row.ID)
	if err != nil {
		w.log().Warn("mcp.task.expire_wakes_failed", "error", err.Error())
		return
	}
	w.log().Info("mcp.task.expired", "expired_wakes", n)
}

// mode selects how to watch with the current connection.
func (w *taskWatcher) mode(sess *connSession) watchMode {
	if w.binding == nil || w.eventsOff || !sess.discover.HasExtension(ExtensionEvents) {
		return modeTasksGet
	}
	if w.streamOff || time.Now().Before(w.pollUntil) {
		return modeEventsPoll
	}
	return modeStream
}

func (w *taskWatcher) taskOpts() RawOpts {
	o := w.caps.rawOpts()
	o.Tasks = true
	return o
}

func (w *taskWatcher) eventOpts() RawOpts {
	o := w.caps.rawOpts()
	o.Events = true
	return o
}

func (w *taskWatcher) subscription() json.RawMessage {
	args, _ := json.Marshal(map[string]string{w.binding.TaskArgument: w.row.TaskID})
	return args
}

func (w *taskWatcher) heartbeat() time.Duration {
	if w.timing.heartbeat > 0 {
		return w.timing.heartbeat
	}
	return time.Duration(w.binding.HeartbeatSec) * time.Second
}

func (w *taskWatcher) safetyPoll() time.Duration {
	if w.timing.safetyPoll > 0 {
		return w.timing.safetyPoll
	}
	if w.binding != nil {
		return time.Duration(w.binding.SafetyPollSec) * time.Second
	}
	return time.Duration(taskEventsSafetyPollDefault) * time.Second
}

// runStream keeps one events/stream open until it ends.
func (w *taskWatcher) runStream(sess *connSession) watchOutcome {
	st, err := openEventStream(w.runCtx, sess, w.binding.Event, w.subscription(), w.row.EventCursor, w.eventOpts())
	if err != nil {
		w.log().Warn("mcp.task.stream_open_failed", "error", err.Error())
		return w.streamFailed(false)
	}
	defer st.Close()

	silence := 2 * w.heartbeat()
	watchdog := time.NewTimer(silence)
	defer watchdog.Stop()
	safety := time.NewTicker(w.safetyPoll())
	defer safety.Stop()
	gotFrame := false

	for {
		select {
		case <-w.runCtx.Done():
			return outcomeDone
		case f := <-st.Frames:
			resetTimer(watchdog, silence)
			if !gotFrame {
				gotFrame = true
				w.failedStreams = 0
				w.backoff = 0
			}
			if out, stop := w.handleFrame(sess, f); stop {
				return out
			}
		case err := <-st.Done:
			// Keep the cursors of frames that came before the end.
			for {
				select {
				case f := <-st.Frames:
					if out, stop := w.handleFrame(sess, f); stop {
						return out
					}
					continue
				default:
				}
				break
			}
			return w.streamEnded(err, gotFrame)
		case <-watchdog.C:
			w.log().Warn("mcp.task.stream_silent", "silence", silence.String())
			return w.streamFailed(gotFrame)
		case <-safety.C:
			if done, _ := w.truth(sess); done {
				return outcomeDone
			}
		}
	}
}

// handleFrame applies one stream frame. stop tells the caller to return out.
func (w *taskWatcher) handleFrame(sess *connSession, f streamFrame) (out watchOutcome, stop bool) {
	switch f.Kind {
	case frameActive:
		w.persistCursor(f.Cursor)
		// The first active frame confirms a (re)connect; a later one with
		// truncated=true reports a gap. Both need the truth.
		if done, _ := w.truth(sess); done {
			return outcomeDone, true
		}
	case frameHeartbeat:
		w.persistCursor(f.Cursor)
	case frameEvent:
		w.persistCursor(f.Cursor)
		if done := w.onEvents(sess, []EventOccurrence{*f.Event}); done {
			return outcomeDone, true
		}
	case frameError:
		if f.Error != nil {
			w.log().Info("mcp.task.stream_error", "code", f.Error.Code)
		}
	case frameTerminated:
		code := 0
		if f.Error != nil {
			code = f.Error.Code
		}
		w.log().Warn("mcp.task.stream_terminated", "code", code, "next", "tasks/get only")
		w.eventsOff = true
		return outcomeSwitch, true
	}
	return 0, false
}

// streamEnded classifies the end of an events/stream request.
func (w *taskWatcher) streamEnded(err error, gotFrame bool) watchOutcome {
	if w.runCtx.Err() != nil {
		return outcomeDone
	}
	switch {
	case err == nil:
		// The server ended the subscription response; open it again.
		return outcomeRetry
	case errors.Is(err, ErrMethodNotFound):
		w.log().Info("mcp.task.stream_unsupported", "next", "events/poll")
		w.streamOff = true
		return outcomeSwitch
	case isFatalEventsError(err):
		w.log().Warn("mcp.task.events_refused", "error", err.Error(), "next", "tasks/get only")
		w.eventsOff = true
		return outcomeSwitch
	}
	w.log().Info("mcp.task.stream_dropped", "error", err.Error())
	if !isRPCError(err) {
		w.reacquire = true
	}
	return w.streamFailed(gotFrame)
}

// streamFailed counts a failed stream attempt. After too many in a row the
// watcher uses events/poll for a while.
func (w *taskWatcher) streamFailed(gotFrame bool) watchOutcome {
	if !gotFrame {
		w.failedStreams++
	}
	if w.failedStreams >= w.timing.streamFailures {
		w.failedStreams = 0
		w.pollUntil = time.Now().Add(w.timing.streamRetryAfter)
		w.log().Info("mcp.task.stream_fallback", "next", "events/poll", "for", w.timing.streamRetryAfter.String())
		return outcomeSwitch
	}
	return outcomeRetry
}

// runEventsPoll polls events with the cursor until the stream may be tried
// again.
func (w *taskWatcher) runEventsPoll(sess *connSession) watchOutcome {
	// A (re)connect needs the truth first.
	if done, err := w.truth(sess); done {
		return outcomeDone
	} else if err != nil {
		return w.connectionProblem(err)
	}
	nextSafety := time.Now().Add(w.safetyPoll())
	for {
		res, err := eventsPoll(w.runCtx, sess.client, w.binding.Event, w.subscription(), w.row.EventCursor, w.eventOpts())
		switch {
		case w.runCtx.Err() != nil:
			return outcomeDone
		case err == nil:
		case errors.Is(err, ErrMethodNotFound), isFatalEventsError(err):
			w.log().Warn("mcp.task.events_poll_refused", "error", err.Error(), "next", "tasks/get only")
			w.eventsOff = true
			return outcomeSwitch
		default:
			return w.connectionProblem(err)
		}
		w.backoff = 0
		w.persistCursor(res.Cursor)
		if done := w.onEvents(sess, res.Events); done {
			return outcomeDone
		}
		if res.Truncated || !time.Now().Before(nextSafety) {
			if done, _ := w.truth(sess); done {
				return outcomeDone
			}
			nextSafety = time.Now().Add(w.safetyPoll())
		}
		if !w.streamOff && !time.Now().Before(w.pollUntil) {
			return outcomeSwitch
		}
		wait := clampDuration(time.Duration(res.NextPollMs)*time.Millisecond, w.timing.eventsPollFloor, w.timing.eventsPollCap)
		if res.HasMore {
			wait = 0
		}
		if !w.sleep(wait) {
			return outcomeDone
		}
	}
}

// runTasksGet polls tasks/get at the poll interval.
func (w *taskWatcher) runTasksGet(sess *connSession) watchOutcome {
	for {
		done, err := w.truth(sess)
		if done {
			return outcomeDone
		}
		if err != nil {
			return w.connectionProblem(err)
		}
		w.backoff = 0
		if !w.sleep(w.pollInterval) {
			return outcomeDone
		}
	}
}

// connectionProblem handles a failed request: a transport error makes the
// watcher take a fresh pool reference before the next try.
func (w *taskWatcher) connectionProblem(err error) watchOutcome {
	if w.runCtx.Err() != nil {
		return outcomeDone
	}
	w.log().Info("mcp.task.request_failed", "error", err.Error())
	if !isRPCError(err) {
		w.reacquire = true
	}
	return outcomeRetry
}

// onEvents applies new events: one tasks/get for the batch, then the
// pr_ready check against the refreshed status. Event data never sets the
// status. It reports whether the task ended.
func (w *taskWatcher) onEvents(sess *connSession, events []EventOccurrence) bool {
	fresh := make([]EventOccurrence, 0, len(events))
	for i := range events {
		if events[i].EventID != "" && w.dedup.firstSeen(events[i].EventID) {
			fresh = append(fresh, events[i])
		}
	}
	if len(fresh) == 0 {
		return false
	}
	done, err := w.truth(sess)
	if done || err != nil {
		return done
	}
	if w.row.PRWoken || w.row.Status != TaskStatusWorking {
		return false
	}
	for i := range fresh {
		if eventPRURL(&fresh[i]) {
			w.prReady()
			break
		}
	}
	return false
}

// prReady records the pr_ready wake once.
func (w *taskWatcher) prReady() {
	if !w.row.HasACPOrigin() {
		return
	}
	// The wake goes in first: its id is stable, so a crash before
	// MarkPRWoken repeats nothing.
	if !w.wake(store.MCPWakeReasonPRReady, "pr") {
		return
	}
	if _, err := w.mgr.tasks.MarkPRWoken(w.baseCtx, w.row.ID); err != nil {
		w.log().Warn("mcp.task.store_failed", "op", "mark_pr_woken", "error", err.Error())
		return
	}
	w.row.PRWoken = true
}

// truth reads the task with tasks/get and applies it. It reports whether the
// watcher must stop (terminal status or a task the server no longer knows).
func (w *taskWatcher) truth(sess *connSession) (bool, error) {
	raw, err := rawCall(w.runCtx, sess.client, methodTasksGet, map[string]any{"taskId": w.row.TaskID}, w.taskOpts())
	if err != nil {
		if w.runCtx.Err() != nil {
			return true, nil
		}
		if errors.Is(err, ErrInvalidParams) {
			w.unknownTask()
			return true, nil
		}
		return false, err
	}
	var task DetailedTask
	if err := json.Unmarshal(raw, &task); err != nil || task.Status == "" {
		return false, fmt.Errorf("mcp tasks/get: invalid result")
	}
	return w.apply(task), nil
}

// unknownTask ends the watch of a task that the server no longer knows. The
// status stays as it was; the open wakes expire.
func (w *taskWatcher) unknownTask() {
	w.log().Warn("mcp.task.unknown_on_server", "next", "stop watching")
	if _, err := w.mgr.tasks.ExpireWakesForTask(w.baseCtx, w.row.ID); err != nil {
		w.log().Warn("mcp.task.store_failed", "op", "expire_wakes", "error", err.Error())
	}
	if err := w.mgr.tasks.MarkTaskTerminal(w.baseCtx, w.row.ID); err != nil {
		w.log().Warn("mcp.task.store_failed", "op", "mark_terminal", "error", err.Error())
	}
}

// apply stores the truth of one tasks/get and records its wakes. It reports
// whether the task is terminal.
func (w *taskWatcher) apply(task DetailedTask) bool {
	if task.PollIntervalMs != nil && *task.PollIntervalMs > 0 {
		w.pollInterval = clampDuration(time.Duration(*task.PollIntervalMs)*time.Millisecond, w.timing.minPoll, w.timing.maxPoll)
	}
	terminal := isTerminalStatus(task.Status)

	switch {
	case task.Status == TaskStatusInputRequired:
		w.inputWakes(task)
	case terminal:
		// The wake goes in before terminal_at: a row with terminal_at is
		// not resumed at the next start.
		w.wake(task.Status, task.Status)
	}

	var msg *string
	if task.StatusMessage != "" {
		m := task.StatusMessage
		msg = &m
	}
	if terminal || task.Status != w.row.Status || !sameString(msg, w.row.StatusMessage) {
		err := w.mgr.tasks.UpdateTaskStatus(w.baseCtx, w.row.ID, store.MCPTaskStatusUpdate{
			Status: task.Status, StatusMessage: msg, Terminal: terminal,
		})
		if err != nil {
			w.log().Warn("mcp.task.store_failed", "op", "update_status", "error", err.Error())
		} else {
			if task.Status != w.row.Status {
				w.log().Info("mcp.task.status", "status", task.Status)
			}
			w.row.Status, w.row.StatusMessage = task.Status, msg
		}
	}
	if terminal {
		w.log().Info("mcp.task.ended", "status", task.Status)
	}
	return terminal
}

// inputWakes records one input_required wake per input request key that is
// not answered. With no input requests (the server asks in statusMessage)
// it records one wake per lastUpdatedAt.
func (w *taskWatcher) inputWakes(task DetailedTask) {
	if !w.row.HasACPOrigin() {
		return
	}
	if len(task.InputRequests) == 0 {
		w.wake(store.MCPWakeReasonInputRequired, "status:"+task.LastUpdatedAt)
		return
	}
	// answered_keys changes in mcp_task input, so read the row again.
	row, err := w.mgr.tasks.GetTask(w.baseCtx, w.row.ID)
	if err != nil || row == nil {
		w.log().Warn("mcp.task.store_failed", "op", "get_task", "error", fmt.Sprint(err))
		return
	}
	keys := make([]string, 0, len(task.InputRequests))
	for k := range task.InputRequests {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !row.IsAnswered(k) {
			w.wake(store.MCPWakeReasonInputRequired, k)
		}
	}
}

// wake records a wake for the row. It reports false only when the store
// failed.
func (w *taskWatcher) wake(reason, discriminator string) bool {
	if !w.row.HasACPOrigin() {
		return true
	}
	wk := store.MCPTaskWake{
		WakeID:    store.MCPWakeID(w.row.ID, reason, discriminator),
		TaskRowID: w.row.ID,
		ThreadKey: store.MCPWakeThreadKey(w.row.ThreadContext, w.row.ACPSessionID),
		Reason:    reason,
	}
	created, err := w.mgr.tasks.CreateWake(w.baseCtx, &wk)
	if err != nil {
		w.log().Warn("mcp.task.store_failed", "op", "create_wake", "reason", reason, "error", err.Error())
		return false
	}
	if created {
		w.log().Info("mcp.task.wake_created", "wake_id", wk.WakeID, "reason", reason)
		w.mgr.notifyWake(w.baseCtx, wk)
	}
	return true
}

// persistCursor stores a non-empty cursor that differs from the last one.
func (w *taskWatcher) persistCursor(c *string) {
	c = nonEmptyCursor(c)
	if c == nil || (w.row.EventCursor != nil && *w.row.EventCursor == *c) {
		return
	}
	if err := w.mgr.tasks.UpdateTaskCursor(w.baseCtx, w.row.ID, *c); err != nil {
		w.log().Warn("mcp.task.store_failed", "op", "update_cursor", "error", err.Error())
		return
	}
	w.row.EventCursor = c
}

// ensureConn holds a pool reference for the row's server.
func (w *taskWatcher) ensureConn() error {
	if w.conn != nil && !w.reacquire && w.conn.entry.state.connected.Load() {
		return nil
	}
	w.releaseConn()
	w.reacquire = false
	conn, err := w.mgr.acquire(w.baseCtx, &w.row)
	if err != nil {
		return err
	}
	w.conn = conn
	w.binding = conn.binding
	w.serverName = conn.serverName
	return nil
}

func (w *taskWatcher) releaseConn() {
	if w.conn != nil {
		w.conn.release()
		w.conn = nil
	}
}

func (w *taskWatcher) nextBackoff() time.Duration {
	if w.backoff == 0 {
		w.backoff = w.timing.backoffBase
	} else {
		w.backoff = min(2*w.backoff, w.timing.backoffMax)
	}
	return w.backoff
}

// sleep waits d and reports false when the run context ended.
func (w *taskWatcher) sleep(d time.Duration) bool {
	if d <= 0 {
		return w.runCtx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-w.runCtx.Done():
		return false
	}
}

func isTerminalStatus(s string) bool {
	switch s {
	case TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled:
		return true
	}
	return false
}

// isFatalEventsError reports an events error that will not go away by
// retrying the same subscription.
func isFatalEventsError(err error) bool {
	return errors.Is(err, ErrEventsForbidden) || errors.Is(err, ErrEventsNotFound) ||
		errors.Is(err, ErrEventsUnsupported) || errors.Is(err, ErrInvalidParams)
}

func isRPCError(err error) bool {
	var e *RPCError
	return errors.As(err, &e)
}

func clampDuration(d, lo, hi time.Duration) time.Duration {
	return max(lo, min(d, hi))
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func sameString(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
