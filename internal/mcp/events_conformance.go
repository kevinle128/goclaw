package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

// EventsClientOptions configures RunEventsClient.
type EventsClientOptions struct {
	// Duration is how long the subscriptions run. It must be positive.
	Duration time.Duration
	// Heartbeat is the expected heartbeat interval of a push stream. A
	// stream with no frame for twice this interval is dead. Zero means the
	// default of the task_events setting heartbeat_sec.
	Heartbeat time.Duration
}

// eventDescriptor is the part of one events/list entry that a subscriber
// reads.
type eventDescriptor struct {
	Name     string   `json:"name"`
	Delivery []string `json:"delivery"`
}

// eventsListResult is the result of events/list.
type eventsListResult struct {
	Events []eventDescriptor `json:"events"`
}

// RunEventsClient connects to the MCP server at url over Streamable HTTP
// with the same connect path as the gateway, and subscribes to every event
// type that the server lists, in the delivery mode of its descriptor. It
// sends no events/* request when the server does not declare the events
// extension. The subscriptions use the stream and poll rules of the task
// watcher: cursors stay in memory, a null or absent cursor is never stored,
// nextPollMs gets the poll floor and cap, hasMore polls again at once, and a
// silent stream reconnects with its cursor. Webhook delivery is not
// supported, so a type that offers only webhook is skipped.
//
// The function returns nil when Duration ends or ctx ends after a
// successful connect.
func RunEventsClient(ctx context.Context, url string, opts EventsClientOptions) error {
	if url == "" {
		return errors.New("mcp events client: empty server url")
	}
	if opts.Duration <= 0 {
		return errors.New("mcp events client: duration must be positive")
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = time.Duration(taskEventsHeartbeatDefault) * time.Second
	}

	ss, _, err := connectAndDiscover(ctx, "events-client", "streamable-http", "", nil, nil, url, nil, 0, ProtocolAuto)
	if err != nil {
		return fmt.Errorf("mcp events client: connect: %w", err)
	}
	defer func() { _ = ss.client.Close() }()

	sess := ss.currentSession()
	log := slog.With("protocol_version", ss.client.ProtocolVersion())
	if sess == nil || sess.era != EraModern {
		log.Info("mcp.events_client.legacy_connection", "next", "no events requests")
		return nil
	}
	if !sess.discover.HasExtension(ExtensionEvents) {
		log.Info("mcp.events_client.no_extension", "next", "no events requests")
		return nil
	}

	runCtx, cancel := context.WithTimeout(ctx, opts.Duration)
	defer cancel()

	raw, err := rawCall(runCtx, sess.client, methodEventsList, map[string]any{}, RawOpts{Events: true})
	if err != nil {
		return fmt.Errorf("mcp events client: events/list: %w", err)
	}
	var list eventsListResult
	if err := json.Unmarshal(raw, &list); err != nil {
		return fmt.Errorf("mcp events client: events/list: invalid result: %w", err)
	}

	var wg sync.WaitGroup
	for _, d := range list.Events {
		if d.Name == "" {
			continue
		}
		sub := &eventSubscriber{
			sess:      sess,
			name:      d.Name,
			heartbeat: opts.Heartbeat,
			timing:    defaultWatchTiming(),
			dedup:     newEventDedup(dedupWindow),
			log:       log.With("event", d.Name),
		}
		var run func(context.Context)
		switch {
		case slices.Contains(d.Delivery, "push"):
			run = sub.runPush
		case slices.Contains(d.Delivery, "poll"):
			run = sub.runPoll
		default:
			sub.log.Info("mcp.events_client.skip", "delivery", d.Delivery, "reason", "no poll or push delivery")
			continue
		}
		wg.Go(func() { run(runCtx) })
	}
	wg.Wait()
	return nil
}

// eventSubscriber is one subscription of RunEventsClient. It runs in one
// goroutine; its fields are not shared.
type eventSubscriber struct {
	sess      *connSession
	name      string
	heartbeat time.Duration
	timing    watchTiming
	dedup     *eventDedup
	log       *slog.Logger

	cursor        *string
	backoff       time.Duration
	failedStreams int
	pollUntil     time.Time
	streamOff     bool
}

func (s *eventSubscriber) opts() RawOpts { return RawOpts{Events: true} }

// persistCursor keeps a non-empty cursor. A null or absent cursor leaves the
// stored cursor as it is, as the task watcher does.
func (s *eventSubscriber) persistCursor(c *string) {
	if c = nonEmptyCursor(c); c != nil {
		s.cursor = c
	}
}

// onEvents logs the events that were not seen before on this subscription.
func (s *eventSubscriber) onEvents(events []EventOccurrence) {
	for i := range events {
		if events[i].EventID != "" && s.dedup.firstSeen(events[i].EventID) {
			s.log.Info("mcp.events_client.event", "event_id", events[i].EventID)
		}
	}
}

// runPush keeps an events/stream open. It uses events/poll while the
// stream is not usable, like the task watcher.
func (s *eventSubscriber) runPush(ctx context.Context) {
	for ctx.Err() == nil {
		var retry bool
		if s.streamOff || time.Now().Before(s.pollUntil) {
			retry = s.pollOnce(ctx)
		} else {
			retry = s.streamOnce(ctx)
		}
		if retry && !sleepCtx(ctx, s.nextBackoff()) {
			return
		}
	}
}

// runPoll polls until ctx ends.
func (s *eventSubscriber) runPoll(ctx context.Context) {
	s.streamOff = true
	for ctx.Err() == nil {
		if s.pollOnce(ctx) && !sleepCtx(ctx, s.nextBackoff()) {
			return
		}
	}
}

// pollOnce polls with the cursor until ctx ends, a request fails, or the
// stream may be tried again. It reports whether the caller must back off.
func (s *eventSubscriber) pollOnce(ctx context.Context) (retry bool) {
	for {
		res, err := eventsPoll(ctx, s.sess.client, s.name, nil, s.cursor, s.opts())
		switch {
		case ctx.Err() != nil:
			return false
		case err == nil:
		case errors.Is(err, ErrMethodNotFound), isFatalEventsError(err):
			s.log.Warn("mcp.events_client.poll_refused", "error", err.Error(), "next", "stop")
			<-ctx.Done()
			return false
		default:
			s.log.Info("mcp.events_client.request_failed", "error", err.Error())
			return true
		}
		s.backoff = 0
		s.persistCursor(res.Cursor)
		s.onEvents(res.Events)
		if res.Truncated {
			s.log.Info("mcp.events_client.truncated", "next", "continue from the fresh cursor")
		}
		if !s.streamOff && !time.Now().Before(s.pollUntil) {
			return false
		}
		wait := clampDuration(time.Duration(res.NextPollMs)*time.Millisecond, s.timing.eventsPollFloor, s.timing.eventsPollCap)
		if res.HasMore {
			wait = 0
		}
		if !sleepCtx(ctx, wait) {
			return false
		}
	}
}

// streamOnce keeps one events/stream open until it ends. It reports whether
// the caller must back off before the next attempt.
func (s *eventSubscriber) streamOnce(ctx context.Context) (retry bool) {
	st, err := openEventStream(ctx, s.sess, s.name, nil, s.cursor, s.opts())
	if err != nil {
		s.log.Warn("mcp.events_client.stream_open_failed", "error", err.Error())
		return s.streamFailed(false)
	}
	defer st.Close()

	silence := 2 * s.heartbeat
	watchdog := time.NewTimer(silence)
	defer watchdog.Stop()
	gotFrame := false

	for {
		select {
		case <-ctx.Done():
			return false
		case f := <-st.Frames:
			resetTimer(watchdog, silence)
			if !gotFrame {
				gotFrame = true
				s.failedStreams = 0
				s.backoff = 0
			}
			if stop := s.handleFrame(ctx, f); stop {
				return false
			}
		case err := <-st.Done:
			for drained := false; !drained; {
				select {
				case f := <-st.Frames:
					if stop := s.handleFrame(ctx, f); stop {
						return false
					}
				default:
					drained = true
				}
			}
			return s.streamEnded(ctx, err, gotFrame)
		case <-watchdog.C:
			s.log.Warn("mcp.events_client.stream_silent", "silence", silence.String(), "next", "reconnect with cursor")
			return s.streamFailed(gotFrame)
		}
	}
}

// handleFrame applies one stream frame. It reports whether the subscription
// ended.
func (s *eventSubscriber) handleFrame(ctx context.Context, f streamFrame) (stop bool) {
	switch f.Kind {
	case frameActive:
		s.persistCursor(f.Cursor)
		s.log.Info("mcp.events_client.stream_active", "truncated", f.Truncated)
	case frameHeartbeat:
		s.persistCursor(f.Cursor)
	case frameEvent:
		s.persistCursor(f.Cursor)
		s.onEvents([]EventOccurrence{*f.Event})
	case frameError:
		if f.Error != nil {
			s.log.Info("mcp.events_client.stream_error", "code", f.Error.Code)
		}
	case frameTerminated:
		code := 0
		if f.Error != nil {
			code = f.Error.Code
		}
		s.log.Warn("mcp.events_client.stream_terminated", "code", code, "next", "stop")
		<-ctx.Done()
		return true
	}
	return false
}

// streamEnded classifies the end of an events/stream request, like the task
// watcher.
func (s *eventSubscriber) streamEnded(ctx context.Context, err error, gotFrame bool) (retry bool) {
	switch {
	case ctx.Err() != nil:
		return false
	case err == nil:
		return true
	case errors.Is(err, ErrMethodNotFound):
		s.log.Info("mcp.events_client.stream_unsupported", "next", "events/poll")
		s.streamOff = true
		return false
	case isFatalEventsError(err):
		s.log.Warn("mcp.events_client.events_refused", "error", err.Error(), "next", "stop")
		<-ctx.Done()
		return false
	}
	s.log.Info("mcp.events_client.stream_dropped", "error", err.Error())
	return s.streamFailed(gotFrame)
}

// streamFailed counts a failed stream attempt. After too many attempts in a
// row with no frame, the subscriber polls for a while.
func (s *eventSubscriber) streamFailed(gotFrame bool) (retry bool) {
	if !gotFrame {
		s.failedStreams++
	}
	if s.failedStreams >= s.timing.streamFailures {
		s.failedStreams = 0
		s.pollUntil = time.Now().Add(s.timing.streamRetryAfter)
		s.log.Info("mcp.events_client.stream_fallback", "next", "events/poll", "for", s.timing.streamRetryAfter.String())
		return false
	}
	return true
}

func (s *eventSubscriber) nextBackoff() time.Duration {
	if s.backoff == 0 {
		s.backoff = s.timing.backoffBase
	} else {
		s.backoff = min(2*s.backoff, s.timing.backoffMax)
	}
	return s.backoff
}

// sleepCtx waits d and reports false when ctx ended.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
