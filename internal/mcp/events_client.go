package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	mcpclient "github.com/mark3labs/mcp-go/client"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// Notification methods of an events/stream (events extension).
const (
	notifEventsActive     = "notifications/events/active"
	notifEventsEvent      = "notifications/events/event"
	notifEventsHeartbeat  = "notifications/events/heartbeat"
	notifEventsError      = "notifications/events/error"
	notifEventsTerminated = "notifications/events/terminated"
)

// bootstrapPollTimeout bounds the events/poll that goclaw sends before a
// tools/call to get a cursor from "now".
const bootstrapPollTimeout = 10 * time.Second

// EventOccurrence is one event of the events extension. Data is untrusted
// server content.
type EventOccurrence struct {
	EventID   string          `json:"eventId"`
	Name      string          `json:"name"`
	Timestamp string          `json:"timestamp,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	Cursor    *string         `json:"cursor,omitempty"`
}

// eventsPollResult is the result of events/poll.
type eventsPollResult struct {
	Events     []EventOccurrence `json:"events"`
	Cursor     *string           `json:"cursor"`
	Truncated  bool              `json:"truncated"`
	HasMore    bool              `json:"hasMore"`
	NextPollMs int64             `json:"nextPollMs"`
}

// eventsPoll sends events/poll. A nil cursor asks for events from now.
func eventsPoll(ctx context.Context, client *mcpclient.Client, name string, args json.RawMessage, cursor *string, opts RawOpts) (*eventsPollResult, error) {
	params := map[string]any{"name": name, "arguments": subscriptionArgs(args), "cursor": cursor}
	raw, err := rawCall(ctx, client, methodEventsPoll, params, opts)
	if err != nil {
		return nil, err
	}
	var res eventsPollResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("mcp events/poll: invalid result: %w", err)
	}
	return &res, nil
}

// subscriptionArgs returns args, or {} when it is empty or null.
func subscriptionArgs(args json.RawMessage) json.RawMessage {
	if len(args) == 0 || string(args) == "null" {
		return json.RawMessage(`{}`)
	}
	return args
}

// bootstrapCursor sends events/poll with a null cursor to get the cursor of
// "now" before a tools/call. A failure gives a nil cursor; the watcher then
// starts without a cursor and reads the state with tasks/get.
func bootstrapCursor(ctx context.Context, client *mcpclient.Client, b *TaskEventsBinding, opts RawOpts) *string {
	opts.ID = ""
	opts.Events = true
	opts.Timeout = bootstrapPollTimeout
	res, err := eventsPoll(ctx, client, b.Event, b.BootstrapArguments, nil, opts)
	if err != nil {
		slog.Warn("mcp.events.bootstrap_failed", "event", b.Event, "error", err.Error())
		return nil
	}
	return nonEmptyCursor(res.Cursor)
}

// nonEmptyCursor returns c when it holds a non-empty string, and nil
// otherwise. A null or absent cursor is never stored.
func nonEmptyCursor(c *string) *string {
	if c == nil || *c == "" {
		return nil
	}
	v := *c
	return &v
}

// streamFrameKind is the kind of one events/stream notification.
type streamFrameKind int

const (
	frameActive streamFrameKind = iota + 1
	frameEvent
	frameHeartbeat
	frameError
	frameTerminated
)

// streamFrame is one decoded notification of an events/stream.
type streamFrame struct {
	Kind      streamFrameKind
	Cursor    *string
	Truncated bool
	Event     *EventOccurrence
	Error     *TaskError
}

// decodeStreamFrame decodes a notifications/events/* notification. Unknown
// methods give ok=false and are ignored.
func decodeStreamFrame(n mcpgo.JSONRPCNotification) (streamFrame, bool) {
	var kind streamFrameKind
	switch n.Method {
	case notifEventsActive:
		kind = frameActive
	case notifEventsEvent:
		kind = frameEvent
	case notifEventsHeartbeat:
		kind = frameHeartbeat
	case notifEventsError:
		kind = frameError
	case notifEventsTerminated:
		kind = frameTerminated
	default:
		return streamFrame{}, false
	}
	raw, err := json.Marshal(n.Params)
	if err != nil {
		return streamFrame{}, false
	}
	var p struct {
		Cursor    *string    `json:"cursor"`
		Truncated bool       `json:"truncated"`
		Error     *TaskError `json:"error"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return streamFrame{}, false
	}
	f := streamFrame{Kind: kind, Cursor: p.Cursor, Truncated: p.Truncated, Error: p.Error}
	if kind == frameEvent {
		var ev EventOccurrence
		if err := json.Unmarshal(raw, &ev); err != nil || ev.EventID == "" {
			return streamFrame{}, false
		}
		f.Event = &ev
		f.Cursor = ev.Cursor
	}
	return f, true
}

// frameBuffer is the size of the frame channel of one stream.
const frameBuffer = 64

// eventStream is one open events/stream request.
type eventStream struct {
	// Frames receives the decoded notifications of this stream.
	Frames <-chan streamFrame
	// Done receives the result of the request once: nil when the server
	// sent the final response, an error otherwise (also a drop).
	Done   <-chan error
	cancel context.CancelFunc
}

// Close aborts the stream request (the client cancels a stream by closing
// the request).
func (s *eventStream) Close() { s.cancel() }

// openEventStream sends events/stream on sess and returns at once. The
// subscription id is registered in the router of sess before the request
// goes out, so no frame is lost. The stream ends when ctx ends, Close is
// called, or the server ends the response.
func openEventStream(ctx context.Context, sess *connSession, name string, args json.RawMessage, cursor *string, opts RawOpts) (*eventStream, error) {
	if sess == nil || sess.client == nil || sess.router == nil {
		return nil, errors.New("mcp events/stream: no connection")
	}
	streamCtx, cancel := context.WithCancel(ctx)
	frames := make(chan streamFrame, frameBuffer)
	done := make(chan error, 1)

	id := nextRawRequestID()
	unregister, err := sess.router.Register(id, func(n mcpgo.JSONRPCNotification) {
		f, ok := decodeStreamFrame(n)
		if !ok {
			return
		}
		// The handler runs on the reader goroutine of this request only;
		// waiting here applies back pressure to this stream and ends with
		// the stream context.
		select {
		case frames <- f:
		case <-streamCtx.Done():
		}
	})
	if err != nil {
		cancel()
		return nil, err
	}

	opts.ID = id
	opts.Events = true
	params := map[string]any{"name": name, "arguments": subscriptionArgs(args), "cursor": cursor}
	go func() {
		_, err := rawCall(streamCtx, sess.client, methodEventsStream, params, opts)
		unregister()
		done <- err
	}()
	return &eventStream{Frames: frames, Done: done, cancel: cancel}, nil
}

// eventDedup remembers the last eventIDs of one subscription.
type eventDedup struct {
	seen  map[string]struct{}
	order []string
	limit int
}

func newEventDedup(limit int) *eventDedup {
	return &eventDedup{seen: make(map[string]struct{}, limit), limit: limit}
}

// firstSeen records id and reports whether it is new.
func (d *eventDedup) firstSeen(id string) bool {
	if _, ok := d.seen[id]; ok {
		return false
	}
	d.seen[id] = struct{}{}
	d.order = append(d.order, id)
	if len(d.order) > d.limit {
		delete(d.seen, d.order[0])
		d.order = d.order[1:]
	}
	return true
}

// eventPRURL reports whether the event data has a non-empty string field
// pr_url. Only the presence is read; the value is untrusted.
func eventPRURL(ev *EventOccurrence) bool {
	if ev == nil || len(ev.Data) == 0 {
		return false
	}
	var data struct {
		PRURL any `json:"pr_url"`
	}
	if err := json.Unmarshal(ev.Data, &data); err != nil {
		return false
	}
	s, ok := data.PRURL.(string)
	return ok && s != ""
}
