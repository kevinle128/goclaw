package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
	"github.com/nextlevelbuilder/goclaw/internal/agent"
	"github.com/nextlevelbuilder/goclaw/internal/scheduler"
)

// ACP wake reasons that describe a sub-agent result.
const (
	acpWakeReasonCompleted = "completed"
	acpWakeReasonFailed    = "failed"
	acpWakeReasonCancelled = "cancelled"
)

// deliverAnnounceToACPThread sends the result of a sub-agent announce run to
// the thread of an ACP client. The announce run has no host tools of the
// client, so it cannot post by itself, and its response text does not reach
// the thread. A wake starts a turn of the client in that thread; the agent
// then posts the result with its own host tools. It reports true when the
// client queued the wake, so the caller must not publish the text again.
// It reports false for a session that is not an ACP session, or when the
// wake did not go through; the caller then keeps the normal delivery.
func deliverAnnounceToACPThread(ctx context.Context, waker *acpbridge.ResultWaker, r subagentAnnounceRouting, entries []subagentAnnounceEntry, outcome scheduler.RunOutcome) bool {
	if errors.Is(outcome.Err, context.Canceled) {
		return false
	}
	origin, ok := waker.Origin(r.SessionKey)
	if !ok || !origin.Wake {
		return false
	}
	summary := ""
	if outcome.Err == nil && outcome.Result != nil && !agent.IsSilentReply(outcome.Result.Content) {
		summary = outcome.Result.Content
	}
	prompt := buildSubagentWakePrompt(entries, summary)
	delivered := waker.Wake(ctx, origin, subagentWakeID(r.SessionKey, entries), subagentWakeReason(entries), prompt)
	if !delivered {
		slog.Warn("subagent announce: ACP wake not delivered, result stays in the session",
			"session", r.SessionKey, "batch_size", len(entries))
	}
	return delivered
}

// subagentWakeID gives each announce batch its own wake id. The client keeps
// accepted ids for a week and drops a repeat, so the id must not repeat for
// a later batch of the same session.
func subagentWakeID(sessionKey string, entries []subagentAnnounceEntry) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%d", sessionKey, time.Now().UnixNano())
	for _, e := range entries {
		fmt.Fprintf(h, "|%s|%s", e.Label, e.Status)
	}
	return "w_sub_" + hex.EncodeToString(h.Sum(nil))[:32]
}

// subagentWakeReason maps the statuses of a batch to a wake reason.
func subagentWakeReason(entries []subagentAnnounceEntry) string {
	failed, cancelled := 0, 0
	for _, e := range entries {
		switch e.Status {
		case "failed":
			failed++
		case "cancelled":
			cancelled++
		}
	}
	switch {
	case failed == len(entries):
		return acpWakeReasonFailed
	case cancelled == len(entries):
		return acpWakeReasonCancelled
	}
	return acpWakeReasonCompleted
}

// buildSubagentWakePrompt writes the prompt of the wake turn. The client
// allows 2 KiB. The prompt carries the summary of the announce run, or the
// raw results when that run gave none, cut to fit; the session history holds
// the same summary in full.
func buildSubagentWakePrompt(entries []subagentAnnounceEntry, summary string) string {
	var head strings.Builder
	head.WriteString("[goclaw sub-agent update] ")
	if len(entries) == 1 {
		fmt.Fprintf(&head, "Sub-agent task %q is %s.", cutRunes(entries[0].Label, 80), entries[0].Status)
	} else {
		fmt.Fprintf(&head, "%d sub-agent tasks finished.", len(entries))
	}
	head.WriteString(" Post the result below in this thread now with your host tool (buzz messages send). Do not use the message tool.\n\nResult:\n")

	body := strings.TrimSpace(summary)
	if body == "" {
		var raw strings.Builder
		for _, e := range entries {
			fmt.Fprintf(&raw, "- %s (%s): %s\n", cutRunes(e.Label, 80), e.Status, strings.TrimSpace(e.Content))
		}
		body = strings.TrimSpace(raw.String())
	}
	room := acpbridge.MaxResultWakePromptBytes - head.Len()
	return head.String() + cutBytes(body, room)
}

// cutBytes cuts s to at most limit bytes on a rune boundary and marks the cut.
func cutBytes(s string, limit int) string {
	const mark = "... [cut]"
	if limit <= 0 {
		return ""
	}
	if len(s) <= limit {
		return s
	}
	if limit <= len(mark) {
		return ""
	}
	end := limit - len(mark)
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end] + mark
}

func cutRunes(s string, limit int) string {
	if utf8.RuneCountInString(s) <= limit {
		return s
	}
	return string([]rune(s)[:limit]) + "..."
}
