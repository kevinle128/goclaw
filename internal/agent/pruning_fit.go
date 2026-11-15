package agent

import (
	"fmt"
	"unicode/utf8"

	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/tokencount"
)

const (
	// fitHeadChars + fitTailChars is the size a tool result or an old message
	// body is cut to when the deterministic fit runs.
	fitHeadChars = 2000
	fitTailChars = 2000
	// fitTargetPercent leaves room for drift between the per-message sum used
	// here and the whole-list count PruneStage makes afterwards.
	fitTargetPercent = 90
)

// fitHistoryToBudget reduces history without an LLM call. Soft pruning only
// touches tool results older than the last assistants, and compaction cannot
// split a history that is one long tool chain, so a session with few but huge
// messages can stay over budget after both. Each pass works oldest first and
// stops as soon as the history fits:
//  1. cut every tool result to head + tail;
//  2. replace tool results with the placeholder;
//  3. cut user/assistant bodies to head + tail, except the newest user message;
//  4. drop the oldest messages, starting the kept part at a user message.
//
// The newest user message is never changed or dropped. Tool call pairing is
// repaired at the end with sanitizeHistory.
func fitHistoryToBudget(msgs []providers.Message, budget int, tc tokencount.TokenCounter, model, placeholder string) ([]providers.Message, pipeline.FitStats) {
	var stats pipeline.FitStats
	if budget <= 0 || len(msgs) == 0 {
		return msgs, stats
	}
	target := budget * fitTargetPercent / 100

	count := func(m providers.Message) int {
		if tc != nil {
			return tc.CountMessages(model, []providers.Message{m})
		}
		return utf8.RuneCountInString(m.Content)/charsPerTokenEstimate + 4
	}

	out := make([]providers.Message, len(msgs))
	copy(out, msgs)
	tokens := make([]int, len(out))
	total := 0
	for i, m := range out {
		tokens[i] = count(m)
		total += tokens[i]
	}
	if total <= target {
		return msgs, stats
	}

	lastUser := -1
	for i := len(out) - 1; i >= 0; i-- {
		if out[i].Role == "user" {
			lastUser = i
			break
		}
	}

	replace := func(i int, m providers.Message) {
		out[i] = m
		n := count(m)
		total += n - tokens[i]
		tokens[i] = n
	}

	// Pass 1: cut tool results to head + tail.
	for i := 0; i < len(out) && total > target; i++ {
		m := out[i]
		if m.Role != "tool" || utf8.RuneCountInString(m.Content) <= fitHeadChars+fitTailChars {
			continue
		}
		m.Content = cutMiddle(m.Content, "Tool result trimmed")
		replace(i, m)
		stats.ResultsTrimmed++
	}

	// Pass 2: replace tool results with the placeholder.
	for i := 0; i < len(out) && total > target; i++ {
		m := out[i]
		if m.Role != "tool" || m.Content == placeholder {
			continue
		}
		m.Content = placeholder
		replace(i, m)
		stats.ResultsCleared++
	}

	// Pass 3: cut old user/assistant bodies.
	for i := 0; i < len(out) && total > target; i++ {
		m := out[i]
		if i == lastUser || (m.Role != "user" && m.Role != "assistant") ||
			utf8.RuneCountInString(m.Content) <= fitHeadChars+fitTailChars {
			continue
		}
		m.Content = cutMiddle(m.Content, "Message trimmed")
		replace(i, m)
		stats.BodiesTrimmed++
	}

	// Pass 4: drop the oldest messages. The kept part starts at a user message
	// so it never begins inside a tool chain.
	if total > target && lastUser > 0 {
		start := lastUser
		suffix := 0
		for i := lastUser; i < len(out); i++ {
			suffix += tokens[i]
		}
		for i := lastUser - 1; i > 0; i-- {
			suffix += tokens[i]
			if suffix > target {
				break
			}
			if out[i].Role == "user" {
				start = i
			}
		}
		stats.Dropped = start
		out = out[start:]
	}

	out, _ = sanitizeHistory(out)
	return out, stats
}

// cutMiddle keeps the first fitHeadChars and the last fitTailChars runes of s.
func cutMiddle(s, label string) string {
	return fmt.Sprintf("%s\n...\n%s\n\n[%s: kept first %d chars and last %d chars of %d chars.]",
		takeHead(s, fitHeadChars), takeTail(s, fitTailChars), label,
		fitHeadChars, fitTailChars, utf8.RuneCountInString(s))
}
