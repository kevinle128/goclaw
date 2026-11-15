package providers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Defaults for waiting out a provider rate limit. Override with the
// GOCLAW_LLM_RATE_LIMIT_* environment variables (read by DefaultRetryConfig).
const (
	defaultRateLimitMaxWait      = 120 * time.Second
	defaultRateLimitMaxAttempts  = 5
	defaultRateLimitMaxTotalWait = 300 * time.Second

	// rateLimitWaitMargin is added to the provider's reported wait so the retry
	// lands just after the limit resets instead of racing it.
	rateLimitWaitMargin = time.Second
)

const (
	envRateLimitMaxWait      = "GOCLAW_LLM_RATE_LIMIT_MAX_WAIT"
	envRateLimitMaxAttempts  = "GOCLAW_LLM_RATE_LIMIT_MAX_ATTEMPTS"
	envRateLimitMaxTotalWait = "GOCLAW_LLM_RATE_LIMIT_MAX_TOTAL_WAIT"
)

// applyRateLimitDefaults fills the rate-limit fields of cfg from the defaults,
// then from the environment. An invalid environment value keeps the default.
func applyRateLimitDefaults(cfg *RetryConfig) {
	cfg.RateLimitMaxWait = envDuration(envRateLimitMaxWait, defaultRateLimitMaxWait)
	cfg.RateLimitMaxTotalWait = envDuration(envRateLimitMaxTotalWait, defaultRateLimitMaxTotalWait)
	cfg.RateLimitMaxAttempts = defaultRateLimitMaxAttempts
	if raw := os.Getenv(envRateLimitMaxAttempts); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			cfg.RateLimitMaxAttempts = n
		} else {
			slog.Warn("providers.rate_limit_env_invalid", "name", envRateLimitMaxAttempts)
		}
	}
}

func envDuration(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		slog.Warn("providers.rate_limit_env_invalid", "name", name)
		return def
	}
	return d
}

type retryLabelKey struct{}

type retryLabels struct{ provider, model string }

// WithRetryLabels attaches the provider and model names to ctx so rate-limit
// retries can be logged with them. RetryDo itself does not know either.
func WithRetryLabels(ctx context.Context, provider, model string) context.Context {
	return context.WithValue(ctx, retryLabelKey{}, retryLabels{provider: provider, model: model})
}

func retryLabelsFromContext(ctx context.Context) retryLabels {
	l, _ := ctx.Value(retryLabelKey{}).(retryLabels)
	return l
}

// sleepCtx waits for d or until ctx ends, whichever comes first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var status429Re = regexp.MustCompile(`(?:^|\D)429(?:\D|$)`)

// isRateLimitError reports whether err is a provider rate limit: HTTP 429, or a
// 5xx whose body shows that an upstream 429 was wrapped (for example a router
// that answers 503 with "[429]: Rate limit exceeded" inside).
func isRateLimitError(err error) bool {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	switch httpErr.Status {
	case http.StatusTooManyRequests:
		return true
	case 500, 502, 503, 504:
		lower := strings.ToLower(httpErr.Body)
		return strings.Contains(lower, "rate limit") ||
			strings.Contains(lower, "rate_limit") ||
			status429Re.MatchString(lower)
	}
	return false
}

// rateLimitResetHeaders are checked in order after Retry-After; the longest value wins.
var rateLimitResetHeaders = []string{
	"X-Ratelimit-Reset",
	"X-Ratelimit-Reset-Requests",
	"X-Ratelimit-Reset-Tokens",
	"Ratelimit-Reset",
}

var (
	// Compound or single duration such as "8s", "500ms", "1m30s".
	bodyDurationRe = regexp.MustCompile(
		`(?i)(?:reset(?:s)?\s+(?:after|in)|retry\s+after|retry-after|try\s+again\s+in|retry\s+in)\s*[:=]?\s*` +
			`((?:\d+(?:\.\d+)?(?:ms|h|m|s))+)`)
	// Number with a spelled-out or spaced unit such as "3 seconds", "2 minutes".
	bodyWordUnitRe = regexp.MustCompile(
		`(?i)(?:reset(?:s)?\s+(?:after|in)|retry\s+after|retry-after|try\s+again\s+in|retry\s+in)\s*[:=]?\s*` +
			`(\d+(?:\.\d+)?)\s*(milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|h)\b`)
)

// parseRateLimitWait extracts the wait the provider asked for. Order: the
// Retry-After header, the x-ratelimit-reset* headers, then the body text.
// The bool is false when no wait could be found.
func parseRateLimitWait(err error) (time.Duration, bool) {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return 0, false
	}

	if httpErr.RetryAfter > 0 {
		return httpErr.RetryAfter, true
	}
	if httpErr.Header != nil {
		if d := ParseRetryAfter(httpErr.Header.Get("Retry-After")); d > 0 {
			return d, true
		}
		var longest time.Duration
		for _, name := range rateLimitResetHeaders {
			if d, ok := parseResetValue(httpErr.Header.Get(name)); ok && d > longest {
				longest = d
			}
		}
		if longest > 0 {
			return longest, true
		}
	}
	return parseWaitFromBody(httpErr.Body)
}

// parseResetValue reads a reset header: seconds ("12", "1.5"), a Go-style
// duration ("8s", "1m30s", "500ms"), or an epoch timestamp in seconds.
func parseResetValue(v string) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
		if f > 1e9 { // epoch seconds, not a relative wait
			d := time.Until(time.Unix(int64(f), 0))
			return d, d > 0
		}
		return time.Duration(f * float64(time.Second)), f > 0
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d, true
	}
	return 0, false
}

func parseWaitFromBody(body string) (time.Duration, bool) {
	if m := bodyDurationRe.FindStringSubmatch(body); m != nil {
		if d, err := time.ParseDuration(strings.ToLower(m[1])); err == nil {
			return d, true
		}
	}
	if m := bodyWordUnitRe.FindStringSubmatch(body); m != nil {
		n, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, false
		}
		unit := strings.ToLower(m[2])
		var base time.Duration
		switch {
		case strings.HasPrefix(unit, "ms") || strings.HasPrefix(unit, "milli"):
			base = time.Millisecond
		case strings.HasPrefix(unit, "h"):
			base = time.Hour
		case strings.HasPrefix(unit, "m"):
			base = time.Minute
		default:
			base = time.Second
		}
		return time.Duration(n * float64(base)), true
	}
	return 0, false
}

// rateLimitBudget tracks the rate-limit waits spent by one provider call.
type rateLimitBudget struct {
	waits int
	total time.Duration
}

// rateLimitAction is the verdict of rateLimitBudget.next.
type rateLimitAction int

const (
	// rateLimitNotApplicable: not a rate limit with a usable wait; use normal retry handling.
	rateLimitNotApplicable rateLimitAction = iota
	// rateLimitWait: wait the returned duration, then retry the same request.
	rateLimitWait
	// rateLimitGiveUp: a bound was exceeded; return the error without waiting.
	rateLimitGiveUp
)

// next decides how to handle a failed call that may be a rate limit. For
// rateLimitWait the duration is the reported wait plus the margin. When a bound
// is exceeded the call fails right away: waiting on the normal backoff would
// still honour the provider's long Retry-After, which the bounds forbid.
func (b *rateLimitBudget) next(cfg RetryConfig, err error) (time.Duration, rateLimitAction) {
	if cfg.Attempts <= 1 || cfg.RateLimitMaxAttempts <= 0 || !isRateLimitError(err) {
		return 0, rateLimitNotApplicable
	}
	reported, found := parseRateLimitWait(err)
	if !found {
		return 0, rateLimitNotApplicable
	}
	wait := reported + rateLimitWaitMargin
	var reason string
	switch {
	case cfg.RateLimitMaxWait > 0 && reported > cfg.RateLimitMaxWait:
		reason = "wait_above_max"
	case b.waits >= cfg.RateLimitMaxAttempts:
		reason = "max_attempts"
	case cfg.RateLimitMaxTotalWait > 0 && b.total+wait > cfg.RateLimitMaxTotalWait:
		reason = "max_total_wait"
	default:
		return wait, rateLimitWait
	}
	slog.Warn("llm.rate_limit_give_up", "reason", reason, "reported_wait", reported, "waits_done", b.waits, "waited_total", b.total)
	return 0, rateLimitGiveUp
}

// wait sleeps for the planned rate-limit wait and records it in the budget.
// A non-nil error is the context error and the caller must stop retrying.
func (b *rateLimitBudget) wait(ctx context.Context, cfg RetryConfig, d time.Duration, err error) error {
	b.waits++
	b.total += d
	labels := retryLabelsFromContext(ctx)
	slog.Warn("llm.rate_limited",
		"wait", d,
		"attempt", b.waits,
		"max_attempts", cfg.RateLimitMaxAttempts,
		"provider", labels.provider,
		"model", labels.model,
		"error", err.Error(),
	)
	if hook := retryHookFromContext(ctx); hook != nil {
		hook(b.waits, cfg.RateLimitMaxAttempts+1, err)
	}
	sleep := cfg.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	return sleep(ctx, d)
}
