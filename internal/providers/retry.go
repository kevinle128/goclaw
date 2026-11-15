package providers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RetryConfig configures retry behavior for provider requests.
type RetryConfig struct {
	Attempts int           // max attempts (default 3, 1 = no retry)
	MinDelay time.Duration // initial delay (default 300ms)
	MaxDelay time.Duration // delay cap (default 30s)
	Jitter   float64       // jitter factor ±N (default 0.1 = ±10%)

	// Rate-limit waiting: when the provider reports a rate limit together with a
	// wait time, RetryDo waits that long and retries the same request. Zero
	// values disable the feature (RateLimitMaxAttempts <= 0).
	RateLimitMaxWait      time.Duration // longest single wait; a longer reported wait fails the call
	RateLimitMaxAttempts  int           // max rate-limit waits per call
	RateLimitMaxTotalWait time.Duration // max sum of rate-limit waits per call

	// Sleep waits for d or until ctx ends. Nil uses a timer. Tests inject a fake.
	Sleep func(ctx context.Context, d time.Duration) error
}

// RetryHookFunc is called before each retry attempt.
// attempt is the failed attempt number (1-based), maxAttempts is the total.
type RetryHookFunc func(attempt, maxAttempts int, err error)

type retryHookKey struct{}

// WithRetryHook injects a retry notification callback into the context.
// RetryDo will call this hook before each retry attempt.
func WithRetryHook(ctx context.Context, fn RetryHookFunc) context.Context {
	return context.WithValue(ctx, retryHookKey{}, fn)
}

// retryHookFromContext returns the retry hook from context, or nil.
func retryHookFromContext(ctx context.Context) RetryHookFunc {
	fn, _ := ctx.Value(retryHookKey{}).(RetryHookFunc)
	return fn
}

// DefaultRetryConfig returns sensible defaults matching TS provider retry behavior.
func DefaultRetryConfig() RetryConfig {
	cfg := RetryConfig{
		Attempts: 3,
		MinDelay: 300 * time.Millisecond,
		MaxDelay: 30 * time.Second,
		Jitter:   0.1,
	}
	applyRateLimitDefaults(&cfg)
	return cfg
}

// HTTPError represents an HTTP error with status code and optional Retry-After.
type HTTPError struct {
	Status     int
	Body       string
	RetryAfter time.Duration // parsed from Retry-After header (0 if absent)
	Header     http.Header   // response headers (nil when unavailable); read for rate-limit reset hints
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body)
}

// IsRetryableError checks if an error is retryable.
// Retryable: 429 (rate limit), 500, 502, 503, 504, connection errors, timeouts.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for HTTPError
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.Status {
		case 429, 500, 502, 503, 504:
			return true
		}
		return false
	}

	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true // includes timeouts
	}

	// Check for connection reset / broken pipe / EOF in error string.
	// Some streaming providers (notably Codex/Responses) surface transient
	// backend failures as SSE `response.failed` events instead of HTTP 429/5xx.
	// Treat explicit retry guidance/rate-limit wording as retryable too.
	errStr := err.Error()
	lowerErr := strings.ToLower(errStr)
	if strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(lowerErr, "timeout") ||
		strings.Contains(lowerErr, "rate limit") ||
		strings.Contains(lowerErr, "429") ||
		(strings.Contains(lowerErr, "response failed") && strings.Contains(lowerErr, "retry")) ||
		strings.Contains(lowerErr, "while processing your request") {
		return true
	}

	return false
}

// RetryDo executes fn with retry logic using exponential backoff and jitter.
func RetryDo[T any](ctx context.Context, cfg RetryConfig, fn func() (T, error)) (T, error) {
	if cfg.Attempts <= 0 {
		cfg.Attempts = 1
	}

	var lastErr error
	var zero T
	var budget rateLimitBudget

	for attempt := 1; attempt <= cfg.Attempts; attempt++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err

		// A rate limit that reports its wait time is waited out and the same
		// request is retried. It does not use up a regular attempt.
		wait, action := budget.next(cfg, err)
		switch action {
		case rateLimitWait:
			if sleepErr := budget.wait(ctx, cfg, wait, err); sleepErr != nil {
				return zero, sleepErr
			}
			attempt--
			continue
		case rateLimitGiveUp:
			return zero, err
		}

		// Don't retry if not retryable or last attempt
		if !IsRetryableError(err) || attempt == cfg.Attempts {
			return zero, err
		}

		// Compute delay
		delay := computeDelay(cfg, attempt, err)

		slog.Debug("provider retry",
			"attempt", attempt,
			"maxAttempts", cfg.Attempts,
			"delay", delay,
			"error", err.Error(),
		)

		// Notify retry hook (for placeholder updates, etc.)
		if hook := retryHookFromContext(ctx); hook != nil {
			hook(attempt, cfg.Attempts, err)
		}

		// Wait with context cancellation support
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(delay):
		}
	}

	return zero, lastErr
}

// computeDelay calculates the retry delay with exponential backoff, jitter, and Retry-After support.
func computeDelay(cfg RetryConfig, attempt int, err error) time.Duration {
	// Check for Retry-After header
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.RetryAfter > 0 {
		return httpErr.RetryAfter
	}

	// Exponential backoff: minDelay * 2^(attempt-1)
	delay := float64(cfg.MinDelay) * math.Pow(2, float64(attempt-1))

	// Cap at maxDelay
	if time.Duration(delay) > cfg.MaxDelay {
		delay = float64(cfg.MaxDelay)
	}

	// Apply jitter: ±jitter%
	if cfg.Jitter > 0 {
		jitterRange := delay * cfg.Jitter
		delay += (rand.Float64()*2 - 1) * jitterRange
	}

	if delay < 0 {
		delay = float64(cfg.MinDelay)
	}

	return time.Duration(delay)
}

// ParseRetryAfter parses a Retry-After header value (seconds or HTTP-date).
func ParseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}

	// Try integer seconds first
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}

	// Try HTTP-date format: IMF-fixdate, RFC 850 and asctime forms, then plain RFC 1123.
	t, err := http.ParseTime(value)
	if err != nil {
		t, err = time.Parse(time.RFC1123, value)
	}
	if err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}

	return 0
}
