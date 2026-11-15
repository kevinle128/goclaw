package providers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// router503Body is the response body the 9router gateway returned in production
// when the upstream Codex account was rate limited.
const router503Body = `9router: {"error":{"message":"[codex/gpt-6.1-sol] [429]: {\"detail\":\"Rate limit exceeded\"} (reset after 8s)"}}`

type fakeSleeper struct{ waits []time.Duration }

func (f *fakeSleeper) sleep(ctx context.Context, d time.Duration) error {
	f.waits = append(f.waits, d)
	return ctx.Err()
}

func rateLimitTestConfig(s *fakeSleeper) RetryConfig {
	return RetryConfig{
		Attempts:              3,
		MinDelay:              time.Millisecond,
		MaxDelay:              time.Millisecond,
		RateLimitMaxWait:      120 * time.Second,
		RateLimitMaxAttempts:  5,
		RateLimitMaxTotalWait: 300 * time.Second,
		Sleep:                 s.sleep,
	}
}

// failThenOK returns a fn that returns each error in turn, then succeeds.
func failThenOK(calls *int, errs ...error) func() (string, error) {
	return func() (string, error) {
		i := *calls
		*calls++
		if i < len(errs) {
			return "", errs[i]
		}
		return "ok", nil
	}
}

func TestRetryDo_RateLimitWrapped503WaitsReportedTime(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	got, err := RetryDo(context.Background(), rateLimitTestConfig(s),
		failThenOK(&calls, &HTTPError{Status: 503, Body: router503Body}))
	if err != nil || got != "ok" {
		t.Fatalf("got (%q, %v), want ok", got, err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (same request retried once)", calls)
	}
	if len(s.waits) != 1 || s.waits[0] != 9*time.Second {
		t.Fatalf("waits = %v, want [9s] (8s reported + 1s margin)", s.waits)
	}
}

func TestRetryDo_RateLimitRetryAfterHeaderSeconds(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	h := http.Header{}
	h.Set("Retry-After", "5")
	_, err := RetryDo(context.Background(), rateLimitTestConfig(s),
		failThenOK(&calls, &HTTPError{Status: 429, Body: "slow down", Header: h}))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.waits) != 1 || s.waits[0] != 6*time.Second {
		t.Fatalf("waits = %v, want [6s]", s.waits)
	}
}

func TestRetryDo_RateLimitRetryAfterParsedField(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	_, err := RetryDo(context.Background(), rateLimitTestConfig(s),
		failThenOK(&calls, &HTTPError{Status: 429, RetryAfter: 4 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.waits) != 1 || s.waits[0] != 5*time.Second {
		t.Fatalf("waits = %v, want [5s]", s.waits)
	}
}

func TestRetryDo_RateLimitRetryAfterHTTPDate(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	h := http.Header{}
	h.Set("Retry-After", time.Now().Add(30*time.Second).UTC().Format(http.TimeFormat))
	_, err := RetryDo(context.Background(), rateLimitTestConfig(s),
		failThenOK(&calls, &HTTPError{Status: 429, Header: h}))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.waits) != 1 || s.waits[0] < 29*time.Second || s.waits[0] > 32*time.Second {
		t.Fatalf("waits = %v, want about 31s (30s date + 1s margin)", s.waits)
	}
}

func TestRetryDo_RateLimitResetHeaders(t *testing.T) {
	cases := []struct {
		name, key, value string
		want             time.Duration
	}{
		{"duration seconds", "X-Ratelimit-Reset-Requests", "8s", 9 * time.Second},
		{"compound duration", "X-Ratelimit-Reset-Tokens", "1m30s", 91 * time.Second},
		{"plain seconds", "X-Ratelimit-Reset", "12", 13 * time.Second},
		{"milliseconds", "X-Ratelimit-Reset-Requests", "500ms", 1500 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeSleeper{}
			calls := 0
			h := http.Header{}
			h.Set(tc.key, tc.value)
			_, err := RetryDo(context.Background(), rateLimitTestConfig(s),
				failThenOK(&calls, &HTTPError{Status: 429, Header: h}))
			if err != nil {
				t.Fatal(err)
			}
			if len(s.waits) != 1 || s.waits[0] != tc.want {
				t.Fatalf("waits = %v, want [%v]", s.waits, tc.want)
			}
		})
	}
}

func TestRetryDo_RateLimitHeaderBeatsBody(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	h := http.Header{}
	h.Set("Retry-After", "2")
	_, err := RetryDo(context.Background(), rateLimitTestConfig(s),
		failThenOK(&calls, &HTTPError{Status: 503, Body: router503Body, Header: h}))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.waits) != 1 || s.waits[0] != 3*time.Second {
		t.Fatalf("waits = %v, want [3s] from the header, not 9s from the body", s.waits)
	}
}

func TestRetryDo_RateLimitBodyForms(t *testing.T) {
	cases := []struct {
		body string
		want time.Duration
	}{
		{"Rate limit exceeded (reset after 8s)", 9 * time.Second},
		{"rate limit hit, retry after 3 seconds", 4 * time.Second},
		{"429 Too Many Requests. Try again in 2s.", 3 * time.Second},
		{"rate_limit_error: retry in 500ms", 1500 * time.Millisecond},
		{"Rate limit reached. Please try again in 1m30s.", 91 * time.Second},
		{"rate limit: retry after 2 minutes", 121 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.body, func(t *testing.T) {
			s := &fakeSleeper{}
			calls := 0
			_, err := RetryDo(context.Background(), rateLimitTestConfig(s),
				failThenOK(&calls, &HTTPError{Status: 502, Body: tc.body}))
			if err != nil {
				t.Fatal(err)
			}
			if len(s.waits) != 1 || s.waits[0] != tc.want {
				t.Fatalf("waits = %v, want [%v]", s.waits, tc.want)
			}
		})
	}
}

func TestRetryDo_RateLimitWaitAboveCapFailsWithoutWaiting(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	h := http.Header{}
	h.Set("Retry-After", "600")
	orig := &HTTPError{Status: 429, Header: h}
	_, err := RetryDo(context.Background(), rateLimitTestConfig(s), failThenOK(&calls, orig))
	if !errors.Is(err, orig) {
		t.Fatalf("err = %v, want the original HTTP error", err)
	}
	if calls != 1 || len(s.waits) != 0 {
		t.Fatalf("calls=%d waits=%v, want one call and no wait", calls, s.waits)
	}
}

func TestRetryDo_RateLimitMaxAttempts(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	always := &HTTPError{Status: 429, RetryAfter: 2 * time.Second}
	errs := make([]error, 20)
	for i := range errs {
		errs[i] = always
	}
	_, err := RetryDo(context.Background(), rateLimitTestConfig(s), failThenOK(&calls, errs...))
	if err == nil {
		t.Fatal("want error after rate-limit attempts are exhausted")
	}
	if len(s.waits) != 5 || calls != 6 {
		t.Fatalf("waits=%d calls=%d, want 5 waits and 6 calls", len(s.waits), calls)
	}
}

func TestRetryDo_RateLimitMaxTotalWait(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	always := &HTTPError{Status: 429, RetryAfter: 99 * time.Second} // 100s each with margin
	errs := make([]error, 20)
	for i := range errs {
		errs[i] = always
	}
	_, err := RetryDo(context.Background(), rateLimitTestConfig(s), failThenOK(&calls, errs...))
	if err == nil {
		t.Fatal("want error once the total wait budget is spent")
	}
	if len(s.waits) != 3 {
		t.Fatalf("waits = %v, want 3 waits of 100s (a fourth would pass 300s)", s.waits)
	}
}

func TestRetryDo_RateLimitContextCancelInterruptsWait(t *testing.T) {
	cfg := rateLimitTestConfig(&fakeSleeper{})
	cfg.Sleep = nil // exercise the real timer-based wait
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	calls := 0
	start := time.Now()
	_, err := RetryDo(ctx, cfg, failThenOK(&calls, &HTTPError{Status: 503, Body: router503Body}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("cancel took %v, want prompt return instead of the 9s wait", elapsed)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestRetryDo_NonRateLimit503KeepsBackoff(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	unavailable := &HTTPError{Status: 503, Body: "upstream connect error: service unavailable"}
	_, err := RetryDo(context.Background(), rateLimitTestConfig(s),
		failThenOK(&calls, unavailable, unavailable, unavailable))
	if err == nil {
		t.Fatal("want error after the default 3 attempts")
	}
	if calls != 3 || len(s.waits) != 0 {
		t.Fatalf("calls=%d waits=%v, want 3 calls on the exponential backoff and no rate-limit wait", calls, s.waits)
	}
}

func TestRetryDo_RateLimitWithoutWaitKeepsBackoff(t *testing.T) {
	s := &fakeSleeper{}
	calls := 0
	limited := &HTTPError{Status: 429, Body: "too many requests"}
	got, err := RetryDo(context.Background(), rateLimitTestConfig(s), failThenOK(&calls, limited))
	if err != nil || got != "ok" {
		t.Fatalf("got (%q, %v), want ok", got, err)
	}
	if len(s.waits) != 0 {
		t.Fatalf("waits = %v, want none (no wait time reported)", s.waits)
	}
}

func TestRetryDo_RateLimitDisabledForSingleAttempt(t *testing.T) {
	s := &fakeSleeper{}
	cfg := rateLimitTestConfig(s)
	cfg.Attempts = 1
	calls := 0
	_, err := RetryDo(context.Background(), cfg, failThenOK(&calls, &HTTPError{Status: 429, RetryAfter: time.Second}))
	if err == nil || calls != 1 || len(s.waits) != 0 {
		t.Fatalf("err=%v calls=%d waits=%v, want a single failed call", err, calls, s.waits)
	}
}

func TestIsRateLimitError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"429", &HTTPError{Status: 429}, true},
		{"503 wrapping 429", &HTTPError{Status: 503, Body: router503Body}, true},
		{"502 rate limit text", &HTTPError{Status: 502, Body: "Rate Limit exceeded"}, true},
		{"500 rate_limit code", &HTTPError{Status: 500, Body: `{"code":"rate_limit"}`}, true},
		{"503 plain", &HTTPError{Status: 503, Body: "service unavailable"}, false},
		{"503 digits only", &HTTPError{Status: 503, Body: "request id 14290031"}, false},
		{"400 rate limit text", &HTTPError{Status: 400, Body: "rate limit"}, false},
		{"non http", errors.New("rate limit"), false},
	}
	for _, tc := range cases {
		if got := isRateLimitError(tc.err); got != tc.want {
			t.Errorf("%s: isRateLimitError = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDefaultRetryConfig_RateLimitDefaults(t *testing.T) {
	cfg := DefaultRetryConfig()
	if cfg.RateLimitMaxWait != 120*time.Second || cfg.RateLimitMaxAttempts != 5 || cfg.RateLimitMaxTotalWait != 300*time.Second {
		t.Fatalf("defaults = %v/%d/%v, want 120s/5/300s", cfg.RateLimitMaxWait, cfg.RateLimitMaxAttempts, cfg.RateLimitMaxTotalWait)
	}
}

func TestDefaultRetryConfig_RateLimitEnvOverride(t *testing.T) {
	t.Setenv("GOCLAW_LLM_RATE_LIMIT_MAX_WAIT", "45s")
	t.Setenv("GOCLAW_LLM_RATE_LIMIT_MAX_ATTEMPTS", "2")
	t.Setenv("GOCLAW_LLM_RATE_LIMIT_MAX_TOTAL_WAIT", "90s")
	cfg := DefaultRetryConfig()
	if cfg.RateLimitMaxWait != 45*time.Second || cfg.RateLimitMaxAttempts != 2 || cfg.RateLimitMaxTotalWait != 90*time.Second {
		t.Fatalf("env override = %v/%d/%v", cfg.RateLimitMaxWait, cfg.RateLimitMaxAttempts, cfg.RateLimitMaxTotalWait)
	}
	t.Setenv("GOCLAW_LLM_RATE_LIMIT_MAX_WAIT", "garbage")
	if got := DefaultRetryConfig().RateLimitMaxWait; got != 120*time.Second {
		t.Fatalf("invalid env must fall back to the default, got %v", got)
	}
}

// The OpenAI-compatible provider must retry the same request after the wait,
// for both the non-streaming and the streaming connection phase.
func newRateLimitedServer(t *testing.T, hits *atomic.Int32, okBody, contentType string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(strings.TrimPrefix(router503Body, "9router: ")))
			return
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write([]byte(okBody))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestOpenAIChat_RateLimited503RetriesSameRequest(t *testing.T) {
	var hits atomic.Int32
	srv := newRateLimitedServer(t, &hits,
		`{"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`, "application/json")
	s := &fakeSleeper{}
	p := NewOpenAIProvider("9router", "k", srv.URL, "m")
	p.retryConfig = rateLimitTestConfig(s)

	resp, err := p.Chat(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if resp.Content != "done" || hits.Load() != 2 {
		t.Fatalf("content=%q hits=%d, want done after 2 requests", resp.Content, hits.Load())
	}
	if len(s.waits) != 1 || s.waits[0] != 9*time.Second {
		t.Fatalf("waits = %v, want [9s]", s.waits)
	}
}

func TestOpenAIChatStream_RateLimited503RetriesBeforeAnyChunk(t *testing.T) {
	var hits atomic.Int32
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	srv := newRateLimitedServer(t, &hits, sse, "text/event-stream")
	s := &fakeSleeper{}
	p := NewOpenAIProvider("9router", "k", srv.URL, "m")
	p.retryConfig = rateLimitTestConfig(s)

	var streamed strings.Builder
	resp, err := p.ChatStream(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}},
		func(c StreamChunk) { streamed.WriteString(c.Content) })
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if resp.Content != "hello" || streamed.String() != "hello" || hits.Load() != 2 {
		t.Fatalf("content=%q streamed=%q hits=%d, want hello once after 2 requests", resp.Content, streamed.String(), hits.Load())
	}
	if len(s.waits) != 1 || s.waits[0] != 9*time.Second {
		t.Fatalf("waits = %v, want [9s]", s.waits)
	}
}

func TestCodexChatStream_RateLimitWaitsThenRetriesSameRequest(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "4")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"detail":"Rate limit exceeded"}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: " + mustJSON(codexSSEEvent{Type: "response.output_text.delta", Delta: "ok"}) + "\n\n" +
			"data: " + mustJSON(codexSSEEvent{Type: "response.completed", Response: &codexAPIResponse{Status: "completed"}}) + "\n\n" +
			"data: [DONE]\n\n"))
	}))
	defer srv.Close()

	s := &fakeSleeper{}
	p := NewCodexProvider("test", &staticTokenSource{token: "tok"}, srv.URL, "gpt-4o")
	p.retryConfig = rateLimitTestConfig(s)

	result, err := p.ChatStream(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}}, nil)
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if result.Content != "ok" || hits.Load() != 2 {
		t.Fatalf("content=%q hits=%d, want ok after 2 requests", result.Content, hits.Load())
	}
	if len(s.waits) != 1 || s.waits[0] != 5*time.Second {
		t.Fatalf("waits = %v, want [5s]", s.waits)
	}
}
