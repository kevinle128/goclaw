package tools

import (
	"context"
	"regexp"
	"strings"
	"sync"
)

// Credential patterns to scrub from tool output before returning to the LLM.
// Inspired by zeroclaw's credential scrubbing system.
var credentialPatterns = []*regexp.Regexp{
	// OpenAI
	regexp.MustCompile(`sk-[a-zA-Z0-9]{20,}`),
	// Anthropic
	regexp.MustCompile(`sk-ant-[a-zA-Z0-9-]{20,}`),
	// GitHub personal access tokens
	regexp.MustCompile(`ghp_[a-zA-Z0-9]{36}`),
	regexp.MustCompile(`gho_[a-zA-Z0-9]{36}`),
	regexp.MustCompile(`ghu_[a-zA-Z0-9]{36}`),
	regexp.MustCompile(`ghs_[a-zA-Z0-9]{36}`),
	regexp.MustCompile(`ghr_[a-zA-Z0-9]{36}`),
	// AWS
	regexp.MustCompile(`AKIA[A-Z0-9]{16}`),
	// Generic key=value patterns (case-insensitive)
	regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password|bearer|authorization)\s*[:=]\s*["']?\S{8,}["']?`),

	// Connection strings (PostgreSQL, MySQL, MongoDB, Redis, AMQP)
	regexp.MustCompile(`(?i)(postgres|postgresql|mysql|mongodb|redis|amqp)://[^\s"']+`),
	// Generic KEY=/SECRET=/CREDENTIAL= env-var patterns (skip already-redacted [REDACTED] values)
	regexp.MustCompile(`(?i)[A-Z_]*(KEY|SECRET|CREDENTIAL|PRIVATE)[A-Z_]*\s*=\s*[^\[\s]{8,}`),
	// DSN/DATABASE_URL env vars (skip already-redacted values)
	regexp.MustCompile(`(?i)(DSN|DATABASE_URL|REDIS_URL|MONGO_URI)\s*=\s*[^\[\s]{8,}`),
	// VIRTUAL_* env vars (internal runtime config, should not leak)
	regexp.MustCompile(`(?i)VIRTUAL_[A-Z_]+\s*=\s*[^\[\s]{4,}`),
}

// longHexPattern matches hex runs of 64+ chars — likely encryption keys, hashes
// or secrets. It runs separately from credentialPatterns so that Nostr ids can
// be kept visible (see scrubLongHex).
var longHexPattern = regexp.MustCompile(`[a-fA-F0-9]{64,}`)

// Nostr event ids and pubkeys are exactly 64 hex chars, so the long-hex rule
// would hide every id a chat CLI returns and the agent could no longer reply
// to or react to a message. A 64-hex value is kept only when it is the value
// of a known id-like key, an entry of mention_pubkeys, or the second element
// of an "e"/"p" tag. Every quote may be preceded by backslashes, so the same
// rules work when the JSON is nested inside a string of another JSON document.
var (
	nostrIDKeyPattern = regexp.MustCompile(
		`\\*"(?:event_id|id|pubkey|reply_to|reply_to_id|thread_root|root|root_id|parent_id|author|author_pubkey)\\*"\s*:\s*\\*"([a-fA-F0-9]{64})\\*"`)
	nostrMentionPattern = regexp.MustCompile(
		`\\*"mention_pubkeys\\*"\s*:\s*\[([^\]]*)\]`)
	nostrTagPattern = regexp.MustCompile(
		`\[\s*\\*"[ep]\\*"\s*,\s*\\*"([a-fA-F0-9]{64})\\*"`)
)

// scrubLongHex redacts 64+ char hex runs except Nostr ids in the positions
// described above.
func scrubLongHex(text string) string {
	runs := longHexPattern.FindAllStringIndex(text, -1)
	if len(runs) == 0 {
		return text
	}
	keep := make(map[int]struct{})
	for _, m := range nostrIDKeyPattern.FindAllStringSubmatchIndex(text, -1) {
		keep[m[2]] = struct{}{}
	}
	for _, m := range nostrTagPattern.FindAllStringSubmatchIndex(text, -1) {
		keep[m[2]] = struct{}{}
	}
	for _, m := range nostrMentionPattern.FindAllStringSubmatchIndex(text, -1) {
		for _, r := range longHexPattern.FindAllStringIndex(text[m[2]:m[3]], -1) {
			keep[m[2]+r[0]] = struct{}{}
		}
	}
	var b strings.Builder
	last := 0
	for _, r := range runs {
		// Only an exact 64-char run can be an id; the key patterns already
		// require exactly 64 hex between the quotes, the mention list does not.
		if _, ok := keep[r[0]]; ok && r[1]-r[0] == 64 {
			continue
		}
		b.WriteString(text[last:r[0]])
		b.WriteString(redactedPlaceholder)
		last = r[1]
	}
	b.WriteString(text[last:])
	return b.String()
}

const redactedPlaceholder = "[REDACTED]"
const serverIPPlaceholder = "[SERVER_IP]"

// dynamicScrubValues holds runtime-discovered values to scrub (e.g., server IPs).
var (
	dynamicScrubMu     sync.RWMutex
	dynamicScrubValues []string
)

// credentialScrubValues holds credential values to scrub (replaced with [REDACTED]).
// Separate from dynamicScrubValues which use [SERVER_IP] placeholder.
var (
	credentialScrubMu     sync.RWMutex
	credentialScrubValues []string
)

// AddDynamicScrubValues adds exact string values to the dynamic scrub list.
// Thread-safe. Deduplicates. Empty strings are ignored.
func AddDynamicScrubValues(values ...string) {
	dynamicScrubMu.Lock()
	defer dynamicScrubMu.Unlock()

	existing := make(map[string]bool, len(dynamicScrubValues))
	for _, v := range dynamicScrubValues {
		existing[v] = true
	}
	for _, v := range values {
		if v != "" && !existing[v] {
			dynamicScrubValues = append(dynamicScrubValues, v)
			existing[v] = true
		}
	}
}

// DynamicScrubCount returns the number of dynamic scrub values registered.
func DynamicScrubCount() int {
	dynamicScrubMu.RLock()
	defer dynamicScrubMu.RUnlock()
	return len(dynamicScrubValues)
}

// ResetDynamicScrubValues clears all dynamic scrub values. For testing only.
func ResetDynamicScrubValues() {
	dynamicScrubMu.Lock()
	defer dynamicScrubMu.Unlock()
	dynamicScrubValues = nil
}

// ResetCredentialScrubValues clears all credential scrub values.
// Called when credentials are updated to prevent stale values from accumulating.
func ResetCredentialScrubValues() {
	credentialScrubMu.Lock()
	defer credentialScrubMu.Unlock()
	credentialScrubValues = nil
}

// AddCredentialScrubValues adds credential values to the scrub list.
// These are replaced with [REDACTED] (not [SERVER_IP]).
// Thread-safe. Deduplicates. Empty/short strings ignored.
func AddCredentialScrubValues(values ...string) {
	credentialScrubMu.Lock()
	defer credentialScrubMu.Unlock()

	existing := make(map[string]bool, len(credentialScrubValues))
	for _, v := range credentialScrubValues {
		existing[v] = true
	}
	for _, v := range values {
		// Skip short values to avoid false-positive scrubbing
		if len(v) >= 6 && !existing[v] {
			credentialScrubValues = append(credentialScrubValues, v)
			existing[v] = true
		}
	}
}

// --- Per-request scrub bag (multi-tenant safe) ---
//
// The package-global credentialScrubValues above accumulates across all
// goroutines for the life of the process. For the adapter pipeline (Phase 2+)
// each exec gets its own scrub bag via context so tenant A's credentials never
// reach tenant B's output, even when adapters run concurrently.

type scrubBag struct {
	mu     sync.RWMutex
	values []string
}

type scrubBagKey struct{}

type execCwdKey struct{}

// WithExecCwd returns a context carrying the working directory the
// credentialed exec will use. Adapters consult this so any pre-flight
// sub-exec (e.g. `git config --get remote.origin.url`) runs inside the
// caller's repo, not goclaw's daemon CWD.
func WithExecCwd(ctx context.Context, cwd string) context.Context {
	if cwd == "" {
		return ctx
	}
	return context.WithValue(ctx, execCwdKey{}, cwd)
}

// ExecCwdFromContext returns the cwd planted by WithExecCwd, or empty.
func ExecCwdFromContext(ctx context.Context) string {
	if s, ok := ctx.Value(execCwdKey{}).(string); ok {
		return s
	}
	return ""
}

// WithScrubBag returns a context carrying a fresh per-request scrub list.
// Call at the top of executeCredentialed; pass the returned ctx downstream.
func WithScrubBag(ctx context.Context) context.Context {
	return context.WithValue(ctx, scrubBagKey{}, &scrubBag{})
}

// AddScrubValuesCtx appends adapter-derived secrets to the per-request bag.
// No-op when ctx has no bag (legacy callers). Skips values shorter than 6
// chars to avoid false-positive matches like "x" or "1".
func AddScrubValuesCtx(ctx context.Context, vs ...string) {
	bag, ok := ctx.Value(scrubBagKey{}).(*scrubBag)
	if !ok {
		return
	}
	bag.mu.Lock()
	defer bag.mu.Unlock()
	for _, v := range vs {
		if len(v) >= 6 {
			bag.values = append(bag.values, v)
		}
	}
}

// ScrubCredentialsCtx runs the same regex pass as ScrubCredentials AND any
// per-request bag values, but does NOT consult the package-global slice.
// Use this in the adapter pipeline so one tenant's secrets cannot leak into
// another tenant's output through the shared global.
func ScrubCredentialsCtx(ctx context.Context, text string) string {
	for _, pat := range credentialPatterns {
		text = pat.ReplaceAllString(text, redactedPlaceholder)
	}
	text = scrubLongHex(text)
	if bag, ok := ctx.Value(scrubBagKey{}).(*scrubBag); ok {
		bag.mu.RLock()
		vals := append([]string(nil), bag.values...)
		bag.mu.RUnlock()
		for _, v := range vals {
			text = strings.ReplaceAll(text, v, redactedPlaceholder)
		}
	}
	// Dynamic server-IP values still apply — they're infra metadata, not creds.
	dynamicScrubMu.RLock()
	dyn := dynamicScrubValues
	dynamicScrubMu.RUnlock()
	for _, v := range dyn {
		text = strings.ReplaceAll(text, v, serverIPPlaceholder)
	}
	return text
}

// ScrubCredentials replaces known credential patterns and dynamic values in text.
func ScrubCredentials(text string) string {
	for _, pat := range credentialPatterns {
		text = pat.ReplaceAllString(text, redactedPlaceholder)
	}
	text = scrubLongHex(text)

	// Credential values (from credentialed exec)
	credentialScrubMu.RLock()
	credVals := credentialScrubValues
	credentialScrubMu.RUnlock()

	for _, v := range credVals {
		text = strings.ReplaceAll(text, v, redactedPlaceholder)
	}

	// Dynamic values (server IPs, etc.)
	dynamicScrubMu.RLock()
	vals := dynamicScrubValues
	dynamicScrubMu.RUnlock()

	for _, v := range vals {
		text = strings.ReplaceAll(text, v, serverIPPlaceholder)
	}

	return text
}
