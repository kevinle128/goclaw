package acpserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestSessionRejectsSecondActivePrompt(t *testing.T) {
	r := NewSessionRegistry(0)
	if err := r.Add(&sessionRecord{ID: "s", SessionKey: "key"}); err != nil {
		t.Fatal(err)
	}
	_, generation, _, err := r.BeginPrompt("s", context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.BeginPrompt("s", context.Background(), nil); !errors.Is(err, ErrPromptActive) {
		t.Fatalf("second prompt error = %v", err)
	}
	if !r.FinishPrompt("s", generation) {
		t.Fatal("first generation did not finish")
	}
}

func TestSessionLateOldGenerationCannotFinishNewPrompt(t *testing.T) {
	r := NewSessionRegistry(0)
	if err := r.Add(&sessionRecord{ID: "s", SessionKey: "key"}); err != nil {
		t.Fatal(err)
	}
	_, first, _, _ := r.BeginPrompt("s", context.Background(), nil)
	r.FinishPrompt("s", first)
	_, second, _, _ := r.BeginPrompt("s", context.Background(), nil)
	if r.FinishPrompt("s", first) {
		t.Fatal("old generation finished new prompt")
	}
	if !r.FinishPrompt("s", second) {
		t.Fatal("new generation did not finish")
	}
}

func TestBeginPromptUsesConfiguredMaxDuration(t *testing.T) {
	for name, test := range map[string]struct {
		configured time.Duration
		want       time.Duration
	}{
		"configured": {5 * time.Minute, 5 * time.Minute},
		"long":       {24 * time.Hour, 24 * time.Hour},
		"default":    {0, MaxPromptDurationDefault},
	} {
		t.Run(name, func(t *testing.T) {
			r := NewSessionRegistry(test.configured)
			if err := r.Add(&sessionRecord{ID: "s", SessionKey: "key"}); err != nil {
				t.Fatal(err)
			}
			before := time.Now()
			_, generation, ctx, err := r.BeginPrompt("s", context.Background(), nil)
			after := time.Now()
			if err != nil {
				t.Fatal(err)
			}
			defer r.FinishPrompt("s", generation)
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Before(before.Add(test.want)) || deadline.After(after.Add(test.want)) {
				t.Fatalf("prompt deadline = %v (set %v), want %v after start", deadline, ok, test.want)
			}
		})
	}
	if MaxPromptDurationDefault != 2*time.Hour {
		t.Fatalf("default prompt duration = %v, want 2h", MaxPromptDurationDefault)
	}
}

func TestThreadContextFromSessionNewAndPromptOverride(t *testing.T) {
	r := NewSessionRegistry(0)
	sessionThread := json.RawMessage(`{"channelId":"c1","rootEventId":"e1"}`)
	if err := r.Add(&sessionRecord{ID: "s", SessionKey: "key", thread: sessionThread}); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.PromptContext("s", 1); ok {
		t.Fatal("prompt context exists without an active prompt")
	}

	_, first, _, err := r.BeginPrompt("s", context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := r.PromptContext("s", first)
	if !ok || got.SessionID != "s" || got.SessionKey != "key" || string(got.Thread) != string(sessionThread) {
		t.Fatalf("prompt context without prompt thread = %#v, %v", got, ok)
	}
	r.FinishPrompt("s", first)

	promptThread := json.RawMessage(`{"channelId":"c2","rootEventId":"e2"}`)
	_, second, _, err := r.BeginPrompt("s", context.Background(), promptThread)
	if err != nil {
		t.Fatal(err)
	}
	got, ok = r.PromptContext("s", second)
	if !ok || string(got.Thread) != string(promptThread) {
		t.Fatalf("prompt context with prompt thread = %#v, %v", got, ok)
	}
	got.Thread[0] = 'x'
	if again, _ := r.PromptContext("s", second); string(again.Thread) != string(promptThread) {
		t.Fatalf("caller changed stored thread: %s", again.Thread)
	}
	if _, ok := r.PromptContext("s", first); ok {
		t.Fatal("old generation returned a prompt context")
	}
	r.FinishPrompt("s", second)

	_, third, _, err := r.BeginPrompt("s", context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := r.PromptContext("s", third); string(got.Thread) != string(sessionThread) {
		t.Fatalf("prompt thread leaked into a later turn: %s", got.Thread)
	}
	r.FinishPrompt("s", third)
}
