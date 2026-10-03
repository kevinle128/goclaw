package acpserver

import (
	"context"
	"errors"
	"testing"
)

func TestSessionRejectsSecondActivePrompt(t *testing.T) {
	r := NewSessionRegistry()
	if err := r.Add(&sessionRecord{ID: "s", SessionKey: "key"}); err != nil {
		t.Fatal(err)
	}
	_, generation, _, err := r.BeginPrompt("s", context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.BeginPrompt("s", context.Background()); !errors.Is(err, ErrPromptActive) {
		t.Fatalf("second prompt error = %v", err)
	}
	if !r.FinishPrompt("s", generation) {
		t.Fatal("first generation did not finish")
	}
}

func TestSessionLateOldGenerationCannotFinishNewPrompt(t *testing.T) {
	r := NewSessionRegistry()
	if err := r.Add(&sessionRecord{ID: "s", SessionKey: "key"}); err != nil {
		t.Fatal(err)
	}
	_, first, _, _ := r.BeginPrompt("s", context.Background())
	r.FinishPrompt("s", first)
	_, second, _, _ := r.BeginPrompt("s", context.Background())
	if r.FinishPrompt("s", first) {
		t.Fatal("old generation finished new prompt")
	}
	if !r.FinishPrompt("s", second) {
		t.Fatal("new generation did not finish")
	}
}
