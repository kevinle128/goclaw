package gateway

import (
	"errors"
	"strings"
	"testing"

	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

func TestGatewayCriticalSendOverflowClosesConnection(t *testing.T) {
	client := &Client{send: make(chan []byte, 1)}
	client.send <- []byte("occupied")

	err := client.SendCriticalResponse(protocol.NewOKResponse("request-1", nil))
	if !errors.Is(err, ErrClientSendQueueFull) {
		t.Fatalf("SendCriticalResponse() error = %v, want %v", err, ErrClientSendQueueFull)
	}
	if err := client.SendCriticalEvent(*protocol.NewEvent("test", nil)); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("SendCriticalEvent() after overflow error = %v, want %v", err, ErrClientClosed)
	}

	client.Close()
	client.Close()
}

func TestGatewayCriticalSendRejectsOversizedFrame(t *testing.T) {
	client := &Client{send: make(chan []byte, 1)}
	err := client.SendCriticalEvent(*protocol.NewEvent("test", strings.Repeat("x", maxCriticalWSMessageSize)))
	if !errors.Is(err, ErrClientCriticalFrameTooLarge) {
		t.Fatalf("SendCriticalEvent() error = %v, want %v", err, ErrClientCriticalFrameTooLarge)
	}
	if len(client.send) != 0 {
		t.Fatal("oversized critical event was queued")
	}
}
