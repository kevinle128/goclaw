package acpserver

import (
	"errors"
	"io"
	"testing"
	"time"
)

func TestMCPTransportStopsOversizedEndlessLine(t *testing.T) {
	transport := newBoundedStdioTransport("unused", nil, nil, t.TempDir(), nil).(*boundedStdioTransport)
	reader, writer := io.Pipe()
	go transport.readLoop(reader)
	go func() {
		_, _ = writer.Write(make([]byte, maxMCPFrameBytes+1))
		_ = writer.Close()
	}()
	select {
	case <-transport.done:
		if !errors.Is(transport.closeErr, ErrMCPFrameTooLarge) {
			t.Fatalf("close error = %v, want frame limit", transport.closeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("oversized MCP output did not close the transport")
	}
}
