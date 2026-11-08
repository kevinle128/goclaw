//go:build conformance

// Command mcpconformance runs goclaw's MCP events client under the client
// scenarios of the MCP conformance suite (events-client-*).
//
// The conformance runner appends the server URL as the last argument, sets
// MCP_CONFORMANCE_SCENARIO to the scenario name, and sets
// MCP_CONFORMANCE_CONTEXT to a JSON object with durationMs. The driver
// connects, subscribes to every listed event type for durationMs, and exits.
//
// Build:
//
//	go build -tags conformance -o goclaw-driver ./tests/mcpconformance
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/mcp"
)

// defaultDuration applies when the runner sends no durationMs.
const defaultDuration = 5 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mcpconformance:", err)
		os.Exit(1)
	}
}

func run() error {
	heartbeat := flag.Duration("heartbeat", 0, "expected heartbeat interval of a push stream (0 = the gateway default of task_events.heartbeat_sec)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))

	if flag.NArg() < 1 {
		return errors.New("usage: mcpconformance [-heartbeat d] <server-url>")
	}
	serverURL := flag.Arg(flag.NArg() - 1)
	if u, err := url.Parse(serverURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("invalid server url %q", serverURL)
	}

	duration, err := durationFromContext(os.Getenv("MCP_CONFORMANCE_CONTEXT"))
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("mcpconformance.start",
		"scenario", os.Getenv("MCP_CONFORMANCE_SCENARIO"),
		"protocol_version", os.Getenv("MCP_CONFORMANCE_PROTOCOL_VERSION"),
		"duration", duration.String(),
		"heartbeat", heartbeat.String())
	return mcp.RunEventsClient(ctx, serverURL, mcp.EventsClientOptions{Duration: duration, Heartbeat: *heartbeat})
}

// durationFromContext reads durationMs from the MCP_CONFORMANCE_CONTEXT JSON.
// An empty context gives defaultDuration.
func durationFromContext(raw string) (time.Duration, error) {
	if raw == "" {
		return defaultDuration, nil
	}
	var c struct {
		DurationMs *int64 `json:"durationMs"`
	}
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return 0, fmt.Errorf("invalid MCP_CONFORMANCE_CONTEXT: %w", err)
	}
	if c.DurationMs == nil {
		return defaultDuration, nil
	}
	if *c.DurationMs <= 0 {
		return 0, fmt.Errorf("invalid durationMs %d in MCP_CONFORMANCE_CONTEXT", *c.DurationMs)
	}
	return time.Duration(*c.DurationMs) * time.Millisecond, nil
}
