//go:build acpclient

// Command acpclient is a scripted ACP client for manual end-to-end checks of
// `goclaw acp`. It plays the part of a chat bridge: it starts the adapter,
// opens a session in a fake thread, sends one prompt, keeps reading the
// adapter output while no prompt is active, accepts `_buzz/wake` requests by
// opening a new turn with the wake prompt, and answers `elicitation/create`
// from a script. Every frame goes to a JSONL log.
//
// Build: go build -tags acpclient -o /tmp/acpclient ./tests/acpclient
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type options struct {
	cwd         string
	prompt      string
	channelID   string
	rootEventID string
	answers     []string
	wakeReplies []string
	declareWake bool
	declareForm bool
	stopReasons map[string]bool
	timeout     time.Duration
	logPath     string
	maxTurns    int
	adapterArgv []string
}

type frame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

type client struct {
	opts    options
	stdin   io.WriteCloser
	writeMu sync.Mutex
	logMu   sync.Mutex
	logFile *os.File
	nextID  atomic.Int64

	pendingMu sync.Mutex
	pending   map[string]chan frame

	answerMu sync.Mutex
	answerIx int
	wakeIx   int

	prompts chan promptJob
	done    chan string
}

type promptJob struct {
	text   string
	thread json.RawMessage
}

func main() {
	var opts options
	var answers, wakeReplies, stop string
	flag.StringVar(&opts.cwd, "cwd", "", "session working directory (inside the adapter workspace root)")
	flag.StringVar(&opts.prompt, "prompt", "", "first prompt text")
	flag.StringVar(&opts.channelID, "channel", "g2", "thread channel id")
	flag.StringVar(&opts.rootEventID, "root", "g2-root", "thread root event id")
	flag.StringVar(&answers, "answers", "accept:approve", "comma list of elicitation answers: accept:<decision>, decline, cancel")
	flag.StringVar(&wakeReplies, "wake-replies", "accepted", "comma list of wake replies in order: accepted, busy, duplicate, notfound; the last one repeats")
	flag.BoolVar(&opts.declareWake, "declare-wake", true, "declare the buzz.wake capability")
	flag.BoolVar(&opts.declareForm, "declare-form", true, "declare elicitation form mode")
	flag.StringVar(&stop, "stop-on", "completed,failed,cancelled", "wake reasons that end the run after their turn")
	flag.DurationVar(&opts.timeout, "timeout", 20*time.Minute, "overall time limit")
	flag.StringVar(&opts.logPath, "log", "/tmp/acpclient.jsonl", "JSONL frame log")
	flag.IntVar(&opts.maxTurns, "max-turns", 0, "stop after this many turns (0 = no limit)")
	flag.Parse()
	opts.adapterArgv = flag.Args()
	if len(opts.adapterArgv) == 0 || opts.cwd == "" || opts.prompt == "" {
		log.Fatal("usage: acpclient -cwd DIR -prompt TEXT [flags] -- <adapter command and args>")
	}
	opts.answers = splitList(answers)
	opts.wakeReplies = splitList(wakeReplies)
	opts.stopReasons = map[string]bool{}
	for _, reason := range splitList(stop) {
		opts.stopReasons[reason] = true
	}
	if err := run(opts); err != nil {
		log.Fatal(err)
	}
}

func splitList(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func run(opts options) error {
	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	logFile, err := os.OpenFile(opts.logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()

	cmd := exec.CommandContext(ctx, opts.adapterArgv[0], opts.adapterArgv[1:]...)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	c := &client{opts: opts, stdin: stdin, logFile: logFile, pending: map[string]chan frame{}, prompts: make(chan promptJob, 16), done: make(chan string, 1)}
	go c.readLoop(stdout)

	if err := c.initialize(ctx); err != nil {
		return err
	}
	thread, _ := json.Marshal(map[string]string{"channelId": opts.channelID, "rootEventId": opts.rootEventID})
	c.prompts <- promptJob{text: opts.prompt, thread: thread}

	var outcome string
	turns := 0
	for outcome == "" {
		select {
		case job := <-c.prompts:
			if err := c.runTurn(ctx, job); err != nil {
				c.note("turn_error", err.Error())
			}
			turns++
			if opts.maxTurns > 0 && turns >= opts.maxTurns {
				outcome = "max-turns"
			}
		case outcome = <-c.done:
		case <-ctx.Done():
			outcome = "timeout"
		}
	}
	// Let the last turn finish when the stop reason arrived during idle time.
	for len(c.prompts) > 0 {
		if err := c.runTurn(ctx, <-c.prompts); err != nil {
			c.note("turn_error", err.Error())
		}
	}
	c.note("finished", outcome)
	_ = stdin.Close()
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case <-waitDone:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
	}
	fmt.Println("outcome:", outcome)
	if outcome == "timeout" {
		return errors.New("time limit reached")
	}
	return nil
}

func (c *client) initialize(ctx context.Context) error {
	caps := map[string]any{}
	if c.opts.declareForm {
		caps["elicitation"] = map[string]any{"form": map[string]any{}}
	}
	if c.opts.declareWake {
		caps["_meta"] = map[string]any{"buzz.wake": map[string]any{"version": 1}}
	}
	_, err := c.call(ctx, "initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": caps, "clientInfo": map[string]string{"name": "acpclient", "version": "1"}})
	return err
}

func (c *client) runTurn(ctx context.Context, job promptJob) error {
	result, err := c.call(ctx, "session/new", map[string]any{"cwd": c.opts.cwd, "mcpServers": []any{}, "_meta": map[string]json.RawMessage{"buzz.thread": job.thread}})
	if err != nil {
		return err
	}
	var session struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(result, &session); err != nil {
		return err
	}
	_, err = c.call(ctx, "session/prompt", map[string]any{
		"sessionId": session.SessionID,
		"prompt":    []map[string]string{{"type": "text", "text": job.text}},
		"_meta":     map[string]json.RawMessage{"buzz.thread": job.thread},
	})
	return err
}

func (c *client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := fmt.Sprintf("c-%d", c.nextID.Add(1))
	reply := make(chan frame, 1)
	c.pendingMu.Lock()
	c.pending[id] = reply
	c.pendingMu.Unlock()
	raw, _ := json.Marshal(params)
	if err := c.write(frame{JSONRPC: "2.0", ID: json.RawMessage(`"` + id + `"`), Method: method, Params: raw}); err != nil {
		return nil, err
	}
	select {
	case f := <-reply:
		if len(f.Error) > 0 {
			return nil, fmt.Errorf("%s: %s", method, f.Error)
		}
		return f.Result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *client) readLoop(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 1<<20), 8<<20)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		c.log("in", line)
		var f frame
		if json.Unmarshal(line, &f) != nil {
			continue
		}
		switch {
		case f.Method == "" && len(f.ID) > 0:
			var id string
			_ = json.Unmarshal(f.ID, &id)
			c.pendingMu.Lock()
			reply := c.pending[id]
			delete(c.pending, id)
			c.pendingMu.Unlock()
			if reply != nil {
				reply <- f
			}
		case f.Method != "" && len(f.ID) > 0:
			go c.handleRequest(f)
		}
	}
	c.note("adapter_stdout_closed", "")
}

func (c *client) handleRequest(f frame) {
	switch f.Method {
	case "_buzz/wake":
		c.handleWake(f)
	case "elicitation/create":
		c.handleElicitation(f)
	default:
		c.reply(f.ID, nil, map[string]any{"code": -32601, "message": "Method not found"})
	}
}

func (c *client) handleWake(f frame) {
	var params struct {
		WakeID string          `json:"wakeId"`
		Reason string          `json:"reason"`
		Thread json.RawMessage `json:"thread"`
		Prompt string          `json:"prompt"`
	}
	if json.Unmarshal(f.Params, &params) != nil || params.WakeID == "" || params.Prompt == "" {
		c.reply(f.ID, nil, map[string]any{"code": -32602, "message": "Invalid params"})
		return
	}
	c.answerMu.Lock()
	choice := c.opts.wakeReplies[min(c.wakeIx, len(c.opts.wakeReplies)-1)]
	c.wakeIx++
	c.answerMu.Unlock()
	switch choice {
	case "notfound":
		c.reply(f.ID, nil, map[string]any{"code": -32601, "message": "Method not found"})
		return
	case "busy", "duplicate":
		c.reply(f.ID, map[string]string{"outcome": choice}, nil)
		return
	}
	c.reply(f.ID, map[string]string{"outcome": "accepted"}, nil)
	c.prompts <- promptJob{text: params.Prompt, thread: params.Thread}
	if c.opts.stopReasons[params.Reason] {
		select {
		case c.done <- "wake:" + params.Reason:
		default:
		}
	}
}

func (c *client) handleElicitation(f frame) {
	c.answerMu.Lock()
	choice := "cancel"
	if c.answerIx < len(c.opts.answers) {
		choice = c.opts.answers[c.answerIx]
	}
	c.answerIx++
	c.answerMu.Unlock()
	decision, hasDecision := strings.CutPrefix(choice, "accept:")
	if hasDecision {
		c.reply(f.ID, map[string]any{"action": "accept", "content": map[string]string{"decision": decision, "comment": "acpclient"}}, nil)
		return
	}
	c.reply(f.ID, map[string]any{"action": choice}, nil)
}

func (c *client) reply(id json.RawMessage, result any, rpcErr any) {
	out := map[string]any{"jsonrpc": "2.0", "id": id}
	if rpcErr != nil {
		out["error"] = rpcErr
	} else {
		out["result"] = result
	}
	data, _ := json.Marshal(out)
	_ = c.writeRaw(data)
}

func (c *client) write(f frame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return c.writeRaw(data)
}

func (c *client) writeRaw(data []byte) error {
	c.log("out", data)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.stdin.Write(append(data, '\n'))
	return err
}

func (c *client) note(kind, detail string) {
	data, _ := json.Marshal(map[string]string{"note": kind, "detail": detail})
	c.log("note", data)
}

func (c *client) log(direction string, data []byte) {
	entry, _ := json.Marshal(map[string]any{"t": time.Now().Format(time.RFC3339Nano), "dir": direction, "frame": json.RawMessage(validOrString(data))})
	c.logMu.Lock()
	defer c.logMu.Unlock()
	_, _ = c.logFile.Write(append(entry, '\n'))
}

func validOrString(data []byte) []byte {
	if json.Valid(data) {
		return data
	}
	quoted, _ := json.Marshal(string(data))
	return quoted
}
