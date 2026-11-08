package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

// mcpTaskToolName is the name of the built-in tool for started MCP tasks.
const mcpTaskToolName = "mcp_task"

// maxQuestionBytes bounds the question text that goes to people.
const maxQuestionBytes = 8 << 10

// Texts of mcp_task results. They are built by goclaw, not by the server.
const (
	textCannotAnswerChannel = "This channel cannot answer; the run stays paused."
	textCannotAnswerHere    = "goclaw cannot answer this request here."
	textNotAnswered         = "Not answered; the run stays paused. Mention me to answer later."
	textAnswerSent          = "Answer sent; the run continues."
)

// MCPTaskTool is the agent's handle on a task that an MCP tool started:
// read its status, answer its input requests, or cancel it.
type MCPTaskTool struct {
	mgr *TaskWatchManager
}

// NewMCPTaskTool creates the mcp_task tool.
func NewMCPTaskTool(mgr *TaskWatchManager) *MCPTaskTool { return &MCPTaskTool{mgr: mgr} }

func (t *MCPTaskTool) Name() string { return mcpTaskToolName }

func (t *MCPTaskTool) Description() string {
	return "Work with a long-running task that an MCP tool started (for example an Archon run). " +
		"action \"status\" reads the task status and, when the task is done, its result or error. " +
		"action \"input\" shows the task's open question to the people in this conversation and sends their answer. " +
		"action \"cancel\" asks the server to cancel the task; use it only when a person asks for it."
}

func (t *MCPTaskTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        []string{"status", "input", "cancel"},
				"description": "status, input, or cancel",
			},
			"task_id": map[string]any{
				"type":        "string",
				"description": "The task id from the tool result that started the task, or the task id of a task update message",
			},
		},
		"required": []string{"action", "task_id"},
	}
}

func (t *MCPTaskTool) Execute(ctx context.Context, args map[string]any) *tools.Result {
	action, _ := args["action"].(string)
	taskID, _ := args["task_id"].(string)
	taskID = strings.TrimSpace(taskID)
	switch action {
	case "status", "input", "cancel":
	default:
		return tools.ErrorResult(`mcp_task: action must be "status", "input" or "cancel"`)
	}
	if taskID == "" || len(taskID) > 512 {
		return tools.ErrorResult("mcp_task: task_id is required")
	}

	// The row must belong to the tenant and the agent of this run, and to
	// the user of this run when the row has a user; any other row reads as
	// "not found".
	agentID := store.AgentIDFromContext(ctx)
	userID := store.UserIDFromContext(ctx)
	row, err := t.lookup(ctx, agentID, taskID)
	if err != nil || row == nil {
		return tools.ErrorResult(fmt.Sprintf("mcp_task: task %q not found", taskID))
	}
	if row.UserID != "" && row.UserID != userID {
		slog.Warn("security.mcp.task_user_mismatch", "task_row", row.ID, "agent", agentID, "action", action)
		return tools.ErrorResult(fmt.Sprintf("mcp_task: task %q not found", taskID))
	}

	conn, err := t.mgr.acquire(ctx, row, userID)
	if err != nil {
		return tools.ErrorResult(fmt.Sprintf("mcp_task: cannot reach the MCP server of task %q: %v", taskID, err))
	}
	defer conn.release()
	sess := conn.entry.state.currentSession()
	if sess == nil || sess.client == nil || sess.era != EraModern {
		return tools.ErrorResult(fmt.Sprintf("mcp_task: the MCP server of task %q has no modern connection", taskID))
	}
	tc := &taskCall{tool: t, row: row, sess: sess, serverName: conn.serverName, caps: parseRequestCaps(row.RequestCaps)}

	if action == "cancel" {
		return tc.cancel(ctx)
	}
	task, err := tc.get(ctx)
	if err != nil {
		return tools.ErrorResult(fmt.Sprintf("mcp_task: tasks/get for %q failed: %v", taskID, err))
	}
	if action == "status" {
		return tools.NewResult(tc.statusText(task))
	}
	return tc.input(ctx, task)
}

// lookup finds the row of the agent by server task id, or else by goclaw
// row id (a wake names the row id when the server task id is not safe to
// put in a prompt).
func (t *MCPTaskTool) lookup(ctx context.Context, agentID uuid.UUID, ref string) (*store.MCPTask, error) {
	row, err := t.mgr.tasks.GetTaskForAgent(ctx, agentID, ref)
	if err != nil || row != nil {
		return row, err
	}
	id, parseErr := uuid.Parse(ref)
	if parseErr != nil {
		return nil, nil
	}
	row, err = t.mgr.tasks.GetTask(ctx, id)
	if err != nil || row == nil || row.AgentID != agentID {
		return nil, err
	}
	return row, nil
}

// taskCall holds what one mcp_task call needs.
type taskCall struct {
	tool       *MCPTaskTool
	row        *store.MCPTask
	sess       *connSession
	serverName string
	caps       requestCaps
}

func (tc *taskCall) opts() RawOpts {
	o := tc.caps.rawOpts()
	o.Tasks = true
	return o
}

func (tc *taskCall) get(ctx context.Context) (DetailedTask, error) {
	var task DetailedTask
	raw, err := rawCall(ctx, tc.sess.client, methodTasksGet, map[string]any{"taskId": tc.row.TaskID}, tc.opts())
	if err != nil {
		return task, err
	}
	if err := json.Unmarshal(raw, &task); err != nil || task.Status == "" {
		return task, fmt.Errorf("invalid tasks/get result")
	}
	return task, nil
}

// cancel sends tasks/cancel. goclaw never sends it on its own; only this
// action does, when a person asks for it.
func (tc *taskCall) cancel(ctx context.Context) *tools.Result {
	if _, err := rawCall(ctx, tc.sess.client, methodTasksCancel, map[string]any{"taskId": tc.row.TaskID}, tc.opts()); err != nil {
		return tools.ErrorResult(fmt.Sprintf("mcp_task: tasks/cancel for %q failed: %v", tc.row.TaskID, err))
	}
	return tools.NewResult(fmt.Sprintf(
		"Cancel request for task %q sent; the server acknowledged it. The task stops when the server ends it; call mcp_task with action \"status\" to see the result.",
		tc.row.TaskID))
}

// statusText is the wrapped status of a task: status word, status message,
// and the result (completed) or the error (failed).
func (tc *taskCall) statusText(task DetailedTask) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Task %s: status %s.", task.TaskID, task.Status)
	if task.StatusMessage != "" {
		b.WriteString("\nStatus message: ")
		b.WriteString(task.StatusMessage)
	}
	switch task.Status {
	case TaskStatusCompleted:
		if text := taskResultText(task.Result); text != "" {
			b.WriteString("\nResult:\n")
			b.WriteString(text)
		}
	case TaskStatusFailed:
		if task.Error != nil {
			fmt.Fprintf(&b, "\nError (code %d): %s", task.Error.Code, task.Error.Message)
		}
	}
	return wrapMCPContent(b.String(), tc.serverName, mcpTaskToolName)
}

// taskResultText converts the result of a completed task (a CallToolResult)
// to text with the same conversion as a complete tools/call.
func taskResultText(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	result, err := mcpgo.ParseCallToolResult(&raw)
	if err != nil {
		return string(raw)
	}
	text := extractTextContent(result)
	if result.StructuredContent != nil {
		if sc, err := json.Marshal(result.StructuredContent); err == nil {
			if text != "" {
				text += "\n"
			}
			text += "Structured result: " + string(sc)
		}
	}
	if result.IsError {
		text = "The tool reported an error:\n" + text
	}
	return text
}

// formRequest is an open elicitation/create request in form mode.
type formRequest struct {
	key       string
	message   string
	rawSchema json.RawMessage
	schema    *formSchema
}

// parseFormRequest reads an input request that goclaw can show as a form.
func parseFormRequest(key string, req InputRequest) (*formRequest, bool) {
	if req.Method != "elicitation/create" {
		return nil, false
	}
	var p struct {
		Mode            string          `json:"mode"`
		Message         string          `json:"message"`
		RequestedSchema json.RawMessage `json:"requestedSchema"`
	}
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return nil, false
	}
	if p.Mode != "" && p.Mode != "form" {
		return nil, false
	}
	schema, err := parseFormSchema(p.RequestedSchema)
	if err != nil {
		return nil, false
	}
	return &formRequest{key: key, message: p.Message, rawSchema: p.RequestedSchema, schema: schema}, true
}

// input answers the open form requests of a task through the InputRelay of
// the run. The question text goes to people only, never into the result.
func (tc *taskCall) input(ctx context.Context, task DetailedTask) *tools.Result {
	if task.Status != TaskStatusInputRequired {
		return tools.NewResult(tc.statusText(task) + "\nThe task has no open question.")
	}
	if len(task.InputRequests) == 0 {
		// The server asks in its status message (no elicitation declared).
		return tools.NewResult(tc.statusText(task) + "\n" + textCannotAnswerHere)
	}
	relay := InputRelayFromContext(ctx)
	if relay == nil || !relay.CanAskForm() {
		return tools.NewResult(textCannotAnswerChannel)
	}

	keys := make([]string, 0, len(task.InputRequests))
	for k := range task.InputRequests {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sent, unsupported := 0, 0
	for _, key := range keys {
		if tc.row.IsAnswered(key) {
			continue
		}
		form, ok := parseFormRequest(key, task.InputRequests[key])
		if !ok {
			unsupported++
			continue
		}
		switch res, done := tc.askAndSend(ctx, relay, form); {
		case done != nil:
			return done
		case res:
			sent++
		}
	}
	switch {
	case sent > 0 && unsupported > 0:
		return tools.NewResult(textAnswerSent + " " + textCannotAnswerHere)
	case sent > 0:
		return tools.NewResult(textAnswerSent)
	case unsupported > 0:
		return tools.NewResult(textCannotAnswerHere)
	}
	return tools.NewResult(tc.statusText(task) + "\nThe open questions are already answered.")
}

// askAndSend asks one form request and sends an accepted answer. It returns
// (true, nil) when an answer was sent, and a non-nil result when the call
// must end now (declined, failed).
func (tc *taskCall) askAndSend(ctx context.Context, relay InputRelay, form *formRequest) (bool, *tools.Result) {
	message := cleanQuestion(form.message)
	toolCallID := ToolCallIDFromContext(ctx)
	for attempt := 0; ; attempt++ {
		action, content, err := relay.AskForm(ctx, toolCallID, message, form.rawSchema)
		if err != nil {
			return false, tools.ErrorResult(fmt.Sprintf("mcp_task: asking for input failed: %v; the run stays paused", err))
		}
		switch action {
		case "accept":
		case "decline", "cancel":
			// Nothing goes to the server: the run stays paused.
			return false, tools.NewResult(textNotAnswered)
		default:
			return false, tools.ErrorResult(fmt.Sprintf("mcp_task: unknown answer action %q; the run stays paused", action))
		}
		if err := form.schema.check(content); err != nil {
			if attempt == 0 {
				message = cleanQuestion(form.message + "\n\nThe answer was not valid (" + err.Error() + "). Please answer again.")
				continue
			}
			return false, tools.ErrorResult("mcp_task: the answer did not match the form twice; nothing was sent and the run stays paused")
		}
		return tc.send(ctx, form.key, content)
	}
}

// send sends tasks/update, then stores the key as answered. A key is thus
// recorded only when the server took the answer. The row lock makes the
// answers of one row go one at a time: a key that another call answered
// while this one asked is not sent again, and the result is (false, nil).
func (tc *taskCall) send(ctx context.Context, key string, content json.RawMessage) (bool, *tools.Result) {
	mgr := tc.tool.mgr
	unlock := mgr.lockRow(tc.row.ID)
	defer unlock()
	current, err := mgr.tasks.GetTask(ctx, tc.row.ID)
	if err != nil {
		return false, tools.ErrorResult(fmt.Sprintf("mcp_task: cannot read the task: %v; nothing was sent and the run stays paused", err))
	}
	if current == nil {
		return false, tools.ErrorResult(fmt.Sprintf("mcp_task: task %q not found", tc.row.TaskID))
	}
	if current.IsAnswered(key) {
		return false, nil
	}
	params := map[string]any{
		"taskId": tc.row.TaskID,
		"inputResponses": map[string]any{
			key: map[string]any{"action": "accept", "content": content},
		},
	}
	if _, err := rawCall(ctx, tc.sess.client, methodTasksUpdate, params, tc.opts()); err != nil {
		return false, tools.ErrorResult(fmt.Sprintf("mcp_task: tasks/update failed: %v; the run stays paused", err))
	}
	// The server has the answer now, so a failed record does not undo it.
	// The watcher can then make one more wake for the key if the server
	// still lists it.
	if err := mgr.tasks.AddAnsweredKey(context.WithoutCancel(ctx), tc.row.ID, key); err != nil {
		slog.Warn("mcp.task.store_failed", "op", "add_answered_key", "task_row", tc.row.ID, "error", err.Error())
	}
	tc.row.AnsweredKeys = append(tc.row.AnsweredKeys, key)
	return true, nil
}

// cleanQuestion removes control characters (except new line and tab) and
// cuts the text to 8 KiB.
func cleanQuestion(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) || r == utf8.RuneError {
			return -1
		}
		return r
	}, s)
	if len(s) <= maxQuestionBytes {
		return s
	}
	cut := maxQuestionBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
