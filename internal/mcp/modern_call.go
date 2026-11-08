package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	mcpclient "github.com/mark3labs/mcp-go/client"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

type mcpContextKey string

const ctxElicitationForm mcpContextKey = "mcp_elicitation_form"

// WithElicitationForm marks whether the current run can answer an
// elicitation in form mode (for example, an ACP client that declared form
// support). Modern tools/call requests declare elicitation {form: {}} only
// when this is true.
func WithElicitationForm(ctx context.Context, canAsk bool) context.Context {
	return context.WithValue(ctx, ctxElicitationForm, canAsk)
}

// ElicitationFormFromContext reports the value set by WithElicitationForm
// (false when it was not set).
func ElicitationFormFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(ctxElicitationForm).(bool)
	return v
}

// TaskStart describes a task that a modern tools/call started.
type TaskStart struct {
	ServerName     string
	ServerID       uuid.UUID
	ToolName       string // original MCP tool name
	RegisteredName string
	// Client is the client that sent the tools/call. Later tasks/* and
	// events/* requests of this task use the same capability set.
	Client *mcpclient.Client
	// Capabilities is the per-request capability set of the tools/call
	// (ID and Timeout are cleared).
	Capabilities RawOpts
	Task         CreateTaskResult
}

// TaskStarter receives tasks started by modern tools/call requests. It
// returns the tool result that the agent sees.
type TaskStarter interface {
	StartTask(ctx context.Context, start TaskStart) *tools.Result
}

// maxEchoedResultTypeLen bounds how much of an unknown, server-controlled
// resultType goes into an error text.
const maxEchoedResultTypeLen = 64

// executeModern sends tools/call on a 2026-07-28 connection through the raw
// request layer and handles the resultType discriminator.
func (t *BridgeTool) executeModern(ctx, callCtx context.Context, client *mcpclient.Client, args map[string]any, callStart time.Time) *tools.Result {
	caps := client.GetServerCapabilities()
	opts := RawOpts{
		Tasks:           serverHasExtension(caps, ExtensionTasks),
		Events:          serverHasExtension(caps, ExtensionEvents),
		ElicitationForm: ElicitationFormFromContext(ctx),
		Timeout:         time.Duration(t.timeoutSec) * time.Second,
	}
	params := map[string]any{"name": t.toolName}
	if args != nil {
		params["arguments"] = args
	}

	raw, err := rawCall(callCtx, client, methodToolsCall, params, opts)
	latencyMs := time.Since(callStart).Milliseconds()
	if err != nil {
		return t.callErrorResult(ctx, callCtx, err, latencyMs, EraModern)
	}

	var probe struct {
		ResultType string `json:"resultType"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return tools.ErrorResult(fmt.Sprintf("MCP tool %q error: result is not a JSON object", t.registeredName))
	}

	switch probe.ResultType {
	case "", ResultTypeComplete:
		result, err := mcpgo.ParseCallToolResult(&raw)
		if err != nil {
			return tools.ErrorResult(fmt.Sprintf("MCP tool %q error: invalid result: %v", t.registeredName, err))
		}
		slog.Debug("mcp.tool.call.done",
			"server", t.serverName, "tool", t.registeredName,
			"user_id", store.UserIDFromContext(ctx),
			"latency_ms", latencyMs, "is_error", result.IsError, "era", EraModern.String())
		return t.toolResult(result)

	case ResultTypeTask:
		var task CreateTaskResult
		if err := json.Unmarshal(raw, &task); err != nil || task.TaskID == "" {
			return tools.ErrorResult(fmt.Sprintf("MCP tool %q error: invalid task result", t.registeredName))
		}
		slog.Info("mcp.tool.call.task_started",
			"server", t.serverName, "tool", t.registeredName,
			"task_id", task.TaskID, "status", task.Status,
			"user_id", store.UserIDFromContext(ctx),
			"latency_ms", latencyMs)
		if t.taskStarter != nil {
			taskCaps := opts
			taskCaps.ID = ""
			taskCaps.Timeout = 0
			return t.taskStarter.StartTask(ctx, TaskStart{
				ServerName:     t.serverName,
				ServerID:       t.serverID,
				ToolName:       t.toolName,
				RegisteredName: t.registeredName,
				Client:         client,
				Capabilities:   taskCaps,
				Task:           task,
			})
		}
		// The task id and status come from the server, so they are wrapped
		// as untrusted content like every other MCP result.
		text := fmt.Sprintf("MCP task %s started (status %s).", task.TaskID, task.Status)
		return tools.NewResult(wrapMCPContent(text, t.serverName, t.toolName))

	case ResultTypeInputRequired:
		return tools.ErrorResult(fmt.Sprintf(
			"MCP tool %q: server asks for input before the task starts; not supported", t.registeredName))

	default:
		rt := probe.ResultType
		if len(rt) > maxEchoedResultTypeLen {
			rt = rt[:maxEchoedResultTypeLen]
		}
		return tools.ErrorResult(fmt.Sprintf("MCP tool %q error: unknown resultType %q", t.registeredName, rt))
	}
}
