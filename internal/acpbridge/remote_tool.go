package acpbridge

import (
	"context"

	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

type RemoteTool struct {
	manager      *Manager
	capabilityID string
	owner        Owner
	sessionKey   string
	generation   uint64
	schema       ToolSchema
}

func (t *RemoteTool) Name() string               { return t.schema.Name }
func (t *RemoteTool) Description() string        { return t.schema.Description }
func (t *RemoteTool) Parameters() map[string]any { return t.schema.InputSchema }

func (t *RemoteTool) Execute(ctx context.Context, arguments map[string]any) *tools.Result {
	call, err := t.manager.BeginCall(ctx, t.capabilityID, t.owner.ConnectionID, t.sessionKey, t.generation, t.schema.Name, arguments)
	if err != nil {
		return tools.ErrorResult(err.Error()).WithError(err)
	}
	select {
	case result := <-call.Result:
		if result.IsError {
			return tools.ErrorResult(result.Content)
		}
		return tools.NewResult(result.Content)
	case <-ctx.Done():
		return tools.ErrorResult("ACP tool call cancelled").WithError(ctx.Err())
	}
}

func RemoteToolMetadata(name string) tools.ToolMetadata {
	return tools.ToolMetadata{Name: name, Capabilities: []tools.ToolCapability{tools.CapMCPBridged, tools.CapMutating}}
}
