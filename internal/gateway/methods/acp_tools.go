package methods

import (
	"context"
	"encoding/json"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
	"github.com/nextlevelbuilder/goclaw/internal/gateway"
	"github.com/nextlevelbuilder/goclaw/internal/sessions"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// ACPToolMethods exposes the authenticated schema-only ACP tool boundary.
type ACPToolMethods struct {
	manager *acpbridge.Manager
}

func NewACPToolMethods(manager *acpbridge.Manager) *ACPToolMethods {
	return &ACPToolMethods{manager: manager}
}

func (m *ACPToolMethods) Register(router *gateway.MethodRouter) {
	router.Register(protocol.MethodACPToolsRegister, m.handleRegister)
	router.Register(protocol.MethodACPToolsResult, m.handleResult)
	router.Register(protocol.MethodACPToolsRelease, m.handleRelease)
}

type acpRegisterParams struct {
	AgentID    string                 `json:"agentId"`
	SessionKey string                 `json:"sessionKey"`
	Generation uint64                 `json:"generation"`
	Tools      []acpbridge.ToolSchema `json:"tools"`
}

func (m *ACPToolMethods) handleRegister(ctx context.Context, client *gateway.Client, req *protocol.RequestFrame) {
	var params acpRegisterParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, "invalid ACP tool registration"))
		return
	}
	agentID, _ := sessions.ParseSessionKey(params.SessionKey)
	if agentID == "" || agentID != params.AgentID {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, "ACP agent and session do not match"))
		return
	}
	capability, err := m.manager.Register(ctx, acpbridge.Registration{
		Owner: acpOwner(client), AgentID: params.AgentID, SessionKey: params.SessionKey,
		Generation: params.Generation, Tools: params.Tools, Sender: client,
	})
	if err != nil {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, err.Error()))
		return
	}
	_ = client.SendCriticalResponse(protocol.NewOKResponse(req.ID, capability))
}

func (m *ACPToolMethods) handleResult(_ context.Context, client *gateway.Client, req *protocol.RequestFrame) {
	var params struct {
		CapabilityID string               `json:"capabilityId"`
		CallID       string               `json:"callId"`
		Generation   uint64               `json:"generation"`
		Result       acpbridge.CallResult `json:"result"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, "invalid ACP tool result"))
		return
	}
	if err := m.manager.CompleteCall(acpOwner(client), params.CapabilityID, params.CallID, params.Generation, params.Result); err != nil {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, err.Error()))
		return
	}
	_ = client.SendCriticalResponse(protocol.NewOKResponse(req.ID, map[string]bool{"accepted": true}))
}

func (m *ACPToolMethods) handleRelease(_ context.Context, client *gateway.Client, req *protocol.RequestFrame) {
	var params struct {
		CapabilityID string `json:"capabilityId"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil || params.CapabilityID == "" {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, "invalid ACP capability release"))
		return
	}
	if err := m.manager.Release(acpOwner(client), params.CapabilityID); err != nil {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, err.Error()))
		return
	}
	_ = client.SendCriticalResponse(protocol.NewOKResponse(req.ID, map[string]bool{"released": true}))
}

func acpOwner(client *gateway.Client) acpbridge.Owner {
	return acpbridge.Owner{ConnectionID: client.ID(), TenantID: client.TenantID(), UserID: client.UserID()}
}
