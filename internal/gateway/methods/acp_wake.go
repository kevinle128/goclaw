package methods

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
	"github.com/nextlevelbuilder/goclaw/internal/gateway"
	"github.com/nextlevelbuilder/goclaw/internal/i18n"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// ACPAgentResolver returns the UUID of an agent that the caller in ctx may
// run, from its agent key or UUID. It returns an error for an agent outside
// the tenant of ctx.
type ACPAgentResolver func(ctx context.Context, agentID string) (uuid.UUID, error)

// ACPWakeMethods handles acp.wake.subscribe and acp.wake.ack.
type ACPWakeMethods struct {
	sinks        *acpbridge.WakeSinks
	resolveAgent ACPAgentResolver
}

// NewACPWakeMethods creates the handlers. With a nil sinks registry the
// methods answer that the relay is unavailable.
func NewACPWakeMethods(sinks *acpbridge.WakeSinks, resolveAgent ACPAgentResolver) *ACPWakeMethods {
	return &ACPWakeMethods{sinks: sinks, resolveAgent: resolveAgent}
}

func (m *ACPWakeMethods) Register(router *gateway.MethodRouter) {
	router.Register(protocol.MethodACPWakeSubscribe, m.handleSubscribe)
	router.Register(protocol.MethodACPWakeAck, m.handleAck)
}

// handleSubscribe binds a wake sink to the authenticated identity of the
// connection and the agent. The agent must resolve in the tenant of the
// connection, as for chat.send.
func (m *ACPWakeMethods) handleSubscribe(ctx context.Context, client *gateway.Client, req *protocol.RequestFrame) {
	locale := store.LocaleFromContext(ctx)
	if m.sinks == nil || m.resolveAgent == nil {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgACPRelayUnavailable)))
		return
	}
	var params struct {
		AgentID string `json:"agentId"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil || params.AgentID == "" {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgACPInvalidParams, protocol.MethodACPWakeSubscribe)))
		return
	}
	if client.UserID() == "" || client.TenantID() == uuid.Nil {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgUserIDRequired)))
		return
	}
	agentUUID, err := m.resolveAgent(ctx, params.AgentID)
	if err != nil || agentUUID == uuid.Nil {
		slog.Warn("security.acp.wake_subscribe_rejected", "connection", client.ID(), "tenant", client.TenantID(), "agent", params.AgentID)
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrUnauthorized, i18n.T(locale, i18n.MsgACPAgentNotAllowed, params.AgentID)))
		return
	}
	err = m.sinks.Subscribe(acpbridge.WakeSink{
		ConnectionID: client.ID(), TenantID: client.TenantID(), AgentID: agentUUID,
		UserID: client.UserID(), Sender: client,
	})
	switch {
	case errors.Is(err, acpbridge.ErrWakeSinkLimit):
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrResourceExhausted, i18n.T(locale, i18n.MsgACPSubscribeLimit)))
		return
	case err != nil:
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgACPInvalidParams, protocol.MethodACPWakeSubscribe)))
		return
	}
	slog.Info("acp.wake.subscribed", "connection", client.ID(), "agent", params.AgentID)
	_ = client.SendCriticalResponse(protocol.NewOKResponse(req.ID, map[string]any{}))
}

// handleAck gives the outcome of one wake to the waiting delivery.
func (m *ACPWakeMethods) handleAck(ctx context.Context, client *gateway.Client, req *protocol.RequestFrame) {
	locale := store.LocaleFromContext(ctx)
	if m.sinks == nil {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgACPRelayUnavailable)))
		return
	}
	var params struct {
		WakeID  string `json:"wakeId"`
		Outcome string `json:"outcome"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil || params.WakeID == "" || !protocol.IsACPWakeOutcome(params.Outcome) {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgACPInvalidParams, protocol.MethodACPWakeAck)))
		return
	}
	if err := m.sinks.Ack(client.ID(), params.WakeID, params.Outcome); err != nil {
		// A late ack after the delivery timed out also lands here.
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrNotFound, i18n.T(locale, i18n.MsgACPRequestNotFound)))
		return
	}
	_ = client.SendCriticalResponse(protocol.NewOKResponse(req.ID, map[string]any{}))
}
