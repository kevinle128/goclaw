package methods

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
	"github.com/nextlevelbuilder/goclaw/internal/gateway"
	"github.com/nextlevelbuilder/goclaw/internal/i18n"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// ACPInputMethods handles acp.input.respond, the answer of the adapter to an
// acp.input.request event.
type ACPInputMethods struct {
	relays *acpbridge.InputRelays
}

// NewACPInputMethods creates the handler. With a nil registry the method
// answers that the relay is unavailable.
func NewACPInputMethods(relays *acpbridge.InputRelays) *ACPInputMethods {
	return &ACPInputMethods{relays: relays}
}

func (m *ACPInputMethods) Register(router *gateway.MethodRouter) {
	router.Register(protocol.MethodACPInputRespond, m.handleRespond)
}

func (m *ACPInputMethods) handleRespond(ctx context.Context, client *gateway.Client, req *protocol.RequestFrame) {
	locale := store.LocaleFromContext(ctx)
	if m.relays == nil {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgACPRelayUnavailable)))
		return
	}
	var params struct {
		SessionKey    string          `json:"sessionKey"`
		ACPGeneration uint64          `json:"acpGeneration"`
		RequestID     string          `json:"requestId"`
		Action        string          `json:"action"`
		Content       json.RawMessage `json:"content,omitempty"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil || params.SessionKey == "" || params.ACPGeneration == 0 || params.RequestID == "" {
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgACPInvalidParams, protocol.MethodACPInputRespond)))
		return
	}
	err := m.relays.Respond(client.ID(), params.SessionKey, params.ACPGeneration, params.RequestID, params.Action, params.Content)
	switch {
	case err == nil:
		_ = client.SendCriticalResponse(protocol.NewOKResponse(req.ID, map[string]any{}))
	case errors.Is(err, acpbridge.ErrNotFound):
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrNotFound, i18n.T(locale, i18n.MsgACPRequestNotFound)))
	case errors.Is(err, acpbridge.ErrNotOwner):
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrUnauthorized, i18n.T(locale, i18n.MsgACPRequestNotOwner)))
	default:
		_ = client.SendCriticalResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgACPInvalidParams, protocol.MethodACPInputRespond)))
	}
}
