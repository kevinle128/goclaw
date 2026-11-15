package methods

import (
	"context"
	"encoding/json"

	"github.com/nextlevelbuilder/goclaw/internal/agent"
	"github.com/nextlevelbuilder/goclaw/internal/gateway"
	"github.com/nextlevelbuilder/goclaw/internal/i18n"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// handleSteer injects a message into the run that is active on a session. It
// never starts a run: when the session is idle, or the run cannot take the
// message, it answers injected=false with a reason. The answer comes as soon
// as the message is queued, not when the loop reads it.
//
// Params:
//
//	{ sessionKey: string, message: string }
//
// Response:
//
//	{ injected: true, runId: string }
//	{ injected: false, reason: "no_active_run" | "inject_full" }
func (m *ChatMethods) handleSteer(ctx context.Context, client *gateway.Client, req *protocol.RequestFrame) {
	locale := store.LocaleFromContext(ctx)
	var params struct {
		SessionKey string `json:"sessionKey"`
		Message    string `json:"message"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgInvalidJSON)))
		return
	}
	if params.SessionKey == "" {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgRequired, "sessionKey")))
		return
	}
	if params.Message == "" {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgRequired, "message")))
		return
	}
	if !requireSessionOwner(ctx, m.sessions, m.cfg, client, req.ID, params.SessionKey) {
		return
	}

	outcome := m.agents.InjectMessageDetailed(params.SessionKey, agent.InjectedMessage{
		Content: params.Message,
		UserID:  client.UserID(),
	})
	if !outcome.Injected {
		client.SendResponse(protocol.NewOKResponse(req.ID, map[string]any{
			"injected": false,
			"reason":   outcome.Reason,
		}))
		return
	}
	client.SendResponse(protocol.NewOKResponse(req.ID, map[string]any{
		"injected": true,
		"runId":    outcome.RunID,
	}))
}
