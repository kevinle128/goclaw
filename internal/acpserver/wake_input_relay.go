package acpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// ACP methods that GoClaw sends to the client.
const (
	MethodBuzzWake          = "_buzz/wake"
	MethodElicitationCreate = "elicitation/create"
)

const (
	// WakeRequestTimeout bounds the wait for the _buzz/wake result. The
	// client answers when it queued or refused the turn, not after it.
	WakeRequestTimeout = 30 * time.Second
	// MaxWakeParamsBytes bounds the params of one _buzz/wake request.
	MaxWakeParamsBytes = 16 << 10
	// MaxConcurrentWakes bounds the wakes that wait for the client. It
	// matches the transport limit of _buzz/wake requests.
	MaxConcurrentWakes = MaxPendingOutboundWakes
	// wakeSubscribeRetryMin and wakeSubscribeRetryMax bound the delay between
	// subscribe attempts while the Gateway is unavailable.
	wakeSubscribeRetryMin = time.Second
	wakeSubscribeRetryMax = 30 * time.Second
	// gatewayReplyTimeout bounds acp.wake.ack and acp.input.respond.
	gatewayReplyTimeout = 10 * time.Second
)

// handleInitialized starts the wake subscription when the client declared
// buzz.wake exactly {"version": 1}.
func (s *Server) handleInitialized(_ context.Context, caps ClientCapabilities) {
	if !caps.Wake {
		return
	}
	s.goBackground(s.runWakeSubscription)
}

// goBackground runs fn on a goroutine that Run waits for. It reports false
// when Run is not active; then fn does not run.
func (s *Server) goBackground(fn func(ctx context.Context)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifeCtx == nil {
		return false
	}
	ctx := s.lifeCtx
	s.background.Go(func() { fn(ctx) })
	return true
}

// runWakeSubscription keeps one acp.wake.subscribe on the live Gateway
// connection: it subscribes, waits until that connection drops, then
// reconnects and subscribes again. An idle adapter thus gets wakes again
// after a Gateway restart.
func (s *Server) runWakeSubscription(ctx context.Context) {
	delay := wakeSubscribeRetryMin
	for ctx.Err() == nil {
		done, err := s.subscribeWakes(ctx)
		var rejected *GatewayResponseError
		switch {
		case err == nil:
			delay = wakeSubscribeRetryMin
			slog.Info("acp.wake.subscribed", "agent", s.agentID)
		case done != nil && errors.As(err, &rejected):
			// The Gateway answered the subscribe: an older Gateway does not
			// know the method, or the key may not run the agent. Try again
			// only on the next connection. A refused connect has no done
			// channel and goes to the retry below.
			slog.Warn("acp.wake.subscribe_rejected", "agent", s.agentID, "code", rejected.Code)
		default:
			if ctx.Err() != nil {
				return
			}
			slog.Warn("acp.wake.subscribe_failed", "agent", s.agentID, "error", err, "retry_in", delay.String())
			if !sleepContext(ctx, delay) {
				return
			}
			delay = min(2*delay, wakeSubscribeRetryMax)
			continue
		}
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
}

// subscribeWakes connects when needed and sends acp.wake.subscribe. done
// closes when the connection of the subscription drops.
func (s *Server) subscribeWakes(ctx context.Context) (<-chan struct{}, error) {
	if err := s.gateway.EnsureConnected(ctx); err != nil {
		return nil, err
	}
	done := s.gateway.connDone()
	callCtx, cancel := context.WithTimeout(ctx, GatewayControlTimeout)
	defer cancel()
	if err := s.gateway.Call(callCtx, protocol.MethodACPWakeSubscribe, map[string]string{"agentId": s.agentID}, nil); err != nil {
		return done, err
	}
	return done, nil
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// handleGatewayWake runs on the Gateway read loop; the delivery runs on its
// own goroutine.
func (s *Server) handleGatewayWake(wake GatewayWake) {
	select {
	case s.wakeSlots <- struct{}{}:
	default:
		slog.Warn("acp.wake.too_many", "wake_id", wake.WakeID)
		go s.ackWake(context.Background(), wake.WakeID, protocol.ACPWakeOutcomeFailed)
		return
	}
	started := s.goBackground(func(ctx context.Context) {
		defer func() { <-s.wakeSlots }()
		s.ackWake(ctx, wake.WakeID, s.deliverWake(ctx, wake))
	})
	if !started {
		// The ACP connection ended: this process cannot reach the client.
		<-s.wakeSlots
		go s.ackWake(context.Background(), wake.WakeID, protocol.ACPWakeOutcomeUnsupported)
	}
}

// deliverWake sends _buzz/wake to the client and maps the answer to an ack
// outcome.
func (s *Server) deliverWake(ctx context.Context, wake GatewayWake) string {
	transport := s.currentTransport()
	if transport == nil || !transport.ClientCapabilities().Wake {
		return protocol.ACPWakeOutcomeUnsupported
	}
	var params struct {
		WakeID string `json:"wakeId"`
	}
	if len(wake.Params) > MaxWakeParamsBytes || json.Unmarshal(wake.Params, &params) != nil || params.WakeID != wake.WakeID {
		slog.Warn("acp.wake.invalid_params", "wake_id", wake.WakeID, "bytes", len(wake.Params))
		return protocol.ACPWakeOutcomeFailed
	}
	requestCtx, cancel := context.WithTimeout(ctx, WakeRequestTimeout)
	defer cancel()
	result, rpcErr, err := transport.Request(requestCtx, MethodBuzzWake, wake.Params)
	return wakeOutcome(result, rpcErr, err)
}

// wakeOutcome maps the client answer: accepted, duplicate and busy pass
// through, -32601 is unsupported, and any other error, timeout or result is
// failed.
func wakeOutcome(result json.RawMessage, rpcErr *RPCError, err error) string {
	switch {
	case err != nil:
		return protocol.ACPWakeOutcomeFailed
	case rpcErr != nil && rpcErr.Code == CodeMethodNotFound:
		return protocol.ACPWakeOutcomeUnsupported
	case rpcErr != nil:
		return protocol.ACPWakeOutcomeFailed
	}
	var answer struct {
		Outcome string `json:"outcome"`
	}
	if json.Unmarshal(result, &answer) != nil {
		return protocol.ACPWakeOutcomeFailed
	}
	switch answer.Outcome {
	case protocol.ACPWakeOutcomeAccepted, protocol.ACPWakeOutcomeDuplicate, protocol.ACPWakeOutcomeBusy:
		return answer.Outcome
	}
	return protocol.ACPWakeOutcomeFailed
}

func (s *Server) ackWake(ctx context.Context, wakeID, outcome string) {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayReplyTimeout)
	defer cancel()
	if err := s.gateway.Call(ackCtx, protocol.MethodACPWakeAck, map[string]string{"wakeId": wakeID, "outcome": outcome}, nil); err != nil {
		slog.Warn("acp.wake.ack_failed", "wake_id", wakeID, "outcome", outcome, "error", err)
	}
}

// handleInputRequest relays one acp.input.request of the active prompt to
// the client as elicitation/create and sends the answer back. A client
// error or a transport error gives the answer "cancel".
func (s *Server) handleInputRequest(ctx context.Context, sessionID string, record *sessionRecord, event GatewayEvent, generation uint64) {
	var request struct {
		SessionKey      string          `json:"sessionKey"`
		Generation      uint64          `json:"acpGeneration"`
		RequestID       string          `json:"requestId"`
		ToolCallID      string          `json:"toolCallId"`
		Mode            string          `json:"mode"`
		Message         string          `json:"message"`
		RequestedSchema json.RawMessage `json:"requestedSchema"`
	}
	if json.Unmarshal(event.Payload, &request) != nil || request.SessionKey != record.SessionKey || request.Generation != generation || request.RequestID == "" {
		return
	}
	action, content := "cancel", json.RawMessage(nil)
	transport := s.currentTransport()
	if transport != nil && transport.ClientCapabilities().ElicitationForm && request.Mode == "form" && isJSONObject(request.RequestedSchema) {
		params := map[string]any{"sessionId": sessionID, "mode": "form", "message": request.Message, "requestedSchema": request.RequestedSchema}
		if request.ToolCallID != "" {
			params["toolCallId"] = request.ToolCallID
		}
		// No deadline beyond the prompt: people may answer late.
		result, rpcErr, err := transport.Request(ctx, MethodElicitationCreate, params)
		switch {
		case err != nil:
			slog.Warn("acp.input.request_failed", "session", sessionID, "error", err)
		case rpcErr != nil:
			slog.Warn("acp.input.client_error", "session", sessionID, "code", rpcErr.Code)
		default:
			action, content = elicitationAnswer(result)
		}
	}
	reply := map[string]any{"sessionKey": request.SessionKey, "acpGeneration": request.Generation, "requestId": request.RequestID, "action": action}
	if content != nil {
		reply["content"] = content
	}
	replyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayReplyTimeout)
	defer cancel()
	if err := s.gateway.Call(replyCtx, protocol.MethodACPInputRespond, reply, nil); err != nil {
		slog.Warn("acp.input.respond_failed", "session", sessionID, "error", err)
	}
}

// elicitationAnswer reads an elicitation result. The content of an accept
// answer goes back unchanged; a bad result, or content over the Gateway
// limit of an answer, is "cancel".
func elicitationAnswer(result json.RawMessage) (string, json.RawMessage) {
	var answer struct {
		Action  string          `json:"action"`
		Content json.RawMessage `json:"content"`
	}
	if json.Unmarshal(result, &answer) != nil {
		return "cancel", nil
	}
	switch answer.Action {
	case "accept":
		if len(answer.Content) > acpbridge.MaxInputContentBytes || !isJSONObject(answer.Content) {
			return "cancel", nil
		}
		return "accept", bytes.Clone(answer.Content)
	case "decline", "cancel":
		return answer.Action, nil
	}
	return "cancel", nil
}

func isJSONObject(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(trimmed, &object) == nil
}

func (s *Server) currentTransport() *Transport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transport
}
