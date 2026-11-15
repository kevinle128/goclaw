package tools

import "context"

const ctxACPSession toolContextKey = "tool_acp_session"

// WithACPSession marks the run as one of an ACP client session, such as
// Buzz. The response text of such a run is not posted to the client's chat
// by goclaw, so tools must not tell the model that it is.
func WithACPSession(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxACPSession, true)
}

// IsACPSessionFromCtx reports whether the run belongs to an ACP client session.
func IsACPSessionFromCtx(ctx context.Context) bool {
	v, _ := ctx.Value(ctxACPSession).(bool)
	return v
}
