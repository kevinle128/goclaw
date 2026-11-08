package mcp

import (
	"errors"
	"log/slog"
	"strconv"
	"sync"

	mcpclient "github.com/mark3labs/mcp-go/client"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// NotificationHandler receives the notifications of one subscription. It runs
// on the transport reader goroutine, so it must return quickly (for example,
// push into a buffered channel).
type NotificationHandler func(mcpgo.JSONRPCNotification)

// notificationRouter sends server notifications to the stream that they
// belong to. The mcp-go client gives every notification to every
// OnNotification handler, with no link to a request. Streams of 2026-07-28
// (events/stream, subscriptions/listen) tag each frame with
// params._meta["io.modelcontextprotocol/subscriptionId"] = the request id, so
// the router uses that value as the key. Frames with an unknown or missing
// id are dropped.
//
// One router belongs to one client. A reconnect creates a new client and a
// new router; registrations on the old router end with the old client.
type notificationRouter struct {
	mu     sync.RWMutex
	routes map[string]NotificationHandler
}

// errSubscriptionInUse is returned when a subscription id is registered twice.
var errSubscriptionInUse = errors.New("mcp notification router: subscription id already registered")

func newNotificationRouter() *notificationRouter {
	return &notificationRouter{routes: make(map[string]NotificationHandler)}
}

// attachNotificationRouter creates a router and installs it as an
// OnNotification handler of client. The client delivers notifications only
// after Client.Start, which goclaw calls for HTTP and SSE transports.
func attachNotificationRouter(client *mcpclient.Client) *notificationRouter {
	r := newNotificationRouter()
	if client != nil {
		client.OnNotification(r.dispatch)
	}
	return r
}

// Register sends the notifications tagged with subscriptionID to handler
// until the returned function is called. The function is safe to call more
// than once.
func (r *notificationRouter) Register(subscriptionID string, handler NotificationHandler) (func(), error) {
	if subscriptionID == "" || handler == nil {
		return nil, errors.New("mcp notification router: empty subscription id or nil handler")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.routes[subscriptionID]; exists {
		return nil, errSubscriptionInUse
	}
	r.routes[subscriptionID] = handler
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.routes, subscriptionID)
			r.mu.Unlock()
		})
	}, nil
}

// dispatch is the OnNotification handler.
func (r *notificationRouter) dispatch(n mcpgo.JSONRPCNotification) {
	id, ok := subscriptionIDOf(n)
	if !ok {
		return
	}
	r.mu.RLock()
	handler := r.routes[id]
	r.mu.RUnlock()
	if handler == nil {
		slog.Debug("mcp.notification.unrouted", "method", n.Method, "subscription_id", id)
		return
	}
	handler(n)
}

// subscriptionIDOf reads params._meta subscriptionId as a string. goclaw uses
// string ids; a numeric id is formatted in decimal.
func subscriptionIDOf(n mcpgo.JSONRPCNotification) (string, bool) {
	if n.Params.Meta == nil {
		return "", false
	}
	switch v := n.Params.Meta[mcpgo.MetaKeySubscriptionID].(type) {
	case string:
		return v, v != ""
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case int64:
		return strconv.FormatInt(v, 10), true
	default:
		return "", false
	}
}
