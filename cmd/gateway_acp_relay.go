package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
	"github.com/nextlevelbuilder/goclaw/internal/agent"
	"github.com/nextlevelbuilder/goclaw/internal/gateway"
	"github.com/nextlevelbuilder/goclaw/internal/gateway/methods"
	mcpbridge "github.com/nextlevelbuilder/goclaw/internal/mcp"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// acpRelay holds the gateway parts of ACP wake delivery and of the in-turn
// input relay: the wake sinks of the adapter connections, the open input
// requests, and the dispatcher of due wake rows.
type acpRelay struct {
	sinks      *acpbridge.WakeSinks
	inputs     *acpbridge.InputRelays
	dispatcher *mcpbridge.WakeDispatcher
}

// newACPRelay creates the registries. With a task watch manager it also
// creates the wake dispatcher: a new wake row and a new subscription wake
// it.
func newACPRelay(taskWatch *mcpbridge.TaskWatchManager, servers store.MCPServerStore) *acpRelay {
	r := &acpRelay{sinks: acpbridge.NewWakeSinks(0), inputs: acpbridge.NewInputRelays()}
	if taskWatch == nil || servers == nil {
		return r
	}
	dispatcher, err := mcpbridge.NewWakeDispatcher(mcpbridge.WakeDispatcherConfig{Tasks: taskWatch.Tasks(), Servers: servers, Deliverer: r.sinks})
	if err != nil {
		slog.Warn("mcp.wake.dispatcher_disabled", "error", err)
		return r
	}
	taskWatch.SetWakeNotifier(dispatcher.Kick)
	r.sinks.SetOnSubscribe(dispatcher.KickNoSink)
	r.dispatcher = dispatcher
	return r
}

func (r *acpRelay) start() {
	if r.dispatcher != nil {
		r.dispatcher.Start()
	}
}

func (r *acpRelay) stop() {
	if r.dispatcher != nil {
		r.dispatcher.Stop()
	}
}

// register adds the acp.wake.* and acp.input.* methods, gives the input
// relay to chat.send, and releases the relay state of a closed connection.
func (r *acpRelay) register(server *gateway.Server, agents *agent.Router, chat *methods.ChatMethods) {
	chat.SetACPInputRelays(r.inputs)
	router := server.Router()
	methods.NewACPWakeMethods(r.sinks, routerAgentResolver(agents)).Register(router)
	methods.NewACPInputMethods(r.inputs).Register(router)
	server.SetACPDisconnectHandler(func(connectionID string) {
		chat.DiscardACPConnection(connectionID)
		r.inputs.ReleaseConnection(connectionID)
		r.sinks.ReleaseConnection(connectionID)
	})
}

// routerAgentResolver resolves an agent like chat.send does: in the tenant of
// the request context.
func routerAgentResolver(agents *agent.Router) methods.ACPAgentResolver {
	return func(ctx context.Context, agentID string) (uuid.UUID, error) {
		ag, err := agents.Get(ctx, agentID)
		if err != nil {
			return uuid.Nil, err
		}
		id := ag.UUID()
		if id == uuid.Nil {
			return uuid.Nil, fmt.Errorf("agent %q has no id", agentID)
		}
		return id, nil
	}
}
