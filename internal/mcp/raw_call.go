package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	mcpclient "github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// Identity that goclaw sends as the MCP client.
const (
	clientInfoName    = "goclaw"
	clientInfoVersion = "1.0.0"
)

// Method names that the raw request layer handles specially.
const (
	methodToolsCall    = "tools/call"
	methodTasksGet     = "tasks/get"
	methodTasksUpdate  = "tasks/update"
	methodTasksCancel  = "tasks/cancel"
	methodEventsList   = "events/list"
	methodEventsPoll   = "events/poll"
	methodEventsStream = "events/stream"
)

// defaultRawCallTimeout bounds every raw request except events/stream. A
// server that ignores an unknown method never answers, so a request without
// a deadline can wait forever.
const defaultRawCallTimeout = 30 * time.Second

// rawRequestSeq makes request ids unique in the process. The ids are strings
// with the prefix "goclaw-", so they cannot collide with the integer ids of
// the mcp-go client on the same connection.
var rawRequestSeq atomic.Uint64

// nextRawRequestID returns a new request id "goclaw-<n>". A stream consumer
// gets the id first, registers it in the notification router, and then
// passes it in RawOpts.ID, because the server tags every stream frame with
// the request id as subscriptionId.
func nextRawRequestID() string {
	return "goclaw-" + strconv.FormatUint(rawRequestSeq.Add(1), 10)
}

// RawOpts selects the per-request client capabilities and limits of a raw
// request.
type RawOpts struct {
	// ID is the JSON-RPC request id. Empty means a new id from nextRawRequestID.
	ID string
	// Tasks declares the tasks extension in _meta clientCapabilities.
	Tasks bool
	// Events declares the events extension in _meta clientCapabilities.
	Events bool
	// ElicitationForm declares elicitation in form mode.
	ElicitationForm bool
	// Timeout overrides the default deadline (30 s). It has no effect on
	// events/stream, which never gets a deadline from this layer.
	Timeout time.Duration
}

// rawDeadline returns the deadline that rawCall applies to method, and false
// when the method must not get one.
func rawDeadline(method string, opts RawOpts) (time.Duration, bool) {
	if method == methodEventsStream {
		return 0, false
	}
	if opts.Timeout > 0 {
		return opts.Timeout, true
	}
	return defaultRawCallTimeout, true
}

// rawCall sends one JSON-RPC request on a modern (2026-07-28) connection
// through the transport of client, and returns the raw result.
//
// The typed mcp-go methods drop or reject the fields of the tasks and events
// extensions, so goclaw sends those requests itself. The transport does not
// add the per-request _meta or the Mcp-Method and Mcp-Name headers on this
// path, so rawCall adds them. The transport adds MCP-Protocol-Version from
// the negotiated version.
//
// A JSON-RPC error comes back as *RPCError. On a modern connection there is
// no session, so an HTTP 404 means "method not found"; rawCall reports it as
// ErrMethodNotFound and not as a session reset.
func rawCall(ctx context.Context, client *mcpclient.Client, method string, params any, opts RawOpts) (json.RawMessage, error) {
	if client == nil {
		return nil, errors.New("mcp raw call: no client")
	}
	version := client.ProtocolVersion()
	if eraFromVersion(version) != EraModern {
		return nil, fmt.Errorf("mcp raw call %s: connection is not on protocol 2026-07-28 (negotiated %q)", method, version)
	}

	fields, err := paramsToFields(params)
	if err != nil {
		return nil, fmt.Errorf("mcp raw call %s: %w", method, err)
	}
	meta, err := buildRequestMeta(fields["_meta"], version, opts)
	if err != nil {
		return nil, fmt.Errorf("mcp raw call %s: %w", method, err)
	}
	fields["_meta"] = meta

	header := make(http.Header, 2)
	header.Set(mcpgo.HeaderMethod, method)
	if name, ok := headerNameFor(method, fields); ok {
		header.Set(mcpgo.HeaderName, name)
	}

	id := opts.ID
	if id == "" {
		id = nextRawRequestID()
	}

	if d, ok := rawDeadline(method, opts); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}

	resp, err := client.GetTransport().SendRequest(ctx, transport.JSONRPCRequest{
		JSONRPC: mcpgo.JSONRPC_VERSION,
		ID:      mcpgo.NewRequestId(id),
		Method:  method,
		Params:  fields,
		Header:  header,
	})
	if err != nil {
		if errors.Is(err, transport.ErrSessionTerminated) {
			return nil, &RPCError{Code: CodeMethodNotFound, Message: "method not found (HTTP 404)", Method: method}
		}
		return nil, fmt.Errorf("mcp raw call %s: %w", method, err)
	}
	if resp == nil {
		return nil, fmt.Errorf("mcp raw call %s: empty response", method)
	}
	if resp.Error != nil {
		return nil, rpcErrorFrom(method, resp.Error)
	}
	return resp.Result, nil
}

// paramsToFields turns params (a struct, a map, or nil) into a JSON object
// that can carry _meta.
func paramsToFields(params any) (map[string]json.RawMessage, error) {
	fields := map[string]json.RawMessage{}
	if params == nil {
		return fields, nil
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("encode params: %w", err)
	}
	if string(encoded) == "null" {
		return fields, nil
	}
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, fmt.Errorf("params must be a JSON object: %w", err)
	}
	return fields, nil
}

// buildRequestMeta merges the protocol metadata of 2026-07-28 into an
// existing _meta object.
func buildRequestMeta(existing json.RawMessage, version string, opts RawOpts) (json.RawMessage, error) {
	meta := map[string]any{}
	if len(existing) > 0 && string(existing) != "null" {
		if err := json.Unmarshal(existing, &meta); err != nil {
			return nil, fmt.Errorf("_meta must be a JSON object: %w", err)
		}
	}
	meta[mcpgo.MetaKeyProtocolVersion] = version
	meta[mcpgo.MetaKeyClientCapabilities] = requestCapabilities(opts)
	meta[mcpgo.MetaKeyClientInfo] = map[string]string{"name": clientInfoName, "version": clientInfoVersion}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("encode _meta: %w", err)
	}
	return encoded, nil
}

// requestCapabilities builds the per-request clientCapabilities object.
func requestCapabilities(opts RawOpts) map[string]any {
	caps := map[string]any{}
	extensions := map[string]any{}
	if opts.Tasks {
		extensions[ExtensionTasks] = map[string]any{}
	}
	if opts.Events {
		extensions[ExtensionEvents] = map[string]any{}
	}
	if len(extensions) > 0 {
		caps["extensions"] = extensions
	}
	if opts.ElicitationForm {
		caps["elicitation"] = map[string]any{"form": map[string]any{}}
	}
	return caps
}

// headerNameFor returns the Mcp-Name header value for method: params.name for
// tools/call and events/poll|stream, params.taskId for tasks/*. events/list
// and other methods have no Mcp-Name. Values that are not plain ASCII use the
// Base64 sentinel form "=?base64?...?=".
func headerNameFor(method string, fields map[string]json.RawMessage) (string, bool) {
	var key string
	switch method {
	case methodToolsCall, methodEventsPoll, methodEventsStream:
		key = "name"
	case methodTasksGet, methodTasksUpdate, methodTasksCancel:
		key = "taskId"
	default:
		return "", false
	}
	raw, ok := fields[key]
	if !ok {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", false
	}
	return mcpgo.EncodeHeaderValue(value)
}

// rpcErrorFrom converts a JSON-RPC error object to *RPCError.
func rpcErrorFrom(method string, details *mcpgo.JSONRPCErrorDetails) *RPCError {
	e := &RPCError{Code: details.Code, Message: details.Message, Method: method}
	if details.Data != nil {
		if data, err := json.Marshal(details.Data); err == nil {
			e.Data = data
		}
	}
	return e
}
