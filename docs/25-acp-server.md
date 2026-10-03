# 25 — ACP Server

GoClaw can run as an Agent Client Protocol version 1 server for Buzz.
The command uses newline-delimited JSON on standard input and standard output and uses one authenticated Gateway WebSocket connection for agent work.

## Start the server

Create an API key that has `operator.write`, an owner, and the expected tenant.
Do not use the broad Gateway token for this command.

```sh
export GOCLAW_ACP_API_KEY="goclaw_..."

goclaw --server ws://127.0.0.1:18790 acp \
  --agent coding-agent \
  --owner user-123 \
  --tenant 00000000-0000-0000-0000-000000000001 \
  --workspace-root /absolute/path/to/workspace \
  --mcp-exec-root /absolute/path/to/approved/bin
```

Use `wss://` when the Gateway is not on an exact loopback address.
The command rejects remote plaintext connections.

The API key can also be supplied with `--api-key`.
Command-line arguments can be visible to other local processes, so an environment variable or process supervisor is safer.
The command does not read `--token`, `GOCLAW_GATEWAY_TOKEN`, or the Gateway token from `config.json`.

## ACP compatibility

The server implements ACP version 1.
Current Buzz builds send a legacy-shaped initialize request with `protocolVersion: 2`.
GoClaw answers with protocol version 1, and Buzz places its standing instructions in the first prompt.
GoClaw does not claim support for official ACP version 2.

The first release supports `initialize`, `session/new`, `session/prompt`, `session/cancel`, and `session/update`.
It does not support `session/load`, client-driven session release, image or audio prompts, HTTP MCP, SSE MCP, or automatic prompt replay.

## Sessions and cancellation

Each ACP session gets an opaque ID and an internal Gateway session key for the fixed `--agent` value.
One prompt can run in each ACP session, and different ACP sessions can run concurrently.
Cancellation keeps the ACP session available for a later prompt.
Idle expiry, input EOF, signal shutdown, or adapter shutdown releases the session and its local MCP processes.

A connection loss after `chat.send` is indeterminate.
GoClaw does not replay that prompt because it can already have produced side effects.

## Buzz MCP tools

Buzz can send structured stdio MCP server definitions in `session/new`.
GoClaw starts those processes in the adapter, not in the Gateway.
The executable must be inside an approved `--mcp-exec-root`, and the working directory must stay inside `--workspace-root` after symlink resolution.

MCP environment values stay in the adapter process.
The Gateway receives filtered tool schemas and bounded tool results, but it does not receive MCP commands, arguments, environment values, or secrets.
Dynamic tools remain subject to the normal GoClaw tool policy for the bound run.

## Resource limits

The adapter limits input lines, output frames, prompt size, active sessions, queued frames, pending requests, MCP servers, discovered tools, and Gateway frame size.
The adapter reserves 480 KiB for serialized Gateway frames below the Gateway 512 KiB connection limit.
Queue overflow on an ACP-critical frame fails the owning connection instead of silently dropping the frame.

## Shutdown

On EOF, `SIGINT`, or `SIGTERM`, the command stops accepting ACP input, cancels and waits for owned work, releases capability leases, terminates local MCP process trees, and closes the Gateway connection.
The Gateway also drains connection-owned ACP work before it closes providers or stores.
