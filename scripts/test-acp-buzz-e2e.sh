#!/bin/sh
set -eu

gateway_port=18791
provider_port=18792
buzz_relay_port=3001
buzz_health_port=8081
postgres_port=55432
redis_port=56379
cleanup_deadline=10

fail() {
  printf '%s\n' "ACP Buzz E2E: $*" >&2
  exit 1
}

for command in cargo go lsof; do
  command -v "$command" >/dev/null 2>&1 || fail "required command not found: $command"
done

[ -n "${BUZZ_REPO:-}" ] || fail "BUZZ_REPO must name an absolute Buzz checkout"
case "$BUZZ_REPO" in
  /*) ;;
  *) fail "BUZZ_REPO must be absolute" ;;
esac
[ -f "$BUZZ_REPO/Cargo.toml" ] || fail "BUZZ_REPO is not a Buzz checkout: $BUZZ_REPO"
[ -f tests/fixtures/acp_openai_stub/main.go ] || fail "ACP OpenAI stub fixture is missing"
grep -q 'func TestACPBuzzE2E' tests/integration/acp_server_test.go 2>/dev/null || fail "TestACPBuzzE2E is missing"

for port in "$gateway_port" "$provider_port" "$buzz_relay_port" "$buzz_health_port" "$postgres_port" "$redis_port"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2 || true
    fail "deterministic port $port is already in use"
  fi
done

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/goclaw-acp-buzz.XXXXXX")
pids=""

cleanup() {
  status=$?
  trap - EXIT INT TERM
  for pid in $pids; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  remaining=$cleanup_deadline
  while [ "$remaining" -gt 0 ]; do
    alive=false
    for pid in $pids; do
      if kill -0 "$pid" 2>/dev/null; then
        alive=true
      fi
    done
    [ "$alive" = false ] && break
    sleep 1
    remaining=$((remaining - 1))
  done
  for pid in $pids; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$work_dir"
  exit "$status"
}
trap cleanup EXIT INT TERM

mkdir -p "$work_dir/bin" "$work_dir/workspace/project"
go build -o "$work_dir/bin/goclaw" .
go build -o "$work_dir/bin/acp-openai-stub" ./tests/fixtures/acp_openai_stub
(
  cd "$BUZZ_REPO"
  cargo build -p buzz-relay -p buzz-cli -p buzz-acp -p buzz-agent -p buzz-dev-mcp
)

buzz_bin_dir="$BUZZ_REPO/target/debug"
for binary in buzz-relay buzz buzz-acp buzz-agent buzz-dev-mcp; do
  [ -x "$buzz_bin_dir/$binary" ] || fail "Buzz build did not produce $buzz_bin_dir/$binary"
done

tenant_id=00000000-0000-0000-0000-000000000101
owner_id=acp-e2e-owner
agent_key=acp-e2e-agent
acp_api_key=acp-e2e-operator-key

export GOCLAW_ACP_E2E_WORK_DIR="$work_dir"
export GOCLAW_ACP_E2E_GOCLAW="$work_dir/bin/goclaw"
export GOCLAW_ACP_E2E_PROVIDER="$work_dir/bin/acp-openai-stub"
export GOCLAW_ACP_E2E_GATEWAY_PORT="$gateway_port"
export GOCLAW_ACP_E2E_PROVIDER_PORT="$provider_port"
export GOCLAW_ACP_E2E_BUZZ_RELAY_PORT="$buzz_relay_port"
export GOCLAW_ACP_E2E_BUZZ_HEALTH_PORT="$buzz_health_port"
export GOCLAW_ACP_E2E_POSTGRES_PORT="$postgres_port"
export GOCLAW_ACP_E2E_REDIS_PORT="$redis_port"
export GOCLAW_ACP_E2E_BUZZ_BIN_DIR="$buzz_bin_dir"
export GOCLAW_ACP_API_KEY="$acp_api_key"
export BUZZ_ACP_AGENT_COMMAND="$work_dir/bin/goclaw"
export BUZZ_ACP_AGENT_ARGS="acp,--agent,$agent_key,--owner,$owner_id,--tenant,$tenant_id,--workspace-root,$work_dir/workspace,--mcp-exec-root,$buzz_bin_dir,--server,ws://127.0.0.1:$gateway_port"
export BUZZ_ACP_MCP_COMMAND="$buzz_bin_dir/buzz-dev-mcp"

go test -tags integration ./tests/integration -run '^TestACPBuzzE2E$' -count=1 -timeout 10m &
test_pid=$!
pids="$test_pid"
wait "$test_pid"
pids=""

for port in "$gateway_port" "$provider_port" "$buzz_relay_port" "$buzz_health_port" "$postgres_port" "$redis_port"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "port $port is still listening after the E2E test"
  fi
done
