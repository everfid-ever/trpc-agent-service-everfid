#!/usr/bin/env bash
# Verifies P2 recovery: a node is killed after model/tool execution but before
# result persistence and terminal commit, and the peer finishes the input.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/deploy/compose/docker-compose.local.yml"
secret_file="${repo_root}/deploy/compose/secrets/deepseek-api-key"
project="trpc-local-inflight-${RANDOM}${RANDOM}"
port_base="$((20000 + RANDOM))"
port_postgres="${port_base}"
port_redis="$((port_base + 1))"
port_jaeger="$((port_base + 2))"
port_otel="$((port_base + 3))"
port_metrics="$((port_base + 4))"
port_a="$((port_base + 5))"
port_b="$((port_base + 6))"
token="${TRPC_WEBUI_LOCAL_TOKEN:-local-webui-token-change-me}"
route="${TRPC_WEBUI_LOCAL_ROUTE_KEY:-local-webui}"
account="local-account"
diagnostics="$(mktemp -d "${TMPDIR:-/tmp}/trpc-inflight-takeover.XXXXXX")"

case "${1:-p2}" in
  p2) ;;
  *) echo "usage: $0 [p2]" >&2; exit 2 ;;
esac

command -v docker >/dev/null 2>&1 || { echo "Docker Desktop is required" >&2; exit 2; }
command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 2; }
command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 2; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required" >&2; exit 2; }
test -s "${secret_file}" || { echo "DeepSeek key file is required at ${secret_file}" >&2; exit 2; }

compose() {
  TRPC_LOCAL_POSTGRES_PORT="${port_postgres}" TRPC_LOCAL_REDIS_PORT="${port_redis}" \
    TRPC_LOCAL_JAEGER_PORT="${port_jaeger}" TRPC_LOCAL_OTEL_HTTP_PORT="${port_otel}" TRPC_LOCAL_OTEL_PROMETHEUS_PORT="${port_metrics}" \
    TRPC_LOCAL_MULTINODE_NODE_A_PORT="${port_a}" TRPC_LOCAL_MULTINODE_NODE_B_PORT="${port_b}" \
    docker compose --project-name "${project}" -f "${compose_file}" --profile webui-multinode "$@"
}

cleanup() {
  local status=$?
  if [[ ${status} -ne 0 ]]; then
    compose logs --no-color >"${diagnostics}/compose.log" || true
    echo "in-flight takeover smoke failed; diagnostics retained at ${diagnostics}/compose.log" >&2
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  [[ ${status} -ne 0 ]] || rmdir "${diagnostics}" || true
  exit "${status}"
}
trap cleanup EXIT

wait_ready() {
  local port="$1"
  for _ in $(seq 1 120); do
    curl --fail --silent --max-time 2 "http://127.0.0.1:${port}/readyz" >/dev/null && return 0
    sleep 1
  done
  return 1
}

signature() {
  local timestamp="$1" nonce="$2" payload="$3"
  printf '%s\n%s\n%s' "${timestamp}" "${nonce}" "${payload}" |
    openssl dgst -sha256 -hmac "${token}" -hex | awk '{print $2}'
}

send_message() {
  local port="$1" user="$2" chat="$3" message_id="$4" text="$5"
  local occurred timestamp nonce body uri sig
  occurred="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  timestamp="$(date +%s)"
  nonce="$(openssl rand -hex 16)"
  body=$(printf '{"schema_version":1,"external_account_id":"%s","external_message_id":"%s","external_user_id":"%s","external_chat_id":"%s","conversation_type":"p2p","message_type":"text","text":"%s","occurred_at":"%s"}' \
    "${account}" "${message_id}" "${user}" "${chat}" "${text}" "${occurred}")
  uri="/webui/api/messages?route_key=${route}"
  sig="$(signature "${timestamp}" "${nonce}" "${body}")"
  curl --fail --silent --show-error --max-time 15 -X POST "http://127.0.0.1:${port}${uri}" \
    -H "Content-Type: application/json" -H "X-WebUI-Timestamp: ${timestamp}" -H "X-WebUI-Nonce: ${nonce}" -H "X-WebUI-Signature: ${sig}" \
    --data-binary "${body}" >/dev/null
}

wait_barrier_hit() {
  local port="$1" body
  for _ in $(seq 1 180); do
    body="$(curl --fail --silent --show-error --max-time 5 "http://127.0.0.1:${port}/test/failover/p2/status" -H "X-TRPC-Local-Token: ${token}")"
    [[ "${body}" == *'"hit":true'* ]] && return 0
    sleep 1
  done
  return 1
}

wait_reply() {
  local port="$1" user="$2" chat="$3" expected="$4"
  local timestamp nonce uri sig body
  uri="/webui/api/replies?route_key=${route}&external_user_id=${user}&external_chat_id=${chat}"
  for _ in $(seq 1 180); do
    timestamp="$(date +%s)"
    nonce="$(openssl rand -hex 16)"
    sig="$(signature "${timestamp}" "${nonce}" $'GET\n'"${uri}")"
    body="$(curl --fail --silent --show-error --max-time 10 "http://127.0.0.1:${port}${uri}" \
      -H "X-WebUI-Timestamp: ${timestamp}" -H "X-WebUI-Nonce: ${nonce}" -H "X-WebUI-Signature: ${sig}")"
    [[ "${body}" == *"${expected}"* ]] && return 0
    sleep 1
  done
  return 1
}

arm_p2() {
  local port="$1"
  curl --fail --silent --show-error --max-time 5 -X POST "http://127.0.0.1:${port}/test/failover/p2/arm" \
    -H "X-TRPC-Local-Token: ${token}" >/dev/null
}

compose up --detach --build
wait_ready "${port_a}"
wait_ready "${port_b}"

message_id="p2-takeover-$(openssl rand -hex 8)"
user="p2-user"
chat="p2-chat"
arm_p2 "${port_a}"
send_message "${port_a}" "${user}" "${chat}" "${message_id}" "Reply exactly ${message_id}"
wait_barrier_hit "${port_a}"
# SIGKILL bypasses the worker's graceful drain while it owns the P2 lease.
compose kill -s KILL webui-node-a
wait_ready "${port_b}"
wait_reply "${port_b}" "${user}" "${chat}" "${message_id}"

echo "in-flight takeover smoke passed: P2 resumed after SIGKILL with one durable visible reply"
