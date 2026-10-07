#!/usr/bin/env bash
# Exercises stable callback entry, node rejoin, and reverse SIGKILL takeover
# for the local two-node WeCom composition.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/deploy/compose/docker-compose.local.yml"
secret_key="${repo_root}/deploy/compose/secrets/deepseek-api-key"
secret_wecom="${repo_root}/deploy/compose/secrets/wecom.env"
project="${TRPC_SINGLE_HOST_PROJECT:-trpc-single-host-${RANDOM}${RANDOM}}"
entry_port="${TRPC_LOCAL_WECOM_HA_ENTRY_PORT:-58087}"
node_a_port="${TRPC_LOCAL_WECOM_HA_NODE_A_PORT:-58088}"
node_b_port="${TRPC_LOCAL_WECOM_HA_NODE_B_PORT:-58089}"
timeout_seconds="${TRPC_SINGLE_HOST_TIMEOUT_SECONDS:-120}"
stability_seconds="${TRPC_SINGLE_HOST_STABILITY_SECONDS:-5}"
keep_environment="${TRPC_SINGLE_HOST_KEEP_ENVIRONMENT:-false}"
diagnostics="$(mktemp -d "${TMPDIR:-/tmp}/trpc-single-host.XXXXXX")"

command -v docker >/dev/null 2>&1 || { echo "Docker Desktop is required" >&2; exit 2; }
command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 2; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required" >&2; exit 2; }
test -s "${secret_key}" || { echo "DeepSeek key file is required at ${secret_key}" >&2; exit 2; }
test -s "${secret_wecom}" || { echo "WeCom environment file is required at ${secret_wecom}" >&2; exit 2; }
[[ "${timeout_seconds}" =~ ^[1-9][0-9]*$ ]] || { echo "TRPC_SINGLE_HOST_TIMEOUT_SECONDS must be positive" >&2; exit 2; }
[[ "${stability_seconds}" =~ ^[0-9]+$ ]] || { echo "TRPC_SINGLE_HOST_STABILITY_SECONDS must be non-negative" >&2; exit 2; }

compose() {
  TRPC_LOCAL_WECOM_HA_ENTRY_PORT="${entry_port}" TRPC_LOCAL_WECOM_HA_NODE_A_PORT="${node_a_port}" TRPC_LOCAL_WECOM_HA_NODE_B_PORT="${node_b_port}" \
    docker compose --project-name "${project}" -f "${compose_file}" --profile wecom-ha-local "$@"
}

capture() {
  local label="$1"
  curl --silent --show-error --max-time 3 "http://127.0.0.1:${node_a_port}/statusz" >"${diagnostics}/${label}-node-a.json" || true
  curl --silent --show-error --max-time 3 "http://127.0.0.1:${node_b_port}/statusz" >"${diagnostics}/${label}-node-b.json" || true
  curl --silent --show-error --max-time 3 "http://127.0.0.1:${entry_port}/statusz" >"${diagnostics}/${label}-entry.json" || true
}

cleanup() {
  local status=$?
  capture final
  compose ps >"${diagnostics}/compose-ps.txt" || true
  compose logs --no-color >"${diagnostics}/compose.log" || true
  if [[ "${keep_environment}" != "true" ]]; then
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  echo "single-host diagnostics retained at ${diagnostics}" >&2
  exit "${status}"
}
trap cleanup EXIT

wait_http() {
  local url="$1" description="$2"
  for _ in $(seq 1 "${timeout_seconds}"); do
    curl --fail --silent --show-error --max-time 3 "${url}" >/dev/null && return 0
    sleep 1
  done
  echo "timed out waiting for ${description}" >&2
  return 1
}

wait_entry_backend() {
  local node="$1" healthy="$2" expected
  expected="\"url\":\"http://${node}:8080\",\"healthy\":${healthy}"
  for _ in $(seq 1 "${timeout_seconds}"); do
    if curl --fail --silent --show-error --max-time 3 "http://127.0.0.1:${entry_port}/statusz" | grep -Fq "${expected}"; then
      return 0
    fi
    sleep 1
  done
  echo "entry did not report ${node} healthy=${healthy}" >&2
  return 1
}

assert_stopped() {
  local service="$1" container running
  container="$(compose ps -aq "${service}")"
  [[ -n "${container}" ]] || { echo "missing container for ${service}" >&2; return 1; }
  running="$(docker inspect --format '{{.State.Running}}' "${container}")"
  [[ "${running}" == "false" ]] || { echo "${service} restarted unexpectedly" >&2; return 1; }
}

process_start_id() {
  sed -nE 's/.*"process_start_id":"([^"]+)".*/\1/p' "$1" | head -n 1
}

compose up --detach --build
wait_http "http://127.0.0.1:${node_a_port}/readyz" "node-a ready"
wait_http "http://127.0.0.1:${node_b_port}/readyz" "node-b ready"
wait_http "http://127.0.0.1:${entry_port}/readyz" "entry ready"
wait_entry_backend wecom-ha-node-a true
wait_entry_backend wecom-ha-node-b true
capture baseline
node_a_start_before="$(process_start_id "${diagnostics}/baseline-node-a.json")"
[[ -n "${node_a_start_before}" ]] || { echo "node-a process identity missing" >&2; exit 1; }

compose kill -s KILL wecom-ha-node-a
wait_http "http://127.0.0.1:${node_b_port}/readyz" "node-b after node-a failure"
wait_http "http://127.0.0.1:${entry_port}/readyz" "entry after node-a failure"
wait_entry_backend wecom-ha-node-a false
wait_entry_backend wecom-ha-node-b true
assert_stopped wecom-ha-node-a
sleep 2
assert_stopped wecom-ha-node-a
capture node-a-down

compose start wecom-ha-node-a
wait_http "http://127.0.0.1:${node_a_port}/readyz" "node-a rejoined"
wait_entry_backend wecom-ha-node-a true
capture node-a-rejoined
node_a_start_after="$(process_start_id "${diagnostics}/node-a-rejoined-node-a.json")"
[[ -n "${node_a_start_after}" && "${node_a_start_after}" != "${node_a_start_before}" ]] || { echo "node-a did not receive a new process identity" >&2; exit 1; }
sleep "${stability_seconds}"

compose kill -s KILL wecom-ha-node-b
wait_http "http://127.0.0.1:${node_a_port}/readyz" "node-a after node-b failure"
wait_http "http://127.0.0.1:${entry_port}/readyz" "entry after node-b failure"
wait_entry_backend wecom-ha-node-a true
wait_entry_backend wecom-ha-node-b false
assert_stopped wecom-ha-node-b
capture node-b-down

case "${TRPC_WECOM_REAL_ACCEPTANCE:-assumed}" in
  assumed) echo "WeCom real-session acceptance is assumed; lifecycle drill passed" ;;
  recorded) echo "WeCom real-session evidence is recorded externally; lifecycle drill passed" ;;
  *) echo "TRPC_WECOM_REAL_ACCEPTANCE must be assumed or recorded" >&2; exit 2 ;;
esac
echo "single-host multi-container drill completed; diagnostics retained at ${diagnostics}"
