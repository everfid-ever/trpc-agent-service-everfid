#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

[[ $# -eq 0 ]] || { echo "usage: $0" >&2; exit 2; }

compose_file="$ROOT/deploy/compose/docker-compose.local.yml"
secret_file="$ROOT/deploy/compose/secrets/deepseek-api-key"

command -v docker >/dev/null 2>&1 || { echo "Docker Desktop is required" >&2; exit 2; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required" >&2; exit 2; }
if [[ ! -s "$secret_file" ]]; then
  cat >&2 <<EOF
DeepSeek API key file is required for the local WebUI:
  mkdir -p deploy/compose/secrets
  install -m 600 /absolute/path/to/deepseek-api-key $secret_file
EOF
  exit 2
fi

docker compose -f "$compose_file" --profile webui up -d --build
echo "Local WebUI: http://localhost:${TRPC_LOCAL_WEBUI_PORT:-58081}/webui/"
