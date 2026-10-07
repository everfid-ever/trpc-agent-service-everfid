#!/usr/bin/env bash
# Compatibility entrypoint for the real WeCom HA failover drill. The lifecycle
# assertions live in single-host-multicontainer.sh so both documents and the
# release workflow exercise one canonical topology.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec "${repo_root}/scripts/e2e/single-host-multicontainer.sh" "$@"
