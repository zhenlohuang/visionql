#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_project="${VQL_COMPOSE_PROJECT:-visionql-test-${CI_JOB_ID:-$$}}"

if [[ -z "${VQL_RTSP_PORT:-}" ]]; then
  VQL_RTSP_PORT="$({ python3 - <<'PY'
import socket

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
  })"
fi
export VQL_RTSP_PORT

compose=(
  docker compose
  -f "${repo_root}/docker-compose.yaml"
  -p "${compose_project}"
  --profile rtsp
)

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if (( status != 0 )); then
    "${compose[@]}" logs mediamtx || true
  fi
  "${compose[@]}" down --volumes --remove-orphans || true
  exit "${status}"
}
trap cleanup EXIT INT TERM

"${compose[@]}" up -d mediamtx

python3 - "${VQL_RTSP_PORT}" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.monotonic() + 15
while time.monotonic() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.25):
            break
    except OSError:
        time.sleep(0.1)
else:
    raise SystemExit(f"MediaMTX did not accept TCP connections on port {port}")
PY

cd "${repo_root}"
VQL_INTEGRATION_TEST=1 \
VQL_TEST_RTSP_URL="rtsp://127.0.0.1:${VQL_RTSP_PORT}/people" \
  cargo test -p vql-testing --locked -- --nocapture
