#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_project="${VQL_COMPOSE_PROJECT:-vql-test-${CI_JOB_ID:-$$}}"

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

if [[ -z "${VQL_KAFKA_PORT:-}" ]]; then
  VQL_KAFKA_PORT="$({ python3 - <<'PY'
import socket

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
  })"
fi
export VQL_KAFKA_PORT

compose=(
  docker compose
  -f "${repo_root}/docker-compose.yaml"
  -p "${compose_project}"
  --profile rtsp
  --profile kafka
)

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if (( status != 0 )); then
    "${compose[@]}" logs mediamtx kafka || true
  fi
  "${compose[@]}" down --volumes --remove-orphans || true
  exit "${status}"
}
trap cleanup EXIT INT TERM

"${compose[@]}" up -d mediamtx kafka

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

for _ in {1..60}; do
  if "${compose[@]}" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
    >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if ! "${compose[@]}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
  >/dev/null 2>&1; then
  echo "Kafka did not become ready within 30 seconds" >&2
  exit 1
fi

cd "${repo_root}"
VQL_INTEGRATION_TEST=1 \
VQL_TEST_RTSP_URL="rtsp://127.0.0.1:${VQL_RTSP_PORT}/people" \
VQL_TEST_KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:${VQL_KAFKA_PORT}" \
  cargo test -p vql-testing --features system-tests "$@" --locked -- --nocapture
