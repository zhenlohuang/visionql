#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_project="${VQL_COMPOSE_PROJECT:-vql-test-${CI_JOB_ID:-$$}}"
need_rtsp=0
need_kafka=0
need_vqld=0
explicit_targets=0
expect_target=0

select_target() {
  explicit_targets=1
  case "$1" in
    rtsp)
      need_rtsp=1
      ;;
    kafka)
      need_kafka=1
      ;;
    vqld)
      need_rtsp=1
      need_kafka=1
      need_vqld=1
      ;;
  esac
}

for argument in "$@"; do
  if (( expect_target )); then
    select_target "${argument}"
    expect_target=0
    continue
  fi
  case "${argument}" in
    --test)
      expect_target=1
      ;;
    --test=*)
      select_target "${argument#--test=}"
      ;;
  esac
done

if (( ! explicit_targets )); then
  need_rtsp=1
  need_kafka=1
  need_vqld=1
fi

choose_port() {
  python3 - <<'PY'
import socket

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
}

compose=(
  docker compose
  -f "${repo_root}/docker-compose.yaml"
  -p "${compose_project}"
)
services=()
system_tmp=""
compose_started=0

if (( need_rtsp )); then
  VQL_RTSP_PORT="${VQL_RTSP_PORT:-$(choose_port)}"
  export VQL_RTSP_PORT
  services+=(mediamtx)
fi

if (( need_kafka )); then
  VQL_KAFKA_PORT="${VQL_KAFKA_PORT:-$(choose_port)}"
  export VQL_KAFKA_PORT
  services+=(kafka)
fi

if (( need_vqld )); then
  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required for the vqld system-test certificate" >&2
    exit 1
  fi
  VQLD_FLIGHT_PORT="${VQLD_FLIGHT_PORT:-$(choose_port)}"
  VQLD_SERVICE_TOKEN="${VQLD_SERVICE_TOKEN:-vql-system-test-${compose_project}}"
  system_tmp="$(mktemp -d "/tmp/vql-system-test.XXXXXX")"
  VQLD_TLS_DIR="${system_tmp}/tls"
  mkdir -p "${VQLD_TLS_DIR}"
  openssl req \
    -x509 \
    -newkey rsa:2048 \
    -sha256 \
    -nodes \
    -days 1 \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,DNS:vqld,IP:127.0.0.1" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=serverAuth" \
    -keyout "${VQLD_TLS_DIR}/server.key" \
    -out "${VQLD_TLS_DIR}/server.crt" \
    >/dev/null 2>&1
  chmod 755 "${system_tmp}" "${VQLD_TLS_DIR}"
  chmod 644 "${VQLD_TLS_DIR}/server.crt" "${VQLD_TLS_DIR}/server.key"
  export VQLD_FLIGHT_PORT VQLD_SERVICE_TOKEN VQLD_TLS_DIR
  services+=(vqld)
fi

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if (( compose_started )); then
    if (( status != 0 )); then
      "${compose[@]}" logs --no-color "${services[@]}" || true
    fi
    "${compose[@]}" down --volumes --remove-orphans || true
  fi
  if [[ -n "${system_tmp}" && -d "${system_tmp}" ]]; then
    rm -rf -- "${system_tmp}"
  fi
  exit "${status}"
}
trap cleanup EXIT INT TERM

if (( ${#services[@]} )); then
  compose_started=1
  "${compose[@]}" up -d --build "${services[@]}"
fi

if (( need_rtsp )); then
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
fi

if (( need_kafka )); then
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
fi

if (( need_vqld )); then
  for _ in {1..120}; do
    if "${compose[@]}" exec -T vqld \
      curl --fail --silent --show-error http://127.0.0.1:6032/health/ready \
      >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
  if ! "${compose[@]}" exec -T vqld \
    curl --fail --silent --show-error http://127.0.0.1:6032/health/ready \
    >/dev/null 2>&1; then
    echo "vqld did not become ready within 30 seconds" >&2
    exit 1
  fi
  export VQL_TEST_VQLD_ENDPOINT="https://127.0.0.1:${VQLD_FLIGHT_PORT}"
  export VQL_TEST_VQLD_TOKEN="${VQLD_SERVICE_TOKEN}"
  export VQL_TEST_VQLD_TLS_CA="${VQLD_TLS_DIR}/server.crt"
  export VQL_TEST_VQLD_COMPOSE_FILE="${repo_root}/docker-compose.yaml"
  export VQL_TEST_VQLD_COMPOSE_PROJECT="${compose_project}"
  export VQL_TEST_VQLD_RTSP_URL="rtsp://mediamtx:8554/people"
  export VQL_TEST_VQLD_KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
fi

if (( need_rtsp )); then
  export VQL_TEST_RTSP_URL="rtsp://127.0.0.1:${VQL_RTSP_PORT}/people"
fi
if (( need_kafka )); then
  export VQL_TEST_KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:${VQL_KAFKA_PORT}"
fi

cd "${repo_root}"
VQL_SYSTEM_TEST=1 \
  cargo test -p vql-testing --features system-tests "$@" --locked -- --nocapture
