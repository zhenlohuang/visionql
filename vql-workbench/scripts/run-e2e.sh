#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workbench_root=$(cd "$script_dir/.." && pwd)
repo_root=$(cd "$workbench_root/.." && pwd)
frontend_root="$workbench_root/frontend"
runtime_root=$(mktemp -d "${TMPDIR:-/tmp}/vql-workbench-e2e.XXXXXX")
vqld_pid=""
workbench_pid=""

cleanup() {
  for pid in "$workbench_pid" "$vqld_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ "$runtime_root" == *vql-workbench-e2e.* ]]; then
    rm -rf "$runtime_root"
  fi
}
trap cleanup EXIT INT TERM

free_port() {
  node -e 'const net = require("node:net"); const server = net.createServer(); server.listen(0, "127.0.0.1", () => { console.log(server.address().port); server.close(); });'
}

wait_for_http() {
  local url=$1
  local pid=$2
  local log=$3
  for _ in {1..120}; do
    if curl --fail --silent --show-error "$url" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      sed -n '1,240p' "$log" >&2
      return 1
    fi
    sleep 0.25
  done
  sed -n '1,240p' "$log" >&2
  return 1
}

flight_port=$(free_port)
health_port=$(free_port)
workbench_port=$(free_port)
vqld_endpoint="http://127.0.0.1:$flight_port"
workbench_url="http://127.0.0.1:$workbench_port"

cargo build --manifest-path "$repo_root/Cargo.toml" --package vql-server --bin vqld --locked
cargo build --manifest-path "$workbench_root/Cargo.toml" --locked
pnpm --dir "$frontend_root" build

mkdir -p "$runtime_root/vql-home"
VQL_HOME="$runtime_root/vql-home" \
  "$repo_root/target/debug/vqld" \
  --flight-addr "127.0.0.1:$flight_port" \
  --http-addr "127.0.0.1:$health_port" \
  >"$runtime_root/vqld.log" 2>&1 &
vqld_pid=$!
wait_for_http "http://127.0.0.1:$health_port/health/ready" "$vqld_pid" "$runtime_root/vqld.log"

"$workbench_root/target/debug/vql-workbench" \
  --listen-addr "127.0.0.1:$workbench_port" \
  --static-dir "$frontend_root/dist" \
  --default-vqld-endpoint "$vqld_endpoint" \
  --session-idle-timeout-seconds 5 \
  >"$runtime_root/workbench.log" 2>&1 &
workbench_pid=$!
wait_for_http "$workbench_url/api/session" "$workbench_pid" "$runtime_root/workbench.log"

if [[ "${1:-}" == "--" ]]; then
  shift
fi
VQL_WORKBENCH_E2E_BASE_URL="$workbench_url" \
VQL_WORKBENCH_E2E_VQLD_ENDPOINT="$vqld_endpoint" \
VQL_WORKBENCH_E2E_IMAGE_DIR="$frontend_root/public" \
  pnpm --dir "$frontend_root" exec playwright test \
  --config "$frontend_root/playwright.config.ts" "$@"
