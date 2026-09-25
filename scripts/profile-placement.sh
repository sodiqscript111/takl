#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8090}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"
SECONDS="${SECONDS:-30}"
CONCURRENCY="${CONCURRENCY:-4}"
OUTPUT_DIR="${OUTPUT_DIR:-profiles}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
DIR="$OUTPUT_DIR/$STAMP"
mkdir -p "$DIR"

AUTH_ARGS=()
if [ -n "$ADMIN_TOKEN" ]; then
  AUTH_ARGS=(-H "X-Takl-Admin-Token: $ADMIN_TOKEN")
fi

STOP="$DIR/stop"
pids=()
for _ in $(seq 1 "$CONCURRENCY"); do
  (
    while [ ! -f "$STOP" ]; do
      curl -fsS "$BASE_URL/api/v1/runners/best" >/dev/null || true
    done
  ) &
  pids+=("$!")
done

cleanup() {
  touch "$STOP"
  for pid in "${pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

curl -fsS "${AUTH_ARGS[@]}" "$BASE_URL/debug/pprof/profile?seconds=$SECONDS" -o "$DIR/cpu.pprof"
curl -fsS "${AUTH_ARGS[@]}" "$BASE_URL/debug/pprof/heap" -o "$DIR/heap.pprof"
cleanup
trap - EXIT

if command -v go >/dev/null 2>&1; then
  go tool pprof -svg -output "$DIR/cpu.svg" "$DIR/cpu.pprof" || true
  go tool pprof -svg -output "$DIR/heap.svg" "$DIR/heap.pprof" || true
fi

printf '%s\n' "$DIR"
printf 'go tool pprof -http=:0 %s\n' "$DIR/cpu.pprof"
