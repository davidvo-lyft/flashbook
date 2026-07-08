#!/usr/bin/env bash
# Build and start the soak capture binary in the background (nohup), keep the
# Mac awake for exactly as long as it runs (caffeinate -w), record its pid.
set -euo pipefail
cd "$(dirname "$0")/.."

PID_FILE=ops/soak/capture.pid

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "capture already running (pid $(cat "$PID_FILE")); refusing to start" >&2
  exit 1
fi

mkdir -p data/raw ops/soak

"$HOME/.cargo/bin/cargo" build --release -p flashbook-feed --bin capture

nohup target/release/capture >> ops/soak/capture.log 2>&1 &
CAP=$!
# Keep the machine awake (idle + system sleep) tied to the capture pid.
nohup caffeinate -is -w "$CAP" >/dev/null 2>&1 &
echo "$CAP" > "$PID_FILE"

echo "capture started: pid $CAP"
echo "  log:   ops/soak/capture.log"
echo "  stats: ops/soak/stats.jsonl"
echo "  data:  data/raw/"
