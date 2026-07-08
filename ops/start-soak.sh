#!/usr/bin/env bash
# Build and start the soak capture binary in the background (nohup), keep the
# Mac awake for exactly as long as it runs (caffeinate -w), record its pid.
# Serialized by an atomic mkdir lock: the pid-file guard alone is
# check-then-act across a full cargo build, so racing starts (operator +
# watchdog) could otherwise double-run capture.
set -euo pipefail
cd "$(dirname "$0")/.."

PID_FILE=ops/soak/capture.pid
LOCK_DIR=ops/soak/start.lock

mkdir -p data/raw ops/soak

# Atomic lock: mkdir either creates the directory (we own this start) or
# fails because another start is in progress. Removed on any exit path
# (bash runs the EXIT trap on TERM/INT too); only a kill -9 leaves it
# behind, in which case remove it by hand.
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "another start-soak.sh is in progress ($LOCK_DIR exists); refusing to start" >&2
  exit 1
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE" 2>/dev/null || echo "")" 2>/dev/null; then
  echo "capture already running (pid $(cat "$PID_FILE")); refusing to start" >&2
  exit 1
fi

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
