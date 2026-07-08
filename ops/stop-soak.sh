#!/usr/bin/env bash
# Gracefully stop the soak capture binary (SIGTERM, wait up to 15 s).
set -euo pipefail
cd "$(dirname "$0")/.."

PID_FILE=ops/soak/capture.pid

if [[ ! -f "$PID_FILE" ]]; then
  echo "no pid file ($PID_FILE); capture not running"
  exit 0
fi

PID=$(cat "$PID_FILE")
if ! kill -0 "$PID" 2>/dev/null; then
  echo "capture (pid $PID) already stopped; removing stale pid file"
  rm -f "$PID_FILE"
  exit 0
fi

echo "sending SIGTERM to capture (pid $PID)"
kill -TERM "$PID"

for _ in $(seq 1 15); do
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "capture stopped"
    rm -f "$PID_FILE"
    exit 0
  fi
  sleep 1
done

echo "capture (pid $PID) still running after 15s" >&2
exit 1
