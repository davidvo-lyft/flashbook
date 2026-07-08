#!/usr/bin/env bash
# Gracefully stop the soak: the watchdog FIRST (otherwise it resurrects
# capture within 60 s and pollutes restart accounting), then the capture
# binary (SIGTERM, wait up to 15 s). Prints exactly what it did.
set -euo pipefail
cd "$(dirname "$0")/.."

PID_FILE=ops/soak/capture.pid
WD_PID_FILE=ops/soak/watchdog.pid

# 1. Stop the watchdog so it cannot restart capture mid-stop.
WD_PID=$(cat "$WD_PID_FILE" 2>/dev/null || echo "")
if [[ -n "$WD_PID" ]] && kill -0 "$WD_PID" 2>/dev/null; then
  kill "$WD_PID" 2>/dev/null || true
  rm -f "$WD_PID_FILE"
  echo "watchdog stopped (pid $WD_PID)"
else
  rm -f "$WD_PID_FILE"
  echo "watchdog not running"
fi

# 2. Stop capture.
if [[ ! -f "$PID_FILE" ]]; then
  echo "no pid file ($PID_FILE); capture not running"
  exit 0
fi

PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
if [[ -z "$PID" ]] || ! kill -0 "$PID" 2>/dev/null; then
  echo "capture (pid ${PID:-unknown}) already stopped; removing stale pid file"
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
