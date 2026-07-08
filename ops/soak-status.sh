#!/usr/bin/env bash
# One-glance soak status: pid liveness, uptime, latest stats lines, raw data
# size, restart count.
set -euo pipefail
cd "$(dirname "$0")/.."

PID_FILE=ops/soak/capture.pid
STATS_FILE=ops/soak/stats.jsonl
RESTARTS=ops/soak/restarts.log

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  PID=$(cat "$PID_FILE")
  echo "capture: RUNNING (pid $PID)"
  echo "uptime:  $(ps -o etime= -p "$PID" | tr -d ' ')"
else
  echo "capture: NOT RUNNING"
fi

echo
if [[ -f "$STATS_FILE" ]]; then
  echo "last stats lines:"
  tail -4 "$STATS_FILE"
else
  echo "no stats file yet ($STATS_FILE)"
fi

echo
if [[ -d data/raw ]]; then
  echo "raw data: $(du -sh data/raw | cut -f1)"
else
  echo "no data/raw yet"
fi

if [[ -f "$RESTARTS" ]]; then
  echo "restarts: $(wc -l < "$RESTARTS" | tr -d ' ')"
else
  echo "restarts: 0 (no restarts.log)"
fi
