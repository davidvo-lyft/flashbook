#!/usr/bin/env bash
# Soak watchdog: every 60 s, if the capture pid is dead (or was recycled by
# an unrelated process), log the restart and re-run start-soak.sh. Restarts
# are honestly counted in ops/soak/restarts.log. Also log-rotation
# insurance: capture.log is truncated (with a note in restarts.log) if it
# ever exceeds 1 GiB.
#
# Usage:
#   bash ops/soak-watchdog.sh start   # launch the watchdog in the background
#   bash ops/soak-watchdog.sh stop    # stop the watchdog (leaves capture alone)
#   bash ops/soak-watchdog.sh run     # the loop itself (internal; foreground)
set -euo pipefail
cd "$(dirname "$0")/.."

WD_PID_FILE=ops/soak/watchdog.pid
CAP_PID_FILE=ops/soak/capture.pid
RESTARTS=ops/soak/restarts.log
CAP_LOG=ops/soak/capture.log
MAX_LOG_BYTES=$((1024 * 1024 * 1024)) # 1 GiB

# True if $1 is a live pid whose command name is the capture binary. A bare
# kill -0 is not enough: after a crash the OS can recycle the pid for an
# unrelated process, which would fool the watchdog forever.
capture_alive() {
  local pid="$1" comm
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  comm=$(ps -o comm= -p "$pid" 2>/dev/null || echo "")
  [[ "$(basename "$comm")" == "capture" ]]
}

case "${1:-}" in
  start)
    mkdir -p ops/soak
    if [[ -f "$WD_PID_FILE" ]] && kill -0 "$(cat "$WD_PID_FILE" 2>/dev/null || echo "")" 2>/dev/null; then
      echo "watchdog already running (pid $(cat "$WD_PID_FILE"))" >&2
      exit 1
    fi
    nohup bash ops/soak-watchdog.sh run >> ops/soak/watchdog.log 2>&1 &
    echo "watchdog started (pid $!)"
    ;;
  stop)
    if [[ -f "$WD_PID_FILE" ]] && kill -0 "$(cat "$WD_PID_FILE" 2>/dev/null || echo "")" 2>/dev/null; then
      kill "$(cat "$WD_PID_FILE")"
      rm -f "$WD_PID_FILE"
      echo "watchdog stopped"
    else
      echo "watchdog not running"
      rm -f "$WD_PID_FILE"
    fi
    ;;
  run)
    mkdir -p ops/soak
    # Single instance.
    if [[ -f "$WD_PID_FILE" ]] && kill -0 "$(cat "$WD_PID_FILE" 2>/dev/null || echo "")" 2>/dev/null; then
      echo "watchdog already running (pid $(cat "$WD_PID_FILE")); exiting" >&2
      exit 1
    fi
    echo $$ > "$WD_PID_FILE"
    trap 'rm -f "$WD_PID_FILE"' EXIT
    echo "watchdog loop running (pid $$)"
    # Every step in the loop is hardened (|| fallback): under set -e a
    # vanishing pid file or a failing append must never kill the watchdog.
    while true; do
      sleep 60
      # Log-rotation insurance: a runaway capture.log gets truncated.
      LOG_BYTES=$(wc -c < "$CAP_LOG" 2>/dev/null || echo 0)
      if [[ "$LOG_BYTES" -gt "$MAX_LOG_BYTES" ]]; then
        echo "truncated $CAP_LOG ($LOG_BYTES bytes) at $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$RESTARTS" || true
        : > "$CAP_LOG" || true
      fi
      PREV=$(cat "$CAP_PID_FILE" 2>/dev/null || echo none)
      if ! capture_alive "$PREV"; then
        echo "restarted at $(date -u +%Y-%m-%dT%H:%M:%SZ) (previous pid $PREV)" >> "$RESTARTS" || true
        rm -f "$CAP_PID_FILE" || true
        if ! bash ops/start-soak.sh; then
          echo "start-soak.sh failed at $(date -u +%Y-%m-%dT%H:%M:%SZ); retrying next tick" >&2
        fi
      fi
    done
    ;;
  *)
    echo "usage: bash ops/soak-watchdog.sh {start|stop|run}" >&2
    exit 2
    ;;
esac
