#!/usr/bin/env bash
# Soak watchdog: every 60 s, if the capture pid is dead, log the restart and
# re-run start-soak.sh. Restarts are honestly counted in ops/soak/restarts.log.
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

case "${1:-}" in
  start)
    mkdir -p ops/soak
    if [[ -f "$WD_PID_FILE" ]] && kill -0 "$(cat "$WD_PID_FILE")" 2>/dev/null; then
      echo "watchdog already running (pid $(cat "$WD_PID_FILE"))" >&2
      exit 1
    fi
    nohup bash ops/soak-watchdog.sh run >> ops/soak/watchdog.log 2>&1 &
    echo "watchdog started (pid $!)"
    ;;
  stop)
    if [[ -f "$WD_PID_FILE" ]] && kill -0 "$(cat "$WD_PID_FILE")" 2>/dev/null; then
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
    if [[ -f "$WD_PID_FILE" ]] && kill -0 "$(cat "$WD_PID_FILE")" 2>/dev/null; then
      echo "watchdog already running (pid $(cat "$WD_PID_FILE")); exiting" >&2
      exit 1
    fi
    echo $$ > "$WD_PID_FILE"
    trap 'rm -f "$WD_PID_FILE"' EXIT
    echo "watchdog loop running (pid $$)"
    while true; do
      sleep 60
      PREV="none"
      [[ -f "$CAP_PID_FILE" ]] && PREV=$(cat "$CAP_PID_FILE")
      if [[ "$PREV" == "none" ]] || ! kill -0 "$PREV" 2>/dev/null; then
        echo "restarted at $(date -u +%Y-%m-%dT%H:%M:%SZ) (previous pid $PREV)" >> "$RESTARTS"
        rm -f "$CAP_PID_FILE"
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
