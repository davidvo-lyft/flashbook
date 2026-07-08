#!/usr/bin/env bash
# One-command reproduction of every number in BENCHMARKS.md.
#
# Usage: ./bench/run-all.sh [--quick]
#   --quick: reduced sample counts (CI smoke / sanity; NOT publishable)
#
# Results land in bench/results/*.json (the committed raw evidence).
# BENCHMARKS.md is regenerated from those files by bench/render.sh —
# numbers are never hand-typed.
#
# Sections (filled in as the harnesses land; each guards on its inputs):
#   feed   - JSON->Event normalization throughput, fast vs serde_json baseline (3a)
#   lob    - replay throughput + top-of-book latency, BTree vs Ladder (3b)
#   store  - write/scan/PIT vs DuckDB/SQLite/Parquet on identical data (3c)
#   bus    - ring vs crossbeam vs tokio::broadcast fan-out curves (3d)
#   e2e    - exchange->subscriber decomposition w/ RTT subtraction (3e)
set -euo pipefail
cd "$(dirname "$0")/.."

CARGO="${CARGO:-$HOME/.cargo/bin/cargo}"
QUICK=""
[[ "${1:-}" == "--quick" ]] && QUICK="--quick"

echo "== flashbook bench: building release =="
"$CARGO" build --workspace --release

run_if_present() {
  local bin="$1"; shift
  if "$CARGO" run --release -p flashbook-bench --bin "$bin" -- --help >/dev/null 2>&1; then
    echo "== $bin $* =="
    "$CARGO" run --release -p flashbook-bench --bin "$bin" -- "$@"
  else
    echo "== $bin: not built yet, skipping =="
  fi
}

# Environment notes recorded by each harness itself (HostInfo). Manual
# isolation steps for publishable runs (documented in BENCHMARKS.md):
# close foreground apps, AC power, no thermal throttle (cold start).

run_if_present bench-feed  $QUICK
run_if_present bench-lob   $QUICK
run_if_present bench-store $QUICK
run_if_present bench-bus   $QUICK
run_if_present bench-e2e   $QUICK

echo "== results =="
ls -la bench/results/*.json 2>/dev/null || echo "(no results yet)"
echo "Now regenerate the tables: bash bench/render.sh"
