#!/usr/bin/env bash
# One-command reproduction of every number in BENCHMARKS.md.
#
# Usage: ./bench/run-all.sh [--quick]
#   --quick: reduced sample counts (sanity/smoke; NOT publishable)
#
# Prerequisites:
#   - a captured raw corpus under data/raw (the soak; see ops/)
#   - an ingested store:  cargo run --release -p flashbook-replay --bin ingest \
#       -- --data data/raw --out data/store/full.fbstore --zstd 3 --kraken-depth 100
#   - an otherwise idle machine on AC power (methodology in BENCHMARKS.md)
#
# Results land in bench/results/*.json (committed raw evidence). Regenerate
# the tables afterwards:  bash bench/render.sh --write
#
# The live e2e section connects to the three venues; it degrades gracefully
# per venue if the network disagrees.
set -euo pipefail
cd "$(dirname "$0")/.."

CARGO="${CARGO:-$HOME/.cargo/bin/cargo}"
STORE="${STORE:-data/store/full.fbstore}"
DATA="${DATA:-data/raw}"
RESULTS="${RESULTS:-bench/results}"
QUICK=()
[[ "${1:-}" == "--quick" ]] && QUICK=(--quick)

if [[ ! -f "$STORE" ]]; then
  echo "store not found: $STORE (run the ingest first; see header)" >&2
  exit 2
fi

echo "== build (release, with the compare feature) =="
"$CARGO" build --workspace --release
"$CARGO" build --release -p flashbook-bench --features compare --bin bench-store

run() { echo; echo "== $* =="; "$@"; }

run ./target/release/bench-feed --data "$DATA" --results-dir "$RESULTS" --overwrite "${QUICK[@]}"
run ./target/release/bench-lob --data "$DATA" --kraken-depth 100 --results-dir "$RESULTS" --overwrite "${QUICK[@]}"
run ./target/release/bench-store --store "$STORE" --results-dir "$RESULTS" --overwrite "${QUICK[@]}"
run ./target/release/bench-bus --results-dir "$RESULTS" --overwrite "${QUICK[@]}"
run ./target/release/bench-e2e net --results-dir "$RESULTS" --overwrite "${QUICK[@]}"
run ./target/release/bench-e2e live --results-dir "$RESULTS" --overwrite "${QUICK[@]}"

echo
echo "== allocation profile (rebuilds bench-feed with dhat; run LAST) =="
"$CARGO" build --release -p flashbook-bench --features alloc-profile --bin bench-feed
./target/release/bench-feed --alloc-check --data "$DATA" --results-dir "$RESULTS" --overwrite
# restore the plain binary so a later throughput run isn't dhat-instrumented
"$CARGO" build --release -p flashbook-bench --bin bench-feed

echo
echo "== results =="
ls -la "$RESULTS"/*.json
echo "Now: bash bench/render.sh --write   (regenerates BENCHMARKS.md)"
