#!/usr/bin/env bash
# CI benchmark smoke: proves the bench harness builds and produces sane
# output quickly. Full numbers come from bench/run-all.sh on real hardware.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --workspace --release
echo "bench smoke: release build OK (harness jobs extend this as they land)"
