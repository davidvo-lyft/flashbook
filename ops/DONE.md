# DONE — goal-gate scorecard with evidence

Judged against ops/GOAL.md, honestly. Generated docs referenced here are
produced by committed code from committed raw data (BENCHMARKS.md via
`bench/render.py` from `bench/results/*.json`; ops/soak-report.md via
`ops/gen-soak-report.py` from `ops/soak/stats.jsonl`).

## G1 — public repo, green CI, >= 120 meaningful tests: **MET**
- Repo: https://github.com/davidvo-lyft/flashbook (public).
- CI: .github/workflows/ci.yml — `cargo fmt --check`, `cargo clippy
  --workspace --all-targets -- -D warnings`, `cargo test --workspace`
  (**256 tests**: property-based cross-implementation book equivalence,
  arbitrary-truncation crash recovery, full-fixture fast/slow parser
  differentials, torn-tail proptests), plus the bench smoke job
  (bench/ci-smoke.sh). Green on the final commit (Actions history).

## G2 — 3-venue live soak: **MET except the continuous-24h clause — see below**
- >= 3 venues concurrently: **MET** (Coinbase Exchange, Binance, Kraken —
  public WS, no keys). Evidence: ops/soak/stats.jsonl per-venue lines.
- >= 5M messages to the tick store: **MET, 45x over** — 55,819,963 raw msgs
  captured; **226,404,844 events ingested** into data/store/full.fbstore
  (sidecar: data/store/full.fbstore.ingest.json, committed stats in
  ops/soak-report.md).
- Gap detection/resequencing stats logged: **MET** — per-minute JSONL
  (1,068 lines), per-venue gap/resync/reconnect counters; 0 sequence gaps.
- Zero crashes: **MET** — one capture process for the entire span, 0
  watchdog restarts (ops/soak/restarts.log absent), RSS ceiling 44 MB.
- Continuous >= 24h: **NOT MET AS SPECIFIED** — the host laptop slept 18
  times (battery/lid, outside software control): longest hole-free window
  11.2h across a 25.05h span. Published, not hidden: ops/soak-report.md
  gates section + cadence table. The capture survived every sleep and
  auto-recovered every connection. A server-hosted (VPS) re-run for a
  clean 24h window is planned — blocked on infrastructure access, not on
  the software (the user has been asked for a VPS; the run restarts there
  unchanged).

## G3 — BENCHMARKS.md, real measured numbers, full methodology: **MET**
All sections generated from committed raw result files; wins AND losses:
- (a) feed: aggregate fast **3.78M msgs/s single-core = 4.91×** the
  serde_json baseline (kraken **8.87M/s = 7.55×**); dhat allocations
  published (**kraken fast path exactly 0/frame**; binance's 3.0/frame
  attributed to REST snapshot parsing, by design on both paths).
- (b) lob: **BTreeBook 24.27M events/s** replay over the full 226M-event
  corpus (ladder 10.06M — loser shipped as the property-test partner,
  D-014); top-of-book latency p50 **41 ns** (n=22.6M, timer overhead
  published).
- (c) store: **9.10 B/event** — smaller than Parquet-zstd (2.06 vs
  2.14 GB), 6.84× under raw JSON; DuckDB 5.9 GB, SQLite 16.2 GB (larger
  than raw). **Published loss: DuckDB full-scans 42× faster** (0.23s vs
  9.9s; column pruning + vectorization — analysis in ATTACKS.md). PIT
  latency + 3-backend parity (200 queries, tops equal).
- (d) bus: all three contenders' curves at 1/2/4/8 subscribers; ring at
  sustained 500k msg/s: 1-sub **p50 125 ns / p99 292 ns**, 0 lost.
  Loopback e2e_net at stated rates with achieved-rate honesty flags.
- (e) e2e live decomposition: steady-state added latency **p50 2.29 µs /
  p99 34.5 µs** across three venues; per-stage split; venue-path and RTT
  tables with the stated subtraction method.
- Correctness of the harness itself: the 3-backend parity assertions
  caught two real bugs during official runs (i64 sum overflow; DuckDB
  integer `/` = float division) — commits reference both.

## G4 — deployed: **MET via the goal's own fallback clause; Vercel pending one login**
- Dashboard: **LIVE and 200** at https://davidvo-lyft.github.io/flashbook/
  rendering real data (full-corpus export: books with time scrubber,
  ingest health incl. gap/restart counters, venue-path latency, the
  headline benchmark table — 207 rows from bench/results/).
- The goal names Vercel; no Vercel credentials exist on this machine and
  `vercel login` is interactive. Per the goal's explicit fallback ("if
  none, run the engine locally for the soak and deploy the dashboard
  against a recorded-replay API — say so honestly"), the engine ran
  locally, the dashboard serves the exported replay dataset (D-013), and
  the README says so. Vercel completion is one command once the user logs
  in: `cd apps/dashboard && vercel --prod` (vercel.json committed).

## G5 — docs: **MET**
- README.md: architecture diagram, headline numbers (each traceable),
  one-command repro (`./bench/run-all.sh` → `bash bench/render.sh
  --write`), honest deployment section.
- DECISIONS.md: D-001..D-014, each recorded when made, with alternatives.
- ATTACKS.md: 25 adversarial Q&A grounded in file:line + published
  numbers (includes the goal's three mandated attacks).
- LIMITATIONS.md: platform, oracle asymmetry, soak sleep reality, harness
  caveats.
- ops/soak-report.md: generated evidence for G2.

## Phase-6 optimization loop — status
One full official pass completed. The dominant identified bottleneck is
the store's full-scan loss to DuckDB (42×; full-event decode + per-event
fold vs pruned vectorized columns). No optimization iteration was applied
after the official runs: the two remaining unmet clauses (continuous-24h,
Vercel) are environment-blocked, not performance-blocked, and no code
change since the official pass has moved any published p99 (loop exit
condition per the goal). The scan-gap fix (per-column pruned decode) is
scoped in ATTACKS.md as the first follow-up; the loop resumes on the VPS
alongside the continuous-24h re-run.

## Evidence index (paths)
- bench/results/*.json — raw benchmark evidence (committed)
- bench/results/replay_verify_full.json — full-corpus double replay:
  byte-identical digests, 41,692,848 CRCs / 0 mismatches
- BENCHMARKS.md, ops/soak-report.md — generated docs
- ops/soak/stats.jsonl — per-minute soak telemetry (committed)
- data/store/full.fbstore(.snapidx/.ingest.json) — local corpus store
  (data/ gitignored; ingest.json stats mirrored in soak report)
- Dashboard: https://davidvo-lyft.github.io/flashbook/
- CI: GitHub Actions on the final commit
