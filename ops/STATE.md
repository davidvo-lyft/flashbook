# STATE — living execution state (update on every milestone)

Last updated: 2026-07-07 ~22:45 UTC (session start)

## Environment facts (scouted 2026-07-07)
- Machine: Apple M5 Max, 18 cores (6P/12E per sysctl perflevels), 64 GB RAM, 1.5 TiB free.
- macOS 26.5.1 (Darwin 25.5.0). Shell zsh. Rust: installing via rustup (was absent).
- Node v24.16.0, npm 11.13.0, pnpm 11.4.0, python 3.14.5, zstd 1.5.7, sqlite3 3.51.0.
- No duckdb CLI (use bundled Rust crate). No websocat (built tools/wsdump instead).
- gh CLI authed as `davidvo-lyft` (repo, workflow scopes) → public repo OK.
- NO vercel CLI/auth, no flyctl, no tunnels. Vercel login is interactive → cannot
  do autonomously. Plan: build dashboard Vercel-ready; deploy via honest fallback
  (see Phase 5 notes) and document per goal's fallback clause.
- Venue reachability (2026-07-07 22:39 UTC): api.exchange.coinbase.com 200,
  api.binance.com 200 (NO geo-block; data-api.binance.vision 200 as fallback),
  api.kraken.com 200.

## Goal gates (from ops/GOAL.md)
- [ ] G1: public GitHub repo, green CI (>=120 tests, clippy -D warnings, fmt, bench smoke)
- [ ] G2: 3-venue live soak >=24h, zero crashes, gap stats, >=5M msgs in tick store, ops/soak-report.md
- [ ] G3: BENCHMARKS.md with real measured numbers (a feed, b lob, c store vs DuckDB/SQLite/Parquet, d bus, e end-to-end)
- [ ] G4: deployed dashboard (Vercel) + engine/read API (or honest documented fallback)
- [ ] G5: README + DECISIONS.md + ATTACKS.md (25 Q&A) + LIMITATIONS.md
- [ ] DONE: ops/DONE.md checklist with evidence links

## Phase status
- [x] Phase 0: scout + rustup install + repo skeleton + public repo + CI
- [~] Phase 1: proto DONE (28 tests); feed API pinned; real fixtures captured;
      workflow wf_1a3f3535-cf1 building 3 codecs + capture core (4 builders ->
      4 docs-verifiers). NEXT: integrate, live smoke, START SOAK.
- [~] Phase 2 (early): lob book engine DONE inline (BTree + Ladder reps,
      cross-impl proptests, depth caps, digests; 10 tests). Replay pending.
- [~] Phase 3 (early): store format layer DONE inline (varint/delta/DoD
      encodings + CRC'd blocks w/ optional zstd; 19 tests). Writer/reader/
      index/PIT + DuckDB/SQLite harness pending.
- [ ] Phase 4: bus + loadgen + e2e latency decomposition
- [ ] Phase 5: dashboard + deploy + CI green
- [ ] Phase 6: optimize loop; ATTACKS.md last; ops/DONE.md

## Running processes (check on resume!)
- (none yet) — when soak starts: PID in ops/soak/capture.pid, logs in ops/soak/,
  stats in ops/soak/stats.jsonl, raw segments in data/raw/<venue>/.
  Capture runs under `caffeinate -is` + nohup so it survives session restarts.

## Resume instructions (if this session/context is lost)
1. Read ops/GOAL.md, this file, ops/LOG.md, DECISIONS.md.
2. Check soak: `cat ops/soak/capture.pid; ps -p $(cat ops/soak/capture.pid)`;
   stats tail: `tail ops/soak/stats.jsonl`. Do NOT restart it if healthy
   (restart count must be reported honestly in soak report).
3. Continue at first unchecked phase above.

## Soak plan (Phase 1)
- Venues/symbols: Coinbase level2_batch+matches+heartbeat, Binance <sym>@depth@100ms
  + @trade + periodic REST depth snapshots, Kraken v2 book(depth) w/ checksum + trade.
  Symbols (5/venue): BTC, ETH, SOL, XRP, DOGE vs USD (USDT on Binance).
- Raw frames appended to CRC-framed segment files (rotate 256MB/15min),
  zstd compaction of closed segments. Est 150-450 msg/s aggregate.

## Next actions
1. Finish skeleton, git init, first commit, `gh repo create` public, push.
2. Write proto crate inline (keystone API), build+test.
3. tools/wsdump → capture real WS samples per venue → fixtures.
4. Workflow fan-out: 3 codec agents + capture/core agent (+ verify agents).
5. Integrate, live smoke 5 min, then START SOAK. Then Phase 2.
