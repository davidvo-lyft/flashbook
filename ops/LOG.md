# Engineering log (append-only; newest last)

## 2026-07-07

- ~22:40Z Session start. Scout: no Rust toolchain (installed 1.96.1), gh authed
  (davidvo-lyft), no vercel auth (fallback planned), all 3 venues reachable
  from this network incl. binance.com (no geo-block). M5 Max / 64 GB / 1.5 TiB free.
- ~22:50Z Workspace skeleton + CI + goal/state docs committed. Public repo
  created: https://github.com/davidvo-lyft/flashbook
- ~23:00Z proto crate: fixed-point (exact 1e-8 parse, i128-referenced proptests),
  64B POD Event (+bytemuck zero-copy), clock, instrument registry, rawlog
  CRC-framed segments with torn-tail recovery (arbitrary-cut proptest). 28 tests.
- ~23:05Z wsdump tool; captured REAL fixtures: coinbase 2456 frames/60s,
  binance 2063/45s, kraken 3000/<60s (2 symbols each) + REST snapshots.
  Verified: Kraken v2 sends checksum on EVERY book update (the oracle);
  Coinbase l2update has no per-message seq (gap detection via heartbeat/
  trade_id continuity + periodic REST cross-checks); Binance U/u chain.
- ~23:20Z Pinned feed API: VenueCodec (fast/slow dual-parse contract),
  SymbolTable, Cursor scanner, strict RFC3339->ns (golden-tested). 8 tests.
- ~23:45Z Verified Kraken v2 accepts BTC/USD, DOGE/USD, XRP/USD (modern names;
  AssetPairs wsname XBT/XDG is v1-only). Fetched pair precisions for CRC oracle:
  BTC 1, ETH 2, SOL 2, XRP 5, DOGE 7 price decimals; qty 8 for all.
- ~00:00Z Launched Phase-1 workflow wf_1a3f3535-cf1: 4 parallel builders
  (coinbase/binance/kraken codecs + conn/sink/stats/capture+soak scripts),
  each pipelined into an adversarial docs-verification agent.
- (parallel) Writing crates/lob core inline: two L2 representations
  (BTreeMap vs contiguous ladder with best-at-end layout) + reference-model
  and cross-impl property tests, digest for replay determinism.

## 2026-07-08

- ~02:00-03:30Z While Phase-1 agents built: wrote inline the store format
  layer (varint/delta/DoD + CRC'd blocks), replay segment merge, bus seqlock
  broadcast ring, bench honesty machinery (nearest-rank percentiles,
  host-stamped result files). Phase-1 workflow landed: 4 codecs/capture
  builders + 4 docs-verifiers; committed at 156 tests green.
- ~03:45Z Fix workflow applied all verification findings (2 coinbase majors:
  advance-only trade baseline + NeedResync on gaps; 4 conn majors: connect
  timeout, health-gated backoff, Retry-After on 429/418, snapshot-parse
  retry; ~10 minors). Replay driver written inline. 170 tests green.
- ~03:57Z Live smoke (3 min, 3 venues): 138,964 msgs @ ~755/s, 0 gaps,
  0 reconnects, 0 fallbacks, 0 parse errors, RSS 31 MB, clean SIGTERM.
- ~04:01Z replay-verify on the smoke capture: 103,590/103,590 Kraken CRCs
  verified, 0 mismatches, two replays byte-identical. The pipeline is
  provably correct on live data.
- ~04:02Z **SOAK STARTED** (capture pid 3702, watchdog 3731, caffeinate).
  8-min health: 417k msgs (~870/s), 0 gaps/errors, RSS 35 MB.
- ~04:10Z CI triage: fmt+clippy+test job green; bench-smoke failed on a
  clobbered exec bit (git add -A re-staged 644) — now invoked via bash.
- ~04:15Z Phase-3 workflow launched: store segment writer/reader -> PIT
  index -> ingest + DuckDB/SQLite/Parquet compare harness (sequential
  chain), bench-lob + bench-feed bins in parallel. DuckDB/SQLite pinned
  behind the bench "compare" feature to keep CI fast (D-012 pending).
- ~05:30-13:15Z Phase-4/5 landed (bus/e2e benches, exporter, dashboard);
  CI fixed (exec bit, 1-ULP float, internal npm registry in lockfile);
  dashboard LIVE on Pages with mid-soak data (111M events, 20.7M CRCs OK).
- ~16:15Z SOAK CONTINUITY EVENT (honestly logged): the machine slept 4x
  between 15:14-16:00Z (~46 min of capture holes; laptop on battery, lid
  events — outside software control; caffeinate -s only holds on AC).
  Capture process NEVER died (pid 3702 throughout, 0 restarts); wake
  recoveries show as reconnects (1 -> 5) with fresh snapshots, gaps=0.
  Consequence: the 24h CONTINUOUS window restarts after the last hole
  (~15:56Z + 4.1min). Longest clean stretch so far: 04:02Z -> 15:14Z
  (11.2h, zero everything). Total capture keeps accumulating regardless;
  the soak report's cadence-hole detector will carry the full truth.
  Notified the user to keep the laptop on AC.
