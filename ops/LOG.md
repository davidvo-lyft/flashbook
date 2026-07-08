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
