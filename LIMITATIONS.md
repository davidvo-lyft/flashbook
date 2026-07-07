# LIMITATIONS.md — what flashbook is NOT (honest)

Started 2026-07-07; grows as the system is built. See BENCHMARKS.md for the
measured consequences of these choices.

## Platform
- Built and benchmarked on **macOS (Apple M5 Max)**, not a tuned Linux box.
  No io_uring, no isolcpus/nohz_full, no NIC interrupt steering, no huge pages,
  no kernel-bypass (DPDK/ef_vi). Numbers are "well-written userspace on a
  laptop-class SoC", not "colo-tuned HFT box". Tail percentiles (p999/max)
  include macOS scheduler and power-management noise; methodology in
  BENCHMARKS.md states the isolation steps actually taken and their limits.
- Single machine. "Cross-network" pub/sub numbers are loopback/LAN unless
  stated otherwise; internet RTT to venues is measured and subtracted, not
  eliminated.

## Data & venues
- Crypto venue public WebSocket feeds (JSON over TLS), not exchange-native
  binary multicast (ITCH/OUCH, SBE). The normalization benchmark exists
  precisely because the input is JSON; a real HFT feed handler would parse
  binary. Latency floors are set by the venues' own batching (e.g. Coinbase
  level2_batch batches at 50ms; Binance depth diffs at 100ms).
- L2 aggregated books only (price-level), not L3 order-by-order. Venue L3
  feeds either require auth or don't exist publicly at these venues.
- Fixed-point scale is a global 1e-8: instruments with price/qty precision
  finer than 1e-8 (some meme pairs) are rejected at subscribe time, not
  supported (D-003).

## Scope
- Market data only: no order entry, no risk, no strategy — this is the
  data-plane half of a trading system.
- The tick store is purpose-built for this workload (append-only, time-ordered
  fixed-width events, PIT snapshot queries). It is not a general database: no
  SQL, no secondary indexes, no concurrent writers, no multi-table anything.
  Where DuckDB/SQLite beat it, BENCHMARKS.md says so.
- The dashboard is evidence, not a product.

## Honesty notes
- Anything this file claims is checked against what was actually built; if a
  limitation is later removed, the entry moves to DECISIONS.md with the change.
