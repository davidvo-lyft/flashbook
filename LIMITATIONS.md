# LIMITATIONS.md — what flashbook is NOT (honest)

Grown throughout the build (started 2026-07-07). BENCHMARKS.md carries the
measured consequences; ATTACKS.md carries the adversarial Q&A.

## Platform
- Built and benchmarked on **macOS (Apple M5 Max, 18 cores, 64 GB)**, a
  laptop-class SoC — not a tuned Linux box. No io_uring, no isolcpus /
  nohz_full, no NIC interrupt steering, no huge pages, no kernel bypass
  (DPDK/ef_vi), no core pinning (macOS offers no public affinity API).
  Numbers are "well-written userspace on a laptop", not "colo-tuned HFT
  box"; p999/max include macOS scheduler and power-management noise.
  Isolation actually taken for official runs: capture stopped, machine
  otherwise idle, AC power, `caffeinate` held. Stated per-run in the result
  files' host fingerprint.
- Single machine. The "cross-network" pub/sub numbers are **loopback TCP**
  — they measure stack/syscall/scheduler cost, not a NIC or a switch. Said
  loudly again in BENCHMARKS.md 3d.
- Latency histograms time each operation with `std::time::Instant` pairs;
  the measured timer overhead is published alongside (not subtracted).
  Sustained-rate latency runs use an absolute-schedule pacer, which bounds
  but does not eliminate coordinated-omission effects; the achieved rate is
  always published next to the target.

## Data & venues
- Crypto venue public WebSocket feeds (JSON over TLS), not exchange-native
  binary multicast (ITCH/SBE). The JSON→binary normalization benchmark
  exists precisely because the input is JSON. Latency floors are set by the
  venues' own batching (Coinbase level2_batch ~50 ms; Binance depth diffs
  100 ms cadence) — the venue-path numbers in 3e are context, not claims.
- L2 aggregated books (price levels), not L3 order-by-order.
- The Kraken CRC32 oracle is the only venue-provided book checksum;
  Coinbase/Binance book correctness rests on the documented sync protocols,
  differential fast/slow parser equality, deterministic replay, and
  periodic REST cross-snapshots — strong evidence, weaker than a per-update
  venue checksum. Kraken's gap counter is 0 *by design* (integrity there is
  checksum-based, not sequence-based).
- Global fixed-point scale 1e-8: instruments with finer precision (some
  meme pairs) are rejected at subscribe time, not supported (D-003).
- Kraken pair precisions for the CRC are a pinned snapshot (2026-07-07);
  a venue precision change would surface as oracle mismatches (the
  tripwire), not silent corruption.

## Soak reality (see ops/soak-report.md for the generated truth)
- The 24h soak ran on the dev laptop. The capture **process** ran >24h with
  zero crashes and zero venue-sequence gaps, but the machine slept several
  times when unplugged/closed (outside software control; `caffeinate -s`
  holds only on AC). Sleep holes are detected by the stats cadence and
  reported honestly; they reset the *continuous-window* clock. Connection
  drops during sleep were auto-recovered (visible as reconnects with fresh
  snapshots). A server-hosted run (VPS) is the intended fix and is pending
  access.

## Scope
- Market data only: no order entry, no risk, no strategy.
- The tick store is purpose-built (append-only, time-ordered fixed-width
  events, PIT snapshot queries): no SQL, no secondary indexes, no
  concurrent writers, no general schema. Where DuckDB/SQLite win in
  BENCHMARKS.md, the table says so.
- Replay determinism digests use FNV-1a (fast, non-cryptographic): they
  detect divergence, not adversaries. The byte-identity claim for store
  files is checked with SHA-256 in tests.
- The bench e2e harness's live mode uses a minimal WebSocket client over a
  TLS child process (`openssl s_client`) because the bench crate carries no
  TLS/WS dependency; this transport sits **before** the measured t0 and
  does not affect the published decomposition. The production capture path
  uses tokio-tungstenite/rustls throughout.
- The deployed dashboard serves an exported replay dataset (D-013), not a
  live feed; the README states this. It is evidence, not a product.

## Honesty notes
- Every number in README/BENCHMARKS.md traces to a committed file under
  bench/results/ (generated tables; `bench/render.py` refuses hand-typed
  numbers by construction). Preliminary smoke figures are never promoted to
  official without a quiesced re-run.
