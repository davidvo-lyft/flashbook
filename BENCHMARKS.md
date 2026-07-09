# BENCHMARKS

**Generated file — do not edit numbers by hand.** Every number below is
rendered from a committed raw result file in `bench/results/*.json` (schema:
`crates/bench/src/results.rs`) by `bench/render.py`; a number that is not
traceable to a result file does not get written here — by construction.
Each table cites its source file inline.

## Methodology (applies to every section)

- **Hardware/OS**: recorded per-run in each result file (`host` field) and
  tabulated below; inconsistencies between result files are flagged.
- **This is a laptop-class SoC on macOS**, not a tuned Linux box: no
  io_uring, no isolcpus, no IRQ steering. Isolation steps actually taken per
  run: AC power, foreground apps closed, machine otherwise idle, cold-start
  (no prior thermal load); these are stated, their limits acknowledged —
  p999/max include macOS scheduler noise. See LIMITATIONS.md.
- **Percentiles**: nearest-rank over raw sample arrays (`P(q) =
  sorted[ceil(q*n)-1]`), never interpolated or fitted. `n`, warmup count,
  mean, stdev, and max are always published alongside. With small `n`, high
  percentiles saturate at max — reported as such, not extrapolated.
- **Warmup**: stated per benchmark; warmup samples are discarded and counted.
- **Real data**: parse/LOB/store benchmarks run over the captured soak
  corpus (see ops/soak-report.md), not synthetic JSON, unless a section
  explicitly says otherwise (bus benchmarks use the seeded deterministic
  generator to isolate transport cost).
- **Baselines are real implementations**, not strawmen: the serde_json
  baseline is the actual `parse_slow` production fallback path; DuckDB and
  SQLite comparisons use their bundled current releases with stated schemas,
  indexes and pragmas, on identical data.
- **Losses are published.** Where an off-the-shelf engine beats this code,
  the table says so (best value per row is bolded, whoever's it is).

## Host & provenance

| file | created (UTC) | cpu | cores | mem (GB) | os | rustc | quick |
|---|---|---|---|---|---|---|---|
| `bus_fanout.json` | 2026-07-09 06:16:16Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `e2e_live.json` | 2026-07-09 06:22:59Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `e2e_net.json` | 2026-07-09 06:17:59Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `e2e_rtt.json` | 2026-07-09 06:22:59Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `feed_alloc.json` | 2026-07-09 06:24:03Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `feed_parse.json` | 2026-07-09 05:10:33Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `lob_replay.json` | 2026-07-09 05:15:48Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `replay_verify_full.json` | — | — | — | — | — | — | — |
| `store_compare.json` | 2026-07-09 06:14:00Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `store_pit.json` | 2026-07-09 06:04:22Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `store_scan.json` | 2026-07-09 05:57:48Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `store_write.json` | 2026-07-09 05:56:59Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |

- **FLAGGED:** `replay_verify_full.json` has no `host` object — not a ResultFile; excluded from host-consistency checks.
- **FLAGGED:** `replay_verify_full.json`: schema None != 1 — renderer targets schema 1

## Feed — JSON→Event normalization (3a)

### Throughput: fast scanner vs serde_json baseline

| venue | ws frames | fast msgs/s | slow msgs/s | fast/slow | fast bytes/s | fallbacks | parse errors |
|---|---|---|---|---|---|---|---|
| coinbase | 1,000,000 | 2.37 M/s | 595.5 k/s | 3.98× | 867.69 MiB/s | 0 | 0 |
| binance | 1,000,000 | 3.85 M/s | 730.6 k/s | 5.27× | 1.15 GiB/s | 0 | 0 |
| kraken | 1,000,000 | 8.87 M/s | 1.18 M/s | 7.55× | 1.71 GiB/s | 0 | 0 |
| **aggregate** | 3,000,000 | **3.78 M/s** | 769.5 k/s | **4.91×** | 1.07 GiB/s | 0 | 0 |

Aggregate fast path emits normalized events at 23.44 M/s. *src: `feed_parse.json`*

> **Producer notes (verbatim):** Single-threaded: one codec instance parses one venue's records in capture order. 'msgs/sec/core' means single-thread msgs/s on one P-core equivalent; macOS does not pin threads, the scheduler picks the core. fast = production fast path including its real parse_slow fallback on Structure errors; slow = parse_slow on every frame (the published naive serde_json baseline). REST snapshots are applied identically on both paths and included in run time but excluded from the msgs/s denominator (WS frames only). Differential guarantee: fast and slow event counts asserted equal per venue over the full loaded corpus. Timing: median of measured fresh-codec runs.

### Allocations per frame (dhat)

| venue | fast allocs/frame | fast bytes/frame | slow allocs/frame | slow bytes/frame | REST snaps (fast) | frames |
|---|---|---|---|---|---|---|
| coinbase | 0.0002 | 0.0192 | 80.9608 | 5391.4081 | 0 | 10,000 |
| binance | 3.011 | 216.4236 | 35.8181 | 2698.1716 | 5 | 10,000 |
| kraken | 0 | 0 | 3.9946 | 842.0059 | 0 | 10,000 |

**Zero allocations/frame measured** (exactly 0 in this run) for the fast path on: kraken (10,000 frames, 0 blocks, 0 bytes). *src: `feed_alloc.json`*
Fast path is **not** zero-allocation for: coinbase (2 blocks / 192 B over 10,000 frames); binance (30,110 blocks / 2,164,236 B over 10,000 frames) — see producer notes for attribution. *src: `feed_alloc.json`*

> **Producer notes (verbatim):** dhat heap deltas around one measured pass per (venue, path). The measured pass uses a FRESH codec (a warmup pass on a separate codec instance amortizes only the shared Event out-buffer), so one-time per-codec lazy allocations — per-symbol state, internal scratch on first use, error/fallback paths — land inside the window and are averaged over the 10k-frame sample. Steady-state target for the fast path is 0 allocations/frame; the numbers here are the real measured deltas, whatever they are. REST snapshot records are parsed in-position on both paths and included in the deltas: the Binance fast-path delta is dominated by parse_rest_snapshot, which parses the depth body via serde_json::Value on BOTH paths by design (rest_snapshots is reported per pass so this is attributable); Coinbase's tiny fast-path residue is one-time per-codec state growth. Kraken (no REST resync in-sample) is the pure WS-frame fast path.

## LOB — book replay & top-of-book latency (3b)

### Replay throughput per representation

Corpus: `data/raw` — 55,820,598 records / 226,404,844 events / 55,819,963 WS frames; checksums ok 41,692,848, mismatches 0; 1 warmup + 5 measured passes, 1 thread(s). *src: `lob_replay.json`*

| representation | events/s per pass | mean events/s | pass seconds |
|---|---|---|---|
| btree | 24.07 M/s, 24.30 M/s, 24.38 M/s, 24.21 M/s, 24.39 M/s | **24.27 M/s** | 9.406 s, 9.318 s, 9.285 s, 9.351 s, 9.283 s |
| ladder | 10.07 M/s, 10.10 M/s, 10.08 M/s, 10.06 M/s, 9.95 M/s | 10.06 M/s | 22.48 s, 22.42 s, 22.45 s, 22.5 s, 22.75 s |

**Winner (declared by the data, mean events/s): `btree`.** Result file's own `winner` field: `btree`.
Differential check: `digests_match = true` — all representations produced identical end-state digests.

### Top-of-book update latency

| representation | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| btree | 22,598,696 | 0 ns | 41 ns | 42 ns | 125 ns | 167 ns | 339.54 µs | 32 ns |
| ladder | 22,598,696 | 0 ns | 42 ns | 2.38 µs | 5.29 µs (5,292 ns) | 5.88 µs | 1.05 ms | 609 ns |

Timer-overhead calibration: each sample includes one `Instant::now()/elapsed()` pair, measured at 31.0 ns on this run (published, not subtracted). *src: `lob_replay.json`*

> **Producer notes (verbatim):** Single-threaded book-apply benchmark over pre-parsed events (parse cost excluded). msgs/s-equivalent derives as ws_frames / pass_seconds. Latency samples each include one Instant::now()/elapsed() pair (~31.0ns measured on this run); only applies returning Mutated{top_changed:true} are kept. Winner is reported, not acted on (representation choice is a later phase).

## Store — write, scan, point-in-time, head-to-head (3c)

### Write throughput (encode only)

| mode | events/s | logical MB/s | bytes/event | stored bytes | zstd level |
|---|---|---|---|---|---|
| raw | 40.19 M/s | 2,572 MB/s | 20.34 B | 4.29 GiB (4,605,379,366 B) | — |
| zstd | 15.69 M/s | 1,004 MB/s | 9.10 B | 1.92 GiB (2,061,064,275 B) | 3 |

*src: `store_write.json`*

> **Producer notes (verbatim):** Re-encoding the store's 226404844 decoded events through StoreWriter (append+seal, block_events=8192) to a scratch file, 3 pass(es) per mode. MB/s is logical (events * 64 B / s); encode cost only, source events pre-decoded in memory.

### Full-scan throughput

| pass | seconds | events/s | logical GB/s | physical GB/s |
|---|---|---|---|---|
| pass 1 | 8.13 s | 27.85 M/s | 1.78 GB/s | 0.254 GB/s |
| pass 2 | 8.116 s | 27.90 M/s | 1.79 GB/s | 0.254 GB/s |
| pass 3 | 8.091 s | 27.98 M/s | 1.79 GB/s | 0.255 GB/s |
| pass 4 | 8.442 s | 26.82 M/s | 1.72 GB/s | 0.244 GB/s |
| pass 5 | 8.191 s | 27.64 M/s | 1.77 GB/s | 0.252 GB/s |
| **mean** | — | **27.64 M/s** | **1.77 GB/s** | **0.252 GB/s** |

226,404,844 events scanned. *src: `store_scan.json`*

> **Producer notes (verbatim):** Sequential decode of every block via StoreReader::scan (mmap, per-block CRC + column decode), 1 warmup + 5 measured passes. Logical GB/s = events * 64 B / s; physical GB/s = file bytes / s.

### Point-in-time snapshot query latency

| queries | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| 1,000 | 1,000 | 42 ns | 111.65 ms (111,646,625 ns) | 1.364 s | 3.267 s (3,267,488,167 ns) | 3.618 s | 3.637 s | 394.42 ms |

Anchor hit rate 93% (927/1,000); 691 snapshots indexed (index source: sidecar). Misses are timed as the near-free lookups they are. *src: `store_pit.json`*

> **Producer notes (verbatim):** 1000 seeded-random (instrument, t) queries (seed 0xf1a5b00c), t uniform over the store's recv_mono span, instruments uniform over the 16 seen. Each query = SnapshotIndex::latest_at + pit_scan folded into an unbounded LadderBook (top-of-book out). Percentiles cover ALL queries; anchor misses (no complete snapshot at or before t) are timed as the near-free lookups they are, and the hit rate is published alongside.

### Head-to-head: ours vs DuckDB vs SQLite vs Parquet-zstd

Identical 226,404,844 events in every backend. Raw-JSON baseline 13.13 GiB (14,095,629,974 B) from `metrics.sizes_bytes.raw_json`. Best value per row is **bolded regardless of whose it is**; — means the backend has no such measurement in the result file. *src: `store_compare.json`*

| metric | ours (fbstore) | DuckDB | SQLite | Parquet-zstd | better |
|---|---|---|---|---|---|
| load seconds | — | 96.347 s | 162.649 s | **6.705 s** | lower |
| on-disk bytes | **1.92 GiB (2,061,078,204 B)** | 5.46 GiB (5,861,814,272 B) | 15.07 GiB (16,180,383,744 B) | 2.00 GiB (2,143,484,948 B) | lower |
| bytes/event | **9.10 B** | 25.89 B | 71.47 B | 9.47 B | lower |
| ratio vs raw JSON | **6.84× smaller** | 2.40× smaller | 0.87× — LARGER than raw JSON | 6.58× smaller | higher |
| full-scan seconds | 9.873 s | **234.9 ms** | 36.575 s | — | lower |
| PIT p50 | 119.44 ms (119,443,541 ns) | **43.93 ms (43,934,500 ns)** | 85.70 ms (85,704,500 ns) | — | lower |
| PIT p99 | 2.474 s (2,474,457,416 ns) | **959.45 ms (959,445,000 ns)** | 3.718 s (3,718,010,875 ns) | — | lower |

Notes on blanks: ours' load is the capture-time ingest (see `store_write.json` for encode cost); Parquet is written via DuckDB COPY and has no scan/PIT harness in this result file.

Full PIT latency percentiles per backend:

| backend | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| duckdb | 200 | 3.33 ms | 43.93 ms (43,934,500 ns) | 298.03 ms | 959.45 ms (959,445,000 ns) | 1.085 s | 1.085 s | 118.55 ms |
| ours | 200 | 250 ns | 119.44 ms (119,443,541 ns) | 1.329 s | 2.474 s (2,474,457,416 ns) | 3.231 s | 3.231 s | 373.10 ms |
| sqlite | 200 | 1.16 ms | 85.70 ms (85,704,500 ns) | 1.101 s | 3.718 s (3,718,010,875 ns) | 4.393 s | 4.393 s | 365.42 ms |

Parity before timing: full_scan_equal=true, pit_tops_equal=true, anchor hits 186/200, divergences 0, failures [].
Result file's own `winners` field (wins and losses, plainly): full_scan → **duckdb**, pit_p50 → **duckdb**, smallest_size → **ours**.

> **Producer notes (verbatim):** Identical events loaded into DuckDB (Appender, no index) and SQLite (single tx + prepared INSERT; load-only pragmas journal_mode=MEMORY, synchronous=OFF; then CREATE INDEX idx_inst_mono + ANALYZE, charged to load). Parquet via DuckDB COPY (FORMAT PARQUET, COMPRESSION ZSTD). Full-scan aggregate and PIT top-of-book asserted EQUAL across backends before any timing is quoted; SQL PIT folds from ours' validated anchor when the naive kind=4 anchor picks an incomplete snapshot (divergences published). Losses are reported as plainly as wins — see winners.

## Bus — in-process fan-out & loopback network (3d)

### Fan-out throughput over subscriber counts

| contender | subs | publish rate | delivery rate/sub (min–max) | lost |
|---|---|---|---|---|
| ring (seqlock, ours) | 1 | 11.47 M/s | 11.47 M/s | 0 |
| ring (seqlock, ours) | 2 | 9.44 M/s | 9.44 M/s – 9.44 M/s | 0 |
| ring (seqlock, ours) | 4 | 7.76 M/s | 7.76 M/s – 7.76 M/s | 0 |
| ring (seqlock, ours) | 8 | 4.65 M/s | 4.65 M/s – 4.65 M/s | 0 |
| crossbeam-channel fan-out | 1 | 58.88 M/s | 58.57 M/s | 0 |
| crossbeam-channel fan-out | 2 | 6.77 M/s | 6.77 M/s – 6.77 M/s | 0 |
| crossbeam-channel fan-out | 4 | 3.53 M/s | 3.53 M/s – 3.53 M/s | 0 |
| crossbeam-channel fan-out | 8 | 1.12 M/s | 1.12 M/s – 1.12 M/s | 0 |
| tokio::broadcast | 1 | 33.06 M/s | 24.65 M/s | **1,249,997 (25.0%)** |
| tokio::broadcast | 2 | 27.61 M/s | 27.60 M/s – 27.60 M/s | 0 |
| tokio::broadcast | 4 | 17.96 M/s | 17.96 M/s – 17.96 M/s | 0 |
| tokio::broadcast | 8 | 12.32 M/s | 12.32 M/s – 12.32 M/s | 0 |

*src: `bus_fanout.json`*

### Fan-out delivery latency (paced publisher)

| contender | subs | achieved pub rate | lost | p50 | p90 | p99 | p999 | max | n |
|---|---|---|---|---|---|---|---|---|---|
| ring (seqlock, ours) | 1 | 500.0 k/s | 0 | 125 ns | 208 ns | 292 ns | 5.25 µs | 146.33 µs | 1,250,000 |
| ring (seqlock, ours) | 2 | 500.0 k/s | 0 | 125 ns | 208 ns | 333 ns | 5.46 µs | 173.92 µs | 2,500,000 |
| ring (seqlock, ours) | 4 | 500.0 k/s | 0 | 209 ns | 250 ns | 5.88 µs (5,875 ns) | 14.29 µs | 407.42 µs | 5,000,000 |
| ring (seqlock, ours) | 8 | 500.0 k/s | 0 | 416 ns | 500 ns | 5.67 µs (5,666 ns) | 43.17 µs | 1.44 ms | 10,000,000 |
| crossbeam-channel fan-out | 1 | 500.0 k/s | 0 | 1.25 µs (1,250 ns) | 4.50 µs | 7.42 µs (7,417 ns) | 20.29 µs | 378.50 µs | 1,250,000 |
| crossbeam-channel fan-out | 2 | 500.0 k/s | 0 | 1.88 µs (1,875 ns) | 5.17 µs | 9.21 µs (9,208 ns) | 24.50 µs | 516.79 µs | 2,500,000 |
| crossbeam-channel fan-out | 4 | 500.0 k/s | 0 | 2.25 µs (2,250 ns) | 7.58 µs | 21.67 µs (21,667 ns) | 47.92 µs | 547.12 µs | 5,000,000 |
| crossbeam-channel fan-out | 8 | 500.0 k/s | 0 | 2.42 µs (2,417 ns) | 13.08 µs | 28.58 µs (28,583 ns) | 43.29 µs | 1.52 ms | 10,000,000 |
| tokio::broadcast | 1 | 500.0 k/s | 0 | 2.08 µs (2,084 ns) | 5.00 µs | 8.92 µs (8,916 ns) | 23.62 µs | 352.75 µs | 1,250,000 |
| tokio::broadcast | 2 | 500.0 k/s | 0 | 2.25 µs (2,250 ns) | 5.38 µs | 9.62 µs (9,625 ns) | 24.29 µs | 509.17 µs | 2,500,000 |
| tokio::broadcast | 4 | 500.0 k/s | 0 | 2.54 µs (2,542 ns) | 5.83 µs | 10.54 µs (10,541 ns) | 28.25 µs | 711.00 µs | 5,000,000 |
| tokio::broadcast | 8 | 500.0 k/s | 0 | 3.00 µs (3,000 ns) | 7.92 µs | 22.04 µs (22,041 ns) | 88.42 µs | 629.83 µs | 10,000,000 |

*src: `bus_fanout.json`*

> **Producer notes (verbatim):** In-process fan-out comparison on identical seeded EventGen workloads. Semantics differ BY DESIGN and are published as-is: the seqlock ring and tokio broadcast overwrite the oldest events when a subscriber lags (subscriber is told the loss count); crossbeam-channel fan-out (one bounded(65536) channel per subscriber, producer send()s a copy to each) BLOCKS the producer when any channel is full — backpressure, never loss. Receive modes are each contender's natural usage: ring consumers spin-poll with std::hint::spin_loop (burns a core per subscriber), crossbeam uses blocking recv(), tokio uses recv().await on a multi_thread runtime. All threads stamp and diff ONE process-monotonic clock (flashbook_proto::clock::mono_ns), so cross-thread latency math is sound; threads are unpinned (macOS). Latency phase paces the producer on an absolute schedule (message i at start + i/rate, spin-waited); a blocked crossbeam send makes it catch up in a burst afterwards. Latency samples are per delivered message, stride-sampled deterministically (every ceil(msgs/2M)-th message, capped at 2M per subscriber) and merged across subscribers for the published percentiles (nearest-rank).

### Loopback TCP fan-out (e2e_net)

> **LOOPBACK IS NOT A NIC.** This measures kernel network-stack + syscall + scheduler-handoff cost on one host: no wire serialization, no propagation, no NIC interrupt/coalescing behavior. Treat as a floor for cross-machine fan-out latency. (Caveat restated from the result file's own notes, quoted in full below.)

**Sustained: NO — the pacing schedule slipped; the ACHIEVED rate 89.6 k/s is what the latencies below were measured at, not the 200.0 k/s target.** (4 subscribers, 6,000,000 events, elapsed 66.973 s). *src: `e2e_net.json`*

| stream | delivered | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|---|
| merged (all subs) | — | 7,998,668 | 4.12 µs | 17.42 µs (17,417 ns) | 27.96 µs | 40.17 µs (40,167 ns) | 56.00 µs | 1.48 ms | 18.62 µs |
| sub 0 | 6,000,000 | 1,999,667 | 7.17 µs | 18.75 µs (18,750 ns) | 28.79 µs | 40.58 µs (40,583 ns) | 57.12 µs | 1.47 ms | 20.03 µs |
| sub 1 | 6,000,000 | 1,999,667 | 4.12 µs | 13.00 µs (13,000 ns) | 22.79 µs | 33.62 µs (33,625 ns) | 48.46 µs | 405.00 µs | 14.44 µs |
| sub 2 | 6,000,000 | 1,999,667 | 8.96 µs | 21.46 µs (21,459 ns) | 31.88 µs | 44.21 µs (44,208 ns) | 61.17 µs | 1.48 ms | 22.77 µs |
| sub 3 | 6,000,000 | 1,999,667 | 5.79 µs | 15.88 µs (15,875 ns) | 25.71 µs | 36.92 µs (36,917 ns) | 52.29 µs | 1.47 ms | 17.22 µs |

*src: `e2e_net.json`*

> **Producer notes (verbatim):** LIMITATIONS: loopback TCP is NOT a NIC. This measures kernel network-stack + syscall + scheduler-handoff cost on one host: no wire serialization, no propagation, no NIC interrupt/coalescing behavior. Treat as a floor for cross-machine fan-out latency. Method: publisher paces on an absolute schedule (event i at start+i/rate), stamps recv_mono_ns once immediately before the first subscriber write, then write(2)s the raw 64B Event (length-implicit framing, no batching) to each subscriber in turn — later subscribers include fan-out serialization. Subscribers stamp after read_exact(64) completes. All stamps share one process-monotonic clock; threads unpinned (macOS). First 1000 events per subscriber excluded as warmup; stride sampling (stride 3, cap 2000000/sub). If the schedule slips, the ACHIEVED rate is reported and sustained=false.

## E2E — exchange→subscriber added latency (3e)

### Local pipeline decomposition on live venue traffic

3 venue(s) connected. `total added` starts at socket read and contains zero internet time by construction; `(steady)` rows exclude initial-snapshot drain events and are the steady-state numbers (see producer notes). *src: `e2e_live.json`*

**Aggregate (all venues)**

| stage | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| parse | 48,003 | 0 ns | 209 ns | 1.00 µs | 2.62 µs (2,625 ns) | 10.67 µs | 1.28 ms | 467 ns |
| publish | 45,009 | 0 ns | 125 ns | 375 ns | 2.58 µs (2,583 ns) | 4.62 µs | 181.17 µs | 233 ns |
| deliver | 213,904 | 0 ns | 1.17 µs (1,167 ns) | 555.54 µs | 1.05 ms (1,051,875 ns) | 1.10 ms | 1.10 ms | 114.35 µs |
| deliver (steady) | 170,894 | 0 ns | 791 ns | 8.00 µs | 33.12 µs (33,125 ns) | 77.08 µs | 1.10 ms | 3.31 µs |
| total added | 213,904 | 125 ns | 3.42 µs (3,416 ns) | 2.02 ms | 2.52 ms (2,517,459 ns) | 2.56 ms | 2.57 ms | 409.16 µs |
| total added (steady) | 170,894 | 125 ns | 2.29 µs (2,292 ns) | 13.17 µs | 34.46 µs (34,458 ns) | 82.83 µs | 1.10 ms | 5.12 µs |

**binance**

| stage | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| parse | 11,402 | 41 ns | 125 ns | 1.21 µs | 4.00 µs (4,000 ns) | 15.12 µs | 31.12 µs | 532 ns |
| publish | 8,412 | 0 ns | 83 ns | 209 ns | 500 ns | 3.17 µs | 5.58 µs | 115 ns |
| deliver | 8,412 | 0 ns | 167 ns | 17.58 µs | 42.54 µs (42,542 ns) | 241.75 µs | 243.29 µs | 5.32 µs |
| deliver (steady) | 8,412 | 0 ns | 167 ns | 17.58 µs | 42.54 µs (42,542 ns) | 241.75 µs | 243.29 µs | 5.32 µs |
| total added | 8,412 | 125 ns | 583 ns | 17.83 µs | 43.04 µs (43,041 ns) | 241.88 µs | 243.71 µs | 5.64 µs |
| total added (steady) | 8,412 | 125 ns | 583 ns | 17.83 µs | 43.04 µs (43,041 ns) | 241.88 µs | 243.71 µs | 5.64 µs |

**coinbase**

| stage | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| parse | 6,719 | 125 ns | 875 ns | 1.79 µs | 4.62 µs (4,625 ns) | 9.00 µs | 1.28 ms | 1.26 µs |
| publish | 6,718 | 0 ns | 291 ns | 959 ns | 3.46 µs (3,458 ns) | 6.17 µs | 181.17 µs | 543 ns |
| deliver | 125,559 | 0 ns | 2.71 µs (2,708 ns) | 768.67 µs | 1.07 ms (1,073,791 ns) | 1.10 ms | 1.10 ms | 192.62 µs |
| deliver (steady) | 82,752 | 0 ns | 1.00 µs (1,000 ns) | 8.83 µs | 30.79 µs (30,792 ns) | 69.96 µs | 1.10 ms | 3.51 µs |
| total added | 125,559 | 250 ns | 8.21 µs (8,208 ns) | 2.23 ms | 2.54 ms (2,539,375 ns) | 2.57 ms | 2.57 ms | 694.50 µs |
| total added (steady) | 82,752 | 250 ns | 3.96 µs (3,958 ns) | 15.50 µs | 32.88 µs (32,875 ns) | 71.17 µs | 1.10 ms | 6.76 µs |

**kraken**

| stage | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| parse | 29,882 | 0 ns | 208 ns | 417 ns | 1.12 µs (1,125 ns) | 3.54 µs | 29.38 µs | 265 ns |
| publish | 29,879 | 0 ns | 125 ns | 334 ns | 1.83 µs (1,833 ns) | 4.29 µs | 137.08 µs | 196 ns |
| deliver | 79,933 | 0 ns | 541 ns | 6.50 µs | 33.67 µs (33,667 ns) | 74.42 µs | 166.17 µs | 2.90 µs |
| deliver (steady) | 79,730 | 0 ns | 541 ns | 6.42 µs | 33.67 µs (33,667 ns) | 74.42 µs | 166.17 µs | 2.89 µs |
| total added | 79,933 | 125 ns | 1.04 µs (1,041 ns) | 7.08 µs | 34.25 µs (34,250 ns) | 74.83 µs | 166.29 µs | 3.40 µs |
| total added (steady) | 79,730 | 125 ns | 1.04 µs (1,041 ns) | 6.88 µs | 34.29 µs (34,291 ns) | 74.83 µs | 166.29 µs | 3.36 µs |

Per-venue counters:

| venue | connected | frames | events | fallbacks | parse errors | resync signals | lagged lost | unmatched deliver |
|---|---|---|---|---|---|---|---|---|
| binance | yes | 11,402 | 8,412 | 0 | 0 | 0 | 0 | 0 |
| coinbase | yes | 6,719 | 125,559 | 0 | 0 | 0 | 0 | 0 |
| kraken | yes | 29,882 | 79,933 | 0 | 0 | 0 | 0 | 0 |

*src: `e2e_live.json`*

### Venue path — context only, **NOT flashbook's added latency**

`venue_path` = venue-side batching + WAN transit + venue↔host wall-clock offset; it is published as context and is not attributable to this code.

| venue | n | min | p50 | p90 | p99 | p999 | max | mean | clamped to 0 |
|---|---|---|---|---|---|---|---|---|---|
| binance | 8,412 | 37.60 ms | 41.21 ms (41,208,000 ns) | 43.55 ms | 46.62 ms (46,623,000 ns) | 78.53 ms | 87.53 ms | 41.56 ms | 0 |
| coinbase | 6,718 | 0 ns | 0 ns | 0 ns | 10.87 ms (10,871,000 ns) | 34.21 ms | 718.46 ms | 400.58 µs | 6,504 **(mostly clamped — uninterpretable; use RTT file)** |
| kraken | 29,579 | 0 ns | 0 ns | 8.92 ms | 36.72 ms (36,718,000 ns) | 85.42 ms | 105.42 ms | 2.62 ms | 23,031 **(mostly clamped — uninterpretable; use RTT file)** |

*src: `e2e_live.json`*

> **Producer notes (verbatim):** Decomposition of the LOCAL pipeline only, measured on live venue traffic (BTC-USD, one extra WS connection per venue, run alongside the capture soak). t0 = mono_ns when a WS text frame has been fully read; parse = t1-t0 (production codec fast path incl. serde_json fallback); publish = t2-t1 (bus ring publish of the frame's events, t2 stamped once per frame after all its publishes); deliver = t3-t2 per event (subscriber thread dequeue, matched to its frame via recv_mono_ns == t0); total_added = t3-t0. 'Exchange->subscriber added latency' = total_added: it starts at socket read and contains zero internet time by construction. VENUE PATH is context, NOT added by flashbook: venue_path = recv_wall - venue_ts per venue-stamped frame; it includes venue-side batching (Coinbase level2_batch ~50 ms, Binance depth@100ms cadence) + WAN transit + venue<->host wall-clock offset; bound venue-internal batching ~= venue_path - rtt/2 using e2e_rtt.json (approximation: symmetric path). LIMITATIONS: (1) TLS is terminated by an openssl s_client child; frames cross one extra pipe hop before t0, inflating the receive path by pipe latency but leaving parse/publish/deliver (which start at t0) untouched. (2) The ring subscriber yields every 256 empty polls (soak politeness); its wakeup cost is inside deliver. (3) No REST resync is wired, so Binance depth events stay unsynced and are dropped by the codec; Binance samples are dominated by trade frames (resync_signals counts the codec asking). (4) venue_path samples where the venue clock is ahead of local wall are clamped to 0 and counted (venue_path_clamped); a clamp count near n means the local-vs-venue wall-clock offset exceeds the one-way path and venue_path is uninterpretable without an offset correction — the RTT file is the trustworthy WAN bound in that case. (5) deliver saturates at 0 for events dequeued before their frame's t2 stamp was taken (t2 is per-frame, after ALL its publishes). (6) The initial full-book snapshot arrives as one enormous frame; its sequential per-event drain dominates event-weighted deliver/ total_added percentiles on short windows, so *_steady_ns (events without the FROM_SNAPSHOT flag) is published alongside and is the steady-state number.

### Internet RTT per venue (WS ping/pong)

| venue | pings | pongs | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|---|---|
| binance | 59 | 59 | 59 | 198.86 ms | 245.66 ms (245,656,625 ns) | 292.89 ms | 297.12 ms (297,117,208 ns) | 297.12 ms | 297.12 ms | 247.92 ms |
| coinbase | 59 | 59 | 59 | 32.60 ms | 38.01 ms (38,009,416 ns) | 43.96 ms | 131.55 ms (131,550,791 ns) | 131.55 ms | 131.55 ms | 39.94 ms |
| kraken | 59 | 59 | 59 | 94.99 ms | 103.99 ms (103,992,208 ns) | 109.10 ms | 117.97 ms (117,973,875 ns) | 117.97 ms | 117.97 ms | 104.09 ms |

*src: `e2e_rtt.json`* — small n by design; high percentiles saturate at max.

Subtraction method, quoted from the result file notes:
> **Producer notes (verbatim):** RTT method: every 5 s a WS Ping with an 8-byte little-endian mono_ns payload is sent; on the Pong echo, rtt = mono_ns - payload. Subtraction method for readers: venue-internal batching ~= venue_path (e2e_live.json) - rtt/2, an approximation that assumes a symmetric WAN path and instant pong turnaround. RTT includes the openssl s_client pipe hops in both directions (adds microseconds against millisecond WANs). n is small by design (one ping per 5 s); high percentiles saturate at the max accordingly.

## Other result files (no dedicated renderer — listed so nothing committed is invisible)

- `replay_verify_full.json` — not a ResultFile; top-level keys: books_digest, checksum_mismatches, checksums_ok, checksums_skipped, codec_resets, event_stream_digest, events, fallbacks, gaps, notes, parse_errors, records, rest_snapshots, span_mono_s, torn_tails, ws_frames. Notes: 74

---

Regenerate: `bash bench/render.sh --write` (after `./bench/run-all.sh`).

Generated 2026-07-09T06:26:04Z from 12 result file(s) in `bench/results`: `bus_fanout.json`, `e2e_live.json`, `e2e_net.json`, `e2e_rtt.json`, `feed_alloc.json`, `feed_parse.json`, `lob_replay.json`, `replay_verify_full.json`, `store_compare.json`, `store_pit.json`, `store_scan.json`, `store_write.json`.
Inputs sha256 (sorted concatenation of input file bytes): `6c38fd16332153c43ab8b1abbed037df3ccad57836cc4c6e13f312b507e8badb`.
