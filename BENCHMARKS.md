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
| `e2e_live.json` | 2026-07-09 16:22:39Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `e2e_net.json` | 2026-07-09 16:17:36Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `e2e_rtt.json` | 2026-07-09 16:22:39Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `feed_alloc.json` | 2026-07-09 06:24:03Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `feed_parse.json` | 2026-07-09 05:10:33Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `lob_replay.json` | 2026-07-09 05:15:48Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `replay_verify_full.json` | — | — | — | — | — | — | — |
| `store_compare.json` | 2026-07-09 16:15:45Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `store_pit.json` | 2026-07-09 16:06:08Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `store_scan.json` | 2026-07-09 15:59:32Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |
| `store_write.json` | 2026-07-09 15:58:40Z | Apple M5 Max | 18 | 64 | macOS 26.5.1 | rustc 1.96.1 (31fca3adb 2026-06-26) | — |

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
| raw | 38.92 M/s | 2,491 MB/s | 20.35 B | 4.29 GiB (4,606,595,438 B) | — |
| zstd | 15.19 M/s | 972 MB/s | 9.11 B | 1.92 GiB (2,062,280,347 B) | 3 |

*src: `store_write.json`*

> **Producer notes (verbatim):** Re-encoding the store's 226404844 decoded events through StoreWriter (append+seal, block_events=8192) to a scratch file, 3 pass(es) per mode. MB/s is logical (events * 64 B / s); encode cost only, source events pre-decoded in memory.

### Full-scan throughput

| pass | seconds | events/s | logical GB/s | physical GB/s |
|---|---|---|---|---|
| pass 1 | 8.607 s | 26.31 M/s | 1.68 GB/s | 0.24 GB/s |
| pass 2 | 8.475 s | 26.72 M/s | 1.71 GB/s | 0.243 GB/s |
| pass 3 | 8.54 s | 26.51 M/s | 1.7 GB/s | 0.241 GB/s |
| pass 4 | 8.582 s | 26.38 M/s | 1.69 GB/s | 0.24 GB/s |
| pass 5 | 8.515 s | 26.59 M/s | 1.7 GB/s | 0.242 GB/s |
| **mean** | — | **26.50 M/s** | **1.7 GB/s** | **0.241 GB/s** |

226,404,844 events scanned. *src: `store_scan.json`*

> **Producer notes (verbatim):** Sequential decode of every block via StoreReader::scan (mmap, per-block CRC + column decode), 1 warmup + 5 measured passes. Logical GB/s = events * 64 B / s; physical GB/s = file bytes / s.

### Point-in-time snapshot query latency

| queries | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| 1,000 | 1,000 | 42 ns | 112.29 ms (112,292,583 ns) | 1.387 s | 3.261 s (3,260,948,834 ns) | 3.592 s | 3.633 s | 396.58 ms |

Anchor hit rate 93% (927/1,000); 691 snapshots indexed (index source: sidecar). Misses are timed as the near-free lookups they are. *src: `store_pit.json`*

> **Producer notes (verbatim):** 1000 seeded-random (instrument, t) queries (seed 0xf1a5b00c), t uniform over the store's recv_mono span, instruments uniform over the 16 seen. Each query = SnapshotIndex::latest_at + pit_scan folded into an unbounded LadderBook (top-of-book out). Percentiles cover ALL queries; anchor misses (no complete snapshot at or before t) are timed as the near-free lookups they are, and the hit rate is published alongside.

### Head-to-head: ours vs DuckDB vs SQLite vs Parquet-zstd

Identical 226,404,844 events in every backend. Raw-JSON baseline 13.13 GiB (14,095,629,974 B) from `metrics.sizes_bytes.raw_json`. Best value per row is **bolded regardless of whose it is**; — means the backend has no such measurement in the result file. *src: `store_compare.json`*

| metric | ours (fbstore) | DuckDB | SQLite | Parquet-zstd | better |
|---|---|---|---|---|---|
| load seconds | — | 94.273 s | 161.541 s | **6.105 s** | lower |
| on-disk bytes | **1.92 GiB (2,062,294,276 B)** | 5.46 GiB (5,861,814,272 B) | 15.07 GiB (16,180,383,744 B) | 2.00 GiB (2,143,484,948 B) | lower |
| bytes/event | **9.11 B** | 25.89 B | 71.47 B | 9.47 B | lower |
| ratio vs raw JSON | **6.83× smaller** | 2.40× smaller | 0.87× — LARGER than raw JSON | 6.58× smaller | higher |
| full-scan seconds | 7.125 s | **188.7 ms** | 40.344 s | — | lower |
| PIT p50 | 118.97 ms (118,972,334 ns) | **42.90 ms (42,896,416 ns)** | 85.88 ms (85,879,125 ns) | — | lower |
| PIT p99 | 2.539 s (2,539,132,417 ns) | **897.13 ms (897,132,416 ns)** | 3.518 s (3,518,131,915 ns) | — | lower |

Notes on blanks: ours' load is the capture-time ingest (see `store_write.json` for encode cost); Parquet is written via DuckDB COPY and has no scan/PIT harness in this result file.

Full PIT latency percentiles per backend:

| backend | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| duckdb | 200 | 3.17 ms | 42.90 ms (42,896,416 ns) | 262.04 ms | 897.13 ms (897,132,416 ns) | 987.25 ms | 987.25 ms | 110.50 ms |
| ours | 200 | 167 ns | 118.97 ms (118,972,334 ns) | 1.326 s | 2.539 s (2,539,132,417 ns) | 3.220 s | 3.220 s | 372.35 ms |
| sqlite | 200 | 1.08 ms | 85.88 ms (85,879,125 ns) | 1.194 s | 3.518 s (3,518,131,915 ns) | 4.273 s | 4.273 s | 371.51 ms |

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

**Sustained: NO — the pacing schedule slipped; the ACHIEVED rate 96.9 k/s is what the latencies below were measured at, not the 200.0 k/s target.** (4 subscribers, 6,000,000 events, elapsed 61.918 s). *src: `e2e_net.json`*

| stream | delivered | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|---|
| merged (all subs) | — | 7,998,668 | 4.29 µs | 14.25 µs (14,250 ns) | 21.17 µs | 42.21 µs (42,209 ns) | 1.51 ms | 10.90 ms | 19.88 µs |
| sub 0 | 6,000,000 | 1,999,667 | 5.71 µs | 12.50 µs (12,500 ns) | 18.62 µs | 39.25 µs (39,250 ns) | 1.37 ms | 10.89 ms | 18.40 µs |
| sub 1 | 6,000,000 | 1,999,667 | 8.83 µs | 17.54 µs (17,541 ns) | 24.25 µs | 47.12 µs (47,125 ns) | 1.62 ms | 10.90 ms | 23.99 µs |
| sub 2 | 6,000,000 | 1,999,667 | 4.29 µs | 10.04 µs (10,042 ns) | 15.71 µs | 35.75 µs (35,750 ns) | 1.44 ms | 10.89 ms | 15.83 µs |
| sub 3 | 6,000,000 | 1,999,667 | 7.25 µs | 15.04 µs (15,041 ns) | 21.46 µs | 43.21 µs (43,209 ns) | 1.64 ms | 10.90 ms | 21.32 µs |

*src: `e2e_net.json`*

> **Producer notes (verbatim):** LIMITATIONS: loopback TCP is NOT a NIC. This measures kernel network-stack + syscall + scheduler-handoff cost on one host: no wire serialization, no propagation, no NIC interrupt/coalescing behavior. Treat as a floor for cross-machine fan-out latency. Method: publisher paces on an absolute schedule (event i at start+i/rate), stamps recv_mono_ns once immediately before the first subscriber write, then write(2)s the raw 64B Event (length-implicit framing, no batching) to each subscriber in turn — later subscribers include fan-out serialization. Subscribers stamp after read_exact(64) completes. All stamps share one process-monotonic clock; threads unpinned (macOS). First 1000 events per subscriber excluded as warmup; stride sampling (stride 3, cap 2000000/sub). If the schedule slips, the ACHIEVED rate is reported and sustained=false.

## E2E — exchange→subscriber added latency (3e)

### Local pipeline decomposition on live venue traffic

3 venue(s) connected. `total added` starts at socket read and contains zero internet time by construction; `(steady)` rows exclude initial-snapshot drain events and are the steady-state numbers (see producer notes). *src: `e2e_live.json`*

**Aggregate (all venues)**

| stage | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| parse | 66,397 | 0 ns | 167 ns | 1.54 µs | 3.83 µs (3,833 ns) | 11.58 µs | 1.27 ms | 552 ns |
| publish | 63,394 | 0 ns | 125 ns | 334 ns | 1.00 µs (1,000 ns) | 4.17 µs | 182.88 µs | 188 ns |
| deliver | 291,551 | 0 ns | 958 ns | 364.83 µs | 1.03 ms (1,026,250 ns) | 1.09 ms | 8.03 ms | 83.96 µs |
| deliver (steady) | 248,414 | 0 ns | 750 ns | 3.33 µs | 15.71 µs (15,708 ns) | 156.50 µs | 8.03 ms | 2.75 µs |
| total added | 291,551 | 125 ns | 3.08 µs (3,083 ns) | 1.81 ms | 2.48 ms (2,477,041 ns) | 2.54 ms | 8.03 ms | 299.86 µs |
| total added (steady) | 248,414 | 125 ns | 2.25 µs (2,250 ns) | 10.00 µs | 23.38 µs (23,375 ns) | 160.75 µs | 8.03 ms | 5.21 µs |

**binance**

| stage | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| parse | 15,662 | 41 ns | 125 ns | 1.75 µs | 5.42 µs (5,417 ns) | 17.92 µs | 29.46 µs | 593 ns |
| publish | 12,663 | 0 ns | 83 ns | 167 ns | 417 ns | 3.17 µs | 10.83 µs | 103 ns |
| deliver | 12,663 | 0 ns | 83 ns | 2.71 µs | 20.62 µs (20,625 ns) | 224.71 µs | 227.71 µs | 1.72 µs |
| deliver (steady) | 12,663 | 0 ns | 83 ns | 2.71 µs | 20.62 µs (20,625 ns) | 224.71 µs | 227.71 µs | 1.72 µs |
| total added | 12,663 | 125 ns | 292 ns | 3.08 µs | 21.12 µs (21,125 ns) | 224.83 µs | 230.71 µs | 2.04 µs |
| total added (steady) | 12,663 | 125 ns | 292 ns | 3.08 µs | 21.12 µs (21,125 ns) | 224.83 µs | 230.71 µs | 2.04 µs |

**coinbase**

| stage | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| parse | 8,528 | 84 ns | 1.17 µs (1,167 ns) | 2.83 µs | 7.25 µs (7,250 ns) | 15.04 µs | 1.27 ms | 1.63 µs |
| publish | 8,527 | 0 ns | 250 ns | 708 ns | 3.04 µs (3,042 ns) | 5.50 µs | 182.88 µs | 377 ns |
| deliver | 169,317 | 0 ns | 1.75 µs (1,750 ns) | 661.58 µs | 1.05 ms (1,054,625 ns) | 1.09 ms | 7.39 ms | 143.07 µs |
| deliver (steady) | 126,383 | 0 ns | 1.21 µs (1,208 ns) | 4.29 µs | 16.50 µs (16,500 ns) | 82.21 µs | 7.39 ms | 3.41 µs |
| total added | 169,317 | 208 ns | 6.79 µs (6,792 ns) | 2.11 ms | 2.51 ms (2,505,666 ns) | 2.54 ms | 7.39 ms | 514.47 µs |
| total added (steady) | 126,383 | 208 ns | 4.75 µs (4,750 ns) | 13.33 µs | 26.29 µs (26,292 ns) | 93.42 µs | 7.39 ms | 7.77 µs |

**kraken**

| stage | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|
| parse | 42,207 | 0 ns | 167 ns | 625 ns | 2.21 µs (2,208 ns) | 3.17 µs | 41.33 µs | 319 ns |
| publish | 42,204 | 0 ns | 125 ns | 333 ns | 708 ns | 4.00 µs | 58.33 µs | 175 ns |
| deliver | 109,571 | 0 ns | 375 ns | 1.83 µs | 13.29 µs (13,292 ns) | 389.25 µs | 8.03 ms | 2.13 µs |
| deliver (steady) | 109,368 | 0 ns | 375 ns | 1.79 µs | 12.54 µs (12,541 ns) | 389.25 µs | 8.03 ms | 2.12 µs |
| total added | 109,571 | 125 ns | 833 ns | 2.75 µs | 15.00 µs (15,000 ns) | 390.00 µs | 8.03 ms | 2.65 µs |
| total added (steady) | 109,368 | 125 ns | 833 ns | 2.75 µs | 13.50 µs (13,500 ns) | 390.00 µs | 8.03 ms | 2.63 µs |

Per-venue counters:

| venue | connected | frames | events | fallbacks | parse errors | resync signals | lagged lost | unmatched deliver |
|---|---|---|---|---|---|---|---|---|
| binance | yes | 15,662 | 12,663 | 0 | 0 | 0 | 0 | 0 |
| coinbase | yes | 8,528 | 169,317 | 0 | 0 | 0 | 0 | 0 |
| kraken | yes | 42,207 | 109,571 | 0 | 0 | 0 | 0 | 0 |

*src: `e2e_live.json`*

### Venue path — context only, **NOT flashbook's added latency**

`venue_path` = venue-side batching + WAN transit + venue↔host wall-clock offset; it is published as context and is not attributable to this code.

| venue | n | min | p50 | p90 | p99 | p999 | max | mean | clamped to 0 |
|---|---|---|---|---|---|---|---|---|---|
| binance | 12,663 | 45.42 ms | 49.26 ms (49,262,000 ns) | 51.87 ms | 99.79 ms (99,794,000 ns) | 215.49 ms | 215.49 ms | 50.70 ms | 0 |
| coinbase | 8,527 | 0 ns | 0 ns | 0 ns | 318.65 ms (318,653,000 ns) | 2.300 s | 2.545 s | 15.87 ms | 7,831 **(mostly clamped — uninterpretable; use RTT file)** |
| kraken | 41,903 | 1.76 ms | 6.94 ms (6,942,000 ns) | 20.35 ms | 71.60 ms (71,602,000 ns) | 162.41 ms | 191.52 ms | 10.78 ms | 0 |

*src: `e2e_live.json`*

> **Producer notes (verbatim):** Decomposition of the LOCAL pipeline only, measured on live venue traffic (BTC-USD, one extra WS connection per venue, run alongside the capture soak). t0 = mono_ns when tungstenite yields a complete WS text frame; parse = t1-t0 (production codec fast path incl. serde_json fallback); publish = t2-t1 (bus ring publish of the frame's events, t2 stamped once per frame after all its publishes); deliver = t3-t2 per event (subscriber thread dequeue, matched to its frame via recv_mono_ns == t0); total_added = t3-t0. 'Exchange->subscriber added latency' = total_added: it starts at socket read and contains zero internet time by construction. VENUE PATH is context, NOT added by flashbook: venue_path = recv_wall - venue_ts per venue-stamped frame; it includes venue-side batching (Coinbase level2_batch ~50 ms, Binance depth@100ms cadence) + WAN transit + venue<->host wall-clock offset; bound venue-internal batching ~= venue_path - rtt/2 using e2e_rtt.json (approximation: symmetric path). TRANSPORT: the same WebSocket/TLS stack as the production capture path — tokio-tungstenite (connect_async) with rustls (native roots), TLS terminated in-process; t0 sits after TLS decrypt + WS frame assembly, matching production's receive stamp (no child process, no pipe hop). LIMITATIONS: (1) The ring subscriber yields every 256 empty polls (soak politeness); its wakeup cost is inside deliver. (2) No REST resync is wired, so Binance depth events stay unsynced and are dropped by the codec; Binance samples are dominated by trade frames (resync_signals counts the codec asking). (3) venue_path samples where the venue clock is ahead of local wall are clamped to 0 and counted (venue_path_clamped); a clamp count near n means the local-vs-venue wall-clock offset exceeds the one-way path and venue_path is uninterpretable without an offset correction — the RTT file is the trustworthy WAN bound in that case. (4) deliver saturates at 0 for events dequeued before their frame's t2 stamp was taken (t2 is per-frame, after ALL its publishes). (5) The initial full-book snapshot arrives as one enormous frame; its sequential per-event drain dominates event-weighted deliver/ total_added percentiles on short windows, so *_steady_ns (events without the FROM_SNAPSHOT flag) is published alongside and is the steady-state number.

### Internet RTT per venue (WS ping/pong)

| venue | pings | pongs | n | min | p50 | p90 | p99 | p999 | max | mean |
|---|---|---|---|---|---|---|---|---|---|---|
| binance | 60 | 59 | 59 | 171.07 ms | 223.38 ms (223,376,583 ns) | 266.01 ms | 271.01 ms (271,014,500 ns) | 271.01 ms | 271.01 ms | 223.23 ms |
| coinbase | 59 | 59 | 59 | 26.92 ms | 27.90 ms (27,895,583 ns) | 31.76 ms | 34.60 ms (34,595,916 ns) | 34.60 ms | 34.60 ms | 28.56 ms |
| kraken | 60 | 59 | 59 | 86.21 ms | 88.60 ms (88,599,875 ns) | 92.18 ms | 100.59 ms (100,592,208 ns) | 100.59 ms | 100.59 ms | 89.31 ms |

*src: `e2e_rtt.json`* — small n by design; high percentiles saturate at max.

Subtraction method, quoted from the result file notes:
> **Producer notes (verbatim):** RTT method: every 5 s a WS Ping with an 8-byte little-endian mono_ns payload is sent; on the Pong echo, rtt = mono_ns - payload. Subtraction method for readers: venue-internal batching ~= venue_path (e2e_live.json) - rtt/2, an approximation that assumes a symmetric WAN path and instant pong turnaround. Pings and pongs travel the same in-process tungstenite+rustls transport as the data frames (the production capture stack; no child process or pipe hops). n is small by design (one ping per 5 s); high percentiles saturate at the max accordingly.

## Other result files (no dedicated renderer — listed so nothing committed is invisible)

- `replay_verify_full.json` — not a ResultFile; top-level keys: books_digest, checksum_mismatches, checksums_ok, checksums_skipped, codec_resets, crossval_price_overlap_p50, crossval_price_overlap_p90, crossval_scored, crossval_snapshots, crossval_top10_overlap_p50, crossval_top10_overlap_p90, crossval_worst_overlap, event_stream_digest, events, fallbacks, gaps, notes, parse_errors, records, rest_snapshots, span_mono_s, torn_tails, ws_frames. Notes: 74

---

Regenerate: `bash bench/render.sh --write` (after `./bench/run-all.sh`).

Generated 2026-07-09T16:23:20Z from 12 result file(s) in `bench/results`: `bus_fanout.json`, `e2e_live.json`, `e2e_net.json`, `e2e_rtt.json`, `feed_alloc.json`, `feed_parse.json`, `lob_replay.json`, `replay_verify_full.json`, `store_compare.json`, `store_pit.json`, `store_scan.json`, `store_write.json`.
Inputs sha256 (sorted concatenation of input file bytes): `78896a711adee9ad62d240088be520cd198fcd654cf62fd08c282265d3c70ed8`.
