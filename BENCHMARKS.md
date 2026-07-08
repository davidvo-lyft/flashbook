# BENCHMARKS

**Status: no numbers yet.** This file is regenerated from committed raw
result files in `bench/results/*.json` by `bench/render.sh`; sections appear
as their harnesses land and produce real measurements. A number that is not
traceable to a result file does not get written here — by construction.

## Methodology (applies to every section)

- **Hardware/OS**: recorded per-run in each result file (`host` field);
  headline runs on Apple M5 Max (18 cores), 64 GB, macOS 26.5.1.
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
- **Runs**: each headline number is from `N` runs stated in its section;
  variance across runs is published.
- **Real data**: parse/LOB/store benchmarks run over the captured soak
  corpus (see ops/soak-report.md), not synthetic JSON, unless a section
  explicitly says otherwise (bus benchmarks use the seeded deterministic
  generator to isolate transport cost).
- **Baselines are real implementations**, not strawmen: the serde_json
  baseline is the actual `parse_slow` production fallback path; DuckDB and
  SQLite comparisons use their bundled current releases with stated schemas,
  indexes and pragmas, on identical data.
- **Losses are published.** Where an off-the-shelf engine beats this code,
  the table says so.

## Sections (pending harnesses)

1. `feed` — JSON→Event normalization: msgs/sec/core, fast scanner vs
   serde_json baseline, multiplier, allocations/msg (dhat). *(pending)*
2. `lob` — replay throughput (msgs/sec, single core) BTree vs Ladder;
   top-of-book update latency histogram. *(pending)*
3. `store` — write throughput; bytes/msg vs raw JSON vs Parquet(zstd);
   full-scan GB/s; point-in-time snapshot query latency — head-to-head vs
   DuckDB and SQLite on identical data. *(pending)*
4. `bus` — in-process fan-out latency histograms: seqlock ring vs
   crossbeam-channel fan-out vs tokio::broadcast; cross-network p99 at
   stated sustained rate. *(pending)*
5. `e2e` — exchange→subscriber added-latency decomposition, internet RTT
   measured and subtracted (method stated). *(pending)*
