# flashbook

An HFT-style market-data platform in Rust, built from scratch against three
live venues (Coinbase Exchange, Binance, Kraken): feed handlers with a
zero-copy fast parse path, a limit-order-book engine verified against the
venue's own CRC32 checksums, a purpose-built columnar tick store with
point-in-time queries, a seqlock broadcast bus, and a deterministic
byte-identical replay harness — all measured honestly. Every number in this
README traces to a committed raw result file in `bench/results/*.json` or to
the generated soak report; the tables are rendered by a script that refuses
hand-typed numbers, and the losses (there are some) are published next to the
wins.

- **Live dashboard (Vercel):** <https://dashboard-kasamixs-projects.vercel.app>
  (mirror: <https://davidvo-lyft.github.io/flashbook/>) — serves an exported
  replay dataset, not a live feed; see [Deployment](#deployment-status-honest).
- **Repo:** <https://github.com/davidvo-lyft/flashbook> — CI green: `fmt`,
  `clippy -D warnings`, 256 tests, bench smoke.

## Architecture

```
   Coinbase Exchange        Binance           Kraken        public WS feeds
          │                    │                 │           (JSON over TLS)
          └──────────┬─────────┴────────┬────────┘
                     ▼                  ▼
          ┌─────────────────────────────────────┐
          │ crates/feed                         │  per-venue codecs: zero-copy
          │   WS connections + codecs           │  fast path w/ serde_json slow
          │   (fast / slow parse paths)         │  path fallback, differentially
          │   bin: capture                      │  checked; REST book snapshots
          └──────────────────┬──────────────────┘
                             ▼
          ┌─────────────────────────────────────┐
          │ data/raw — capture segments         │  CRC32C-framed raw venue bytes
          │   (GROUND TRUTH, append-only)       │  + mono/wall stamps + embedded
          │                                     │  REST snapshots; re-ingestable
          └──────────────────┬──────────────────┘
                             ▼
          ┌─────────────────────────────────────┐
          │ crates/replay                       │  deterministic merge keyed on
          │   bins: replay-verify, ingest,      │  (recv_mono_ns, venue); codec
          │         export-dashboard            │  resets mirror capture exactly
          └───────┬──────────┬─────────┬────────┘
                  ▼          ▼         ▼
    ┌──────────────┐ ┌─────────────┐ ┌──────────────────┐
    │ crates/lob   │ │ crates/store│ │ crates/bus       │
    │ book engine  │ │ columnar    │ │ seqlock broadcast│──► subscribers
    │ BTree+Ladder │ │ tick store, │ │ ring: lossy but  │    (every sub sees
    │ + CRC32      │ │ PIT snapshot│ │ loss-DETECTING,  │     every event or
    │ oracle checks│ │ queries     │ │ never blocks     │     knows the count)
    └──────┬───────┘ └─────────────┘ └──────────────────┘
           │ export-dashboard
           ▼
    ┌──────────────────┐
    │ apps/dashboard   │  Next.js; consumes the committed exported
    │ (GitHub Pages)   │  replay dataset (D-013)
    └──────────────────┘
```

The seven crates plus the dashboard:

| unit | what it is |
|---|---|
| `crates/proto` | shared vocabulary: the 64-byte `#[repr(C)]` POD event (D-004), global 1e-8 fixed-point (D-003), monotonic clock, and the Kraken CRC32 book oracle (D-006 — pure format math, so lob/replay verify books without a transport dep) |
| `crates/feed` | venue WS connections, per-venue codecs (fast scanner + `serde_json` slow path), the `capture` soak binary |
| `crates/lob` | book reconstruction: `BTreeBook` (shipped default, D-014) and `LadderBook` (benchmarked alternative + property-test partner) |
| `crates/store` | append-only columnar tick store: per-column delta/DoD/zigzag encodings + optional per-block zstd (D-008), mmap scan, PIT snapshot index |
| `crates/bus` | seqlock broadcast ring (D-009): producer never blocks or allocates; a lagging subscriber loses data and is told exactly how much |
| `crates/replay` | deterministic replay over raw segments (D-010); `replay-verify` (CRC + digest checks), `ingest` (store build), `export-dashboard` |
| `crates/bench` | all benchmark harnesses, incl. the DuckDB/SQLite/Parquet head-to-head behind the `compare` feature (D-012) |
| `apps/dashboard` | Next.js dashboard over the exported dataset |

## Headline numbers

Machine: Apple M5 Max (18 cores, 64 GB), macOS — a laptop, not a tuned Linux
box; methodology and caveats in `BENCHMARKS.md` and `LIMITATIONS.md`.
Parse/LOB/store benchmarks run over the real 226M-event captured corpus, not
synthetic JSON.

| metric | value | source |
|---|---|---|
| Feed fast path, aggregate 3 venues | 3.78 M msgs/s (4.91× the `serde_json` slow path) | `BENCHMARKS.md` ← `feed_parse.json` |
| Feed fast path, Kraken | 8.87 M msgs/s (7.55× slow path) | `BENCHMARKS.md` ← `feed_parse.json` |
| Kraken fast-path allocations | 0 allocs/frame, 0 bytes (dhat, 10,000 frames) | `BENCHMARKS.md` ← `feed_alloc.json` |
| LOB replay throughput (BTreeBook, full corpus) | 24.27 M events/s | `BENCHMARKS.md` ← `lob_replay.json` |
| Top-of-book update latency (btree) | p50 41 ns (p99 125 ns) | `BENCHMARKS.md` ← `lob_replay.json` |
| Store footprint | 9.10 B/event — 6.84× smaller than raw JSON, and smaller than Parquet-zstd (9.47 B/event) | `BENCHMARKS.md` ← `store_write.json`, `store_compare.json` |
| Store full scan vs DuckDB | DuckDB wins: 234.9 ms vs our 9.873 s (~42× faster) — the published loss | `BENCHMARKS.md` ← `store_compare.json` |
| Bus ring delivery, 1 subscriber (paced 500 k/s) | p50 125 ns / p99 292 ns, 0 lost | `BENCHMARKS.md` ← `bus_fanout.json` |
| Bus ring throughput, 1 subscriber | 11.47 M msgs/s, 0 lost | `BENCHMARKS.md` ← `bus_fanout.json` |
| E2E live added latency, socket-read → subscriber (steady state, 3 live venues) | p50 2.29 µs / p99 34.46 µs | `BENCHMARKS.md` ← `e2e_live.json` |
| Soak | 55.8 M msgs / 226 M events / 0 crashes / 0 gaps | `ops/soak-report.md` |
| Full-corpus CRC verification | 41,692,848 Kraken checksums, 0 mismatches | `ops/soak-report.md` |

## Correctness story

Performance claims are cheap; this project's differentiator is that the data
is provably right:

- **Venue CRC32 oracle.** Kraken publishes a CRC32 checksum of the book with
  every update. Replaying the entire 25-hour capture reconstructs the books
  and checks all 41,692,848 checksums: 0 mismatches. This is the venue
  grading our book engine, update by update.
- **Deterministic, byte-identical replay.** Raw capture segments are ground
  truth (D-001); replay merges them deterministically (D-010) and
  `replay-verify --twice` asserts the double replay produces identical event
  and book digests. The store's byte-identity is checked with SHA-256 in
  tests.
- **Three-backend parity harness.** The DuckDB/SQLite/Parquet head-to-head
  asserts full-scan aggregates and PIT top-of-book EQUAL across backends
  *before* any timing is quoted. This is not theater: during the official
  runs it caught (a) `sum(qty)` overflowing i64 at 226M events — now split
  into two BIGINT sums recombined as i128 — and (b) DuckDB's integer `/`
  being Postgres-style *float* division, which silently rounded the split
  terms until the parity assertion tripped (the fix uses `>>`/`&`).
- **Differential fast/slow parsing.** The zero-copy fast path and the
  `serde_json` slow path are asserted to produce equal event counts per
  venue over the full corpus; the slow path is also the production fallback,
  so the benchmark baseline is a real code path, not a strawman.
- **Cross-representation property tests.** `BTreeBook` and `LadderBook` are
  checked against each other and a reference model; replay digests are
  representation-independent (verified when D-014 swapped the default).
- **256 tests** in CI, plus `clippy -D warnings` and a bench smoke run.

## Reproduce the numbers

Everything regenerates from `bench/results/*.json` only — `bench/render.py`
will not write a number it cannot trace to a result file.

```sh
# prerequisite: an ingested store built from the raw capture corpus
cargo run --release -p flashbook-replay --bin ingest -- \
  --data data/raw --out data/store/full.fbstore --zstd 3 --kraken-depth 100

# run every benchmark (writes bench/results/*.json; refuses silent overwrite)
./bench/run-all.sh

# re-render BENCHMARKS.md from the result files
bash bench/render.sh --write

# re-render the soak report from committed telemetry
python3 ops/gen-soak-report.py
```

## Deployment status (honest)

- **Dashboard: live on GitHub Pages** at
  <https://davidvo-lyft.github.io/flashbook/>, serving a committed exported
  replay dataset (book timeline + soak stats + benchmark tables) generated
  from the captured corpus. Why not live data: no tunnel/VPS credentials
  existed on the build machine (D-013), and faking a "live" websocket over
  canned data would be dishonest. The engine and read API run locally against
  the same data.
- **Engine + capture ran locally** on the dev laptop for the soak.
- **Vercel:** `cd apps/dashboard && vercel --prod` is the whole deploy; it is
  pending one interactive `vercel login` and documented as such.
- **The soak's one NOT-MET gate**, stated plainly: the continuous-24h
  requirement. The capture process ran a 25.05 h span with 0 crashes and 0
  restarts, but the laptop slept 18 times (lid/power, outside software
  control), so the longest hole-free window is 11.2 h. Connections
  auto-recovered after each sleep; `ops/soak-report.md` reports the holes via
  stats cadence rather than papering over them. A VPS migration is planned
  for the continuous-24h re-run.

## Build & run

```sh
rustup toolchain install     # version pinned by rust-toolchain.toml
cargo test --workspace       # 256 tests; DuckDB/SQLite live behind the
                             # non-default `compare` feature so this stays fast
cargo build --workspace --release
```

Capture (the soak binary — connects to all three venues, appends raw frames
to CRC-framed rotating segments, emits per-minute JSONL stats):

```sh
cargo run --release -p flashbook-feed --bin capture -- \
  --data-dir data/raw \
  --venues coinbase,binance,kraken \
  --symbols BTC,ETH,SOL,XRP,DOGE
```

Soak operations live in `ops/`: `start-soak.sh` / `stop-soak.sh`,
`soak-status.sh`, `soak-watchdog.sh` (restarts are honestly counted), and
`gen-soak-report.py`.

## Documentation map

| file | contents |
|---|---|
| [`BENCHMARKS.md`](BENCHMARKS.md) | every measured number + full methodology; generated from `bench/results/*.json` |
| [`DECISIONS.md`](DECISIONS.md) | D-001..D-014 — every non-obvious choice, recorded when made, alternatives included |
| [`LIMITATIONS.md`](LIMITATIONS.md) | what this is not (laptop, loopback, L2-not-L3, JSON-not-ITCH, …) |
| [`ATTACKS.md`](ATTACKS.md) | adversarial Q&A against the claims |
| [`ops/soak-report.md`](ops/soak-report.md) | generated soak evidence, including the NOT-MET gate |
| [`ops/GOAL.md`](ops/GOAL.md) | the original goal spec this was built and graded against |

## License

MIT — see [`LICENSE`](LICENSE).
