# DECISIONS.md — engineering decisions, recorded when made

Format: D-NNN (date) — decision. Alternatives considered and why rejected.
Rule (from goal): every non-obvious choice gets an entry at the moment it's made.

---

## D-001 (2026-07-07) — Capture raw WebSocket frames first; normalize later, re-runnably

The soak capture writes the raw venue payload bytes (plus local monotonic + wall
receive timestamps) to CRC32C-framed append-only segment files, with periodic
REST book snapshots embedded in-stream. Normalization into the binary event
format (and later, tick-store ingestion) runs over these raw segments and can be
re-run at any time.

- Why: the soak needs wall-clock time and must start before the tick store
  exists (Phase 3). Raw bytes are ground truth: any parser bug found later is
  recoverable by re-ingesting; a normalize-only capture bakes bugs into the
  only copy of the data. Raw capture also feeds the feed-handler benchmark
  (3a) with real message distributions instead of synthetic JSON.
- Alternatives rejected:
  - Normalize-only capture: unrecoverable if a codec bug is found post-hoc.
  - Capture directly into the tick store: store doesn't exist yet; would delay
    the 24h soak by days.
  - tcpdump/pcap capture: TLS makes payloads opaque; would need key logging.
- Cost accepted: disk (raw JSON is fat; ~30-60 GB/day estimated). 1.5 TiB free;
  closed segments are zstd-compressed by a compactor.

## D-002 (2026-07-07) — tokio + tokio-tungstenite (rustls) for the connection layer

- Why: the soak's hard requirement is 24h zero-crash operation with reconnects,
  TLS, ping/pong keepalive, and per-venue resubscribe logic. tokio-tungstenite
  is the most battle-tested async WS stack in Rust. At soak rates (hundreds of
  msg/s aggregate) the WS transport is nowhere near the bottleneck; the
  performance budget goes into parsing/normalization (benchmark 3a), which is
  transport-independent.
- Alternatives rejected:
  - fastwebsockets: faster frame handling, but thinner ecosystem around
    long-lived reconnect ergonomics; transport speed is not this system's
    bottleneck at these rates.
  - Hand-rolled WS over hyper upgrade: days of effort for no measured need;
    would still sit behind the same TLS stack.
- Revisit trigger: if e2e decomposition (3e) shows WS read dominating, measure
  fastwebsockets head-to-head and record the numbers.

## D-003 (2026-07-07) — Global fixed-point scale 1e-8 (i64 mantissa) for price and quantity

- Why: one global scale means any two mantissas compare/add without rescaling,
  book levels sort as plain i64, and the columnar store delta-encodes raw i64
  streams. i64 at 1e-8 covers prices to ~$92e9 and quantities to ~9.2e10 units:
  ample for the chosen majors (BTC, ETH, SOL, XRP, DOGE vs USD/USDT), whose
  venue tick/lot precisions are <= 1e-8.
- Guard: instrument metadata is validated at subscribe time; a venue precision
  finer than 1e-8 is a hard error, not silent rounding.
- Alternatives rejected:
  - Per-instrument scale: strictly more general (would admit 1e-10-tick meme
    pairs), but every cross-instrument comparison and every store column would
    carry a scale header; complexity not needed for the chosen universe.
    Documented in LIMITATIONS.md.
  - f64: banned by the goal (and by sense: non-exact equality breaks book
    level identity and checksums).
  - Decimal crates (rust_decimal): 128-bit, heap-free but 2x width and slower
    arithmetic in the hot path; used in tests as a cross-check oracle only.

## D-004 (2026-07-07) — Hand-rolled 64-byte #[repr(C)] POD event + bytemuck zero-copy; rkyv rejected

- The normalized event is a single fixed-size 64-byte #[repr(C)] struct
  (one cache line), read zero-copy from mmap'd/byte buffers via bytemuck Pod
  casts (alignment- and size-checked at compile time; little-endian asserted
  at build time — this system targets LE only).
- Why hand-rolled: the stream is a homogeneous sequence of fixed-size records —
  no variable-length fields, no schema evolution requirement, no nested
  structures. Zero-copy here is a pointer cast; a serialization framework adds
  API surface, proc-macro compile cost, and versioning ceremony for zero
  benefit at this shape.
- Alternatives rejected:
  - rkyv: excellent for complex object graphs; pointless for a flat POD array,
    and its archived types complicate the columnar store's column extraction.
  - flatbuffers/capnp: schema+codegen toolchain cost; vtable indirection on
    every field read in the hottest loop.
  - bytes-only manual offsets: what bytemuck does, minus the compile-time
    safety.
- Snapshots are encoded as bracketed runs of the same 64B event (SnapshotBegin,
  N x SnapshotLevel, SnapshotEnd) so every downstream consumer (store, replay,
  bus) handles exactly one record shape.

## D-005 (2026-07-07) — Capture binary lives in crates/feed/src/bin/capture.rs

- Why: the goal fixes the crate list; capture is operationally a feed-handler
  process (connect, normalize, sink). Keeping it in crates/feed avoids an
  undeclared eighth crate and keeps codecs+transport+capture co-versioned.
- Alternative rejected: separate crates/capture (cleaner compile unit, but a
  structural deviation for marginal benefit).

## D-006 (2026-07-08) — Kraken CRC32 oracle lives in proto, not feed

`kraken_book_crc32` is pure format math (mantissas + precisions -> CRC32);
placing it in proto lets crates/lob and crates/replay verify books without a
transport dependency. Alternative rejected: feed-only (forces lob->feed dep,
inverting the layering); duplicating it (two implementations of a
correctness oracle is how oracles rot). Validated against 2962+ live venue
checksums before first use.

## D-007 (2026-07-08) — Two book representations, best-at-end ladder layout

BTreeBook (BTreeMap per side) vs LadderBook (sorted Vec per side). The
ladder stores bids ascending and asks DESCENDING so the best level of both
sides sits at the vec END: real L2 traffic clusters near the top, so
insert/remove memmoves touch only the short tail and best-of-book reads are
O(1). Both must be behaviorally identical (cross-impl + reference-model
property tests, same state digests); the replay benchmark picks the shipped
one (3b publishes the comparison). Books support an optional per-side depth
cap because Kraken v2 book@depth is maintained AT depth — clients must drop
levels pushed beyond it or stale deep levels later resurface as phantom
liquidity (empirically confirmed: CRC checksums only match with truncation
enabled).

## D-008 (2026-07-08) — Per-column encodings: DoD for clocks, delta for
sequences/prices, plain zigzag for sizes, optional per-block zstd

Delta-of-delta suits near-constant-increment receive clocks; venue_seq and
trade ids are +1-ish (delta ~1 byte); prices random-walk near the book
(delta 1-3 bytes); quantities are repetitive small magnitudes but not
trending (plain zigzag). Blocks optionally zstd the concatenated columns and
keep whichever is smaller. Alternatives rejected: one generic encoding for
all columns (measurably worse fit), FastPFOR/bit-packing SIMD libraries
(dependency against the from-scratch goal; varints are simple, portable and
already far below 64 B/event — the benchmark, not vibes, will judge),
adaptive per-block encoding selection (complexity deferred until 3c numbers
justify it).

## D-009 (2026-07-08) — Bus is a seqlock broadcast ring, loss-detecting, never blocking

Market-data fan-out semantics: every consumer sees every event, a slow
consumer loses data and KNOWS it (Lagged{lost}), and the producer never
blocks or allocates. Slots are [AtomicU64; 8] guarded by per-slot version
counters using crossbeam-utils' SeqLock ordering recipe — no non-atomic
reads, so no UB, unlike the classic UnsafeCell seqlock. Alternatives
rejected: bounded MPMC queues (queue semantics deliver each event to ONE
consumer — wrong shape); per-subscriber bounded channels with producer-side
blocking (a slow subscriber would stall the feed); tokio::broadcast as the
in-process primitive (it is one of the 3d benchmark contenders, not the
default — the numbers decide the shipped transport).

## D-010 (2026-07-08) — Replay merges by (recv_mono_ns, venue) and resets codecs at connect NOTEs

Cross-venue merge keys on the capture process's monotonic clock (all venues
were stamped by the same process), ties broken by venue id then within-file
order — fully deterministic. Codec state resets exactly where capture's
did (a fresh codec per WS session), mirrored via the in-stream
{"event":"connect"} NOTE records, so replay reproduces capture's parse
decisions rather than approximating them.

## D-011 (2026-07-08) — Benchmark honesty is mechanical, not aspirational

Percentiles are nearest-rank over raw samples (no interpolation/fitting;
small-n high percentiles saturate at max and say so). Every result file
embeds host, config, n, warmup; files are written atomically and REFUSE
silent overwrite. BENCHMARKS.md is generated from result files only.
Alternative rejected: quoting criterion summaries by hand into markdown
(that's how numbers drift from evidence).

## D-012 (2026-07-08) — DuckDB/SQLite live behind the bench `compare` feature

The head-to-head harness (goal 3c) bundles DuckDB and SQLite via their
`bundled` crate features for hermetic, version-pinned comparisons — but the
bundled DuckDB build costs ~10 minutes cold. Gating them behind
`flashbook-bench`'s non-default `compare` feature keeps `cargo test`/CI/dev
loops fast; `bench/run-all.sh` builds with `--features compare`.
Alternatives rejected: system-installed engines via CLI (version drift,
process-spawn overhead pollutes latency comparisons, no duckdb CLI on this
machine); making them default deps (every CI run pays 10 minutes for
binaries the tests never exercise).

## D-013 (2026-07-08) — Dashboard data path: exported replay dataset, not a live tunnel

No Vercel/tunnel/VPS credentials exist on this machine (scouted 2026-07-07),
so the deployed dashboard consumes a committed, exported replay dataset
(book timeline + soak stats + benchmark table generated from the captured
corpus by an exporter binary) served statically/serverlessly — exactly the
goal's sanctioned fallback, stated honestly in the README. The engine and
read API run locally against the same data. Alternatives rejected:
tunneling localhost (no ngrok/cloudflared auth; fragile evidence), fake
"live" websockets replaying canned data while claiming liveness (dishonest).

## D-014 (2026-07-09) — BTreeBook ships as the default book (the benchmark decided)

The official 3b run over the full 226M-event corpus: BTreeBook 24.27M
events/s vs LadderBook 10.06M (2.4x), top-of-book p50 41ns vs 42ns but
ladder's p90+ collapses (2.4µs vs 42ns) on deep-book updates — real feeds
update deep levels constantly, and a contiguous vec pays O(n) memmove
there while the ladder's best-at-end trick only helps near the top.
Production bins (replay-verify, ingest, export-dashboard) switched to
BTreeBook; digests are representation-independent (verified: identical
smoke digests before/after the swap), so no stored artifact changed
meaning. LadderBook stays in-tree as the benchmarked alternative and the
cross-implementation property-test partner. The published early smoke
signal (10x) overstated the full-corpus gap (2.4x) — both numbers are in
the result files.

## D-015 (2026-07-09) — Block format v2: column-offset table for pruned scans

The official 3c run exposed a 42x full-scan loss to DuckDB whose root cause
was format, not fold: v1 block bodies are concatenated varint streams with
no offsets, so any scan decodes all 11 columns and materializes whole
events. v2 appends 11 u32 per-column byte lengths to the block header
(~44 B on ~75 KB blocks); readers accept v1 and v2 forever (v1 pinned by
committed binary fixtures), writers emit v2, and `scan_columns` decodes
only selected columns (the compare fold now touches 4). Per-block zstd is
not seekable, so pruning saves decode work but not decompression — the gap
narrows, it does not close (informal same-fold measurements: 1.7-2.75x;
official re-measurement deferred to AC power for comparability, see
LIMITATIONS). Alternatives rejected: per-column compression frames
(seekable, but a bigger format break for a scan path that is explicitly
not this store's primary job); leaving it (the loss was published, but a
44-byte header fix that halves scan cost is not gold-plating).

## D-016 (2026-07-09) — REST cross-validation for the venues without checksums

Kraken's CRC32 is the only venue-provided oracle. For Coinbase/Binance,
replay now scores every periodic REST snapshot against the live
reconstructed book BEFORE applying it: top-10 overlap as exact
(price, qty) pairs and as price-only. It is a statistical cross-check, not
an oracle — the REST body is fetched while the WS stream keeps mutating
the book, so quantity churn makes high-but-not-100% the healthy reading.
Full-corpus result: 556/561 snapshots scored, price overlap p50 95% /
p90 100% (exact p50 40%). Deterministic (pure function of stream content;
digests unchanged). Also added: a startup tripwire fetching Kraken
AssetPairs and warning on precision drift against the pinned table
(non-fatal; the CRC oracle remains the hard check).
