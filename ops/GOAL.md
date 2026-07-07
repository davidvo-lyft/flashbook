# GOAL (verbatim, received 2026-07-07)

Build, benchmark, and deploy "flashbook": a from-scratch, HFT-style real-time
market-data platform in Rust — exchange feed handlers, a limit-order-book
reconstruction engine, a custom columnar tick store, a binary pub/sub fan-out
server, and a deterministic replay/backtest harness — with rigorously measured,
honestly published performance numbers, a live dashboard on Vercel, and a public
GitHub repo with green CI.

THE GOAL IS MET when ALL of the following are true, verified, and evidenced:
1. GitHub repo (public, personal account) with green CI on the final commit:
   `cargo test` (>= 120 meaningful unit/property tests), `cargo clippy -D warnings`,
   `cargo fmt --check`, plus a CI benchmark smoke job.
2. Live ingestion verified: >= 3 venues (Coinbase Exchange, Binance, Kraken —
   public WebSocket feeds, no API keys) ingested concurrently for a continuous
   >= 24h soak with zero crashes, gap detection/resequencing stats logged, and
   >= 5M messages captured to the tick store. Soak evidence committed
   (ops/soak-report.md with message counts, gap counts, memory ceiling, restart count).
3. BENCHMARKS.md publishes REAL measured numbers with full methodology (hardware,
   OS, isolation steps, warmup, N runs, p50/p90/p99/p999 + max, variance) for:
   a. Feed handler: JSON→internal binary normalization throughput (msgs/sec/core)
      vs a naive serde_json baseline you also write — publish the multiplier.
   b. LOB engine: replay throughput (msgs/sec, single core) rebuilding full L2
      books from captured data; top-of-book update latency histogram.
   c. Tick store: write throughput; compressed bytes/msg vs raw JSON and vs
      Parquet(zstd); full-scan GB/s; point-in-time book snapshot query latency
      over the full captured corpus, benchmarked HEAD-TO-HEAD vs DuckDB and
      vs SQLite on identical data — publish wins AND losses.
   d. Pub/sub: in-process fan-out latency (ns/µs histograms) and cross-network
      p99 at a stated sustained msg rate with a load generator you write.
   e. End-to-end: exchange→subscriber added latency decomposition (excluding
      internet RTT, which you measure and subtract with stated method).
4. Deployed: engine + read API on a small VPS or free-tier host (whatever
   infrastructure is available to you; if none, run the engine locally for the
   soak and deploy the dashboard against a recorded-replay API — say so honestly
   in the README); Next.js dashboard on Vercel showing live/replayed books,
   ingest lag, queue depths, and the headline benchmark table. Dashboard URL
   returns 200 and renders real data.
5. Docs: README (architecture diagram, headline numbers, one-command repro
   `./bench/run-all.sh`), DECISIONS.md (every non-obvious choice with the
   alternatives rejected and why), ATTACKS.md (25 adversarial interview
   questions about this system with answers grounded in specific files/numbers),
   LIMITATIONS.md (what this is NOT — honest).

ARCHITECTURE (Rust workspace; deviate only with a DECISIONS.md entry):
- crates/proto: internal binary message format. Fixed-point integer prices/sizes
  (no f64 in the hot path), #[repr(C)] packed structs, zero-copy read via bytes/
  rkyv or hand-rolled — justify the choice. Sequence numbers, venue timestamps,
  local receive timestamps (monotonic + wall).
- crates/feed: per-venue WebSocket handlers. Zero-allocation-per-message goal on
  the hot path (measure allocations with dhat and publish). Handles reconnects,
  sequence gaps (resnapshot logic per venue's documented protocol), and
  backpressure without unbounded buffering.
- crates/lob: L2 order-book engine. Contiguous price-level storage (vec/BTreeMap
  hybrid or ladder array — benchmark at least two representations and keep the
  winner; publish the comparison). Checksummed against venue-provided book
  checksums where available (Kraken provides CRC32 — use it as a correctness oracle).
- crates/store: append-only columnar tick store. Per-column delta / delta-of-delta
  + zigzag varint encoding, block-structured with sparse time index, mmap reads,
  crash-safe (torn-write detection), point-in-time snapshot reconstruction.
  No dependency on an existing storage engine — that's the point.
- crates/bus: SPSC/MPSC fan-out. Benchmark a hand-rolled ring buffer vs
  crossbeam vs tokio::broadcast; ship the winner, publish all three curves.
- crates/replay: deterministic replay of captured days at configurable speed
  (incl. max-speed for benchmarks); byte-identical book states across runs
  (assert via checksum) — this is your correctness backbone.
- crates/bench: criterion micro-benches + a custom load generator + the
  end-to-end harness. Everything BENCHMARKS.md claims is produced by code in
  this crate, re-runnable by one script.
- apps/dashboard: Next.js on Vercel. Thin. It is evidence, not the project.

NON-NEGOTIABLE RULES:
- NEVER fabricate, extrapolate, or round-up a number. Every figure in README/
  BENCHMARKS.md traces to a committed raw result file (bench/results/*.json).
  If a target is missed, publish the real number plus a bottleneck analysis
  (perf/flamegraph evidence) instead. Honest-but-slower beats fake-fast.
- Publish losses: if DuckDB beats the tick store on full scans (it may),moment
  it's made (not backfilled at the end).
- Commit hygiene: small commits, imperative messages; the history should read
  as an engineering log.

EXECUTION PLAN:
Phase 1 — proto + feed handlers + capture; start the soak capture EARLY (it
needs wall-clock time; run it in the background for the rest of the build).
Phase 2 — LOB engine + replay + checksum correctness oracle against Kraken CRCs.
Phase 3 — tick store + DuckDB/SQLite/Parquet comparison harness.
Phase 4 — bus + load generator + end-to-end latency decomposition.
Phase 5 — dashboard + deploy + CI.
Phase 6 — /loop until met: re-run ./bench/run-all.sh, profile the top bottleneck
(flamegraph), fix it, re-measure, update BENCHMARKS.md — repeat while any goal
gate is unmet or the latest optimization moved p99 by >5%. Then write ATTACKS.md
last, red-teaming your own numbers ("why is your p999 40x your p50?", "why not
io_uring?", "what happens on a Binance resnapshot during a gap?") with answers
citing file:line.
Definition of done = every numbered met-condition above checked off in a final
ops/DONE.md with links/paths to the evidence for each.

---
NOTE (not part of verbatim goal): the line "if DuckDB beats the tick store on
full scans (it may),moment it's made" arrived garbled in the original message.
Interpreted as two rules: (a) publish losses honestly incl. DuckDB wins;
(b) record each DECISIONS.md entry at the moment the decision is made.
