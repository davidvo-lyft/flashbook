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
