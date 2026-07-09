# ATTACKS.md — 25 adversarial questions, answered

The questions a hostile interviewer (or a careful reviewer) should ask about
flashbook, each answered from the code and the official results. Rules of
this document: every performance number comes from `BENCHMARKS.md`
(generated from `bench/results/*.json`) or `ops/soak-report.md` (generated
from committed telemetry); every code claim cites `file:line` as of the
commit this document was written at. Companion docs: `LIMITATIONS.md`
(what this is not), `DECISIONS.md` (D-001..D-014, why it is what it is).

---

## Theme 1 — Correctness

### 1. Walk me through the Kraken CRC32 formula and how you discovered the depth-truncation semantic.

Kraken's v2 book checksum takes the top 10 ask levels (ascending price)
then the top 10 bid levels (descending); for each level the price is
formatted at the pair's price precision, the decimal point removed, leading
zeros stripped, digits appended, then the same for the quantity; the
checksum is CRC32 (IEEE) over the concatenated ASCII digits
(`crates/proto/src/kraken_crc.rs:5-11`, implementation
`kraken_crc.rs:64-80`, the top-10 `take(10)` at `kraken_crc.rs:71` and
`:75`). Formatting streams digits from a stack buffer into the hasher
(allocation-free, `kraken_crc.rs:42-49`); a zero value contributes no bytes
at all (`kraken_crc.rs:39-41`), and a mantissa finer than the pair's
precision saturates into a *wrong* CRC deliberately — corrupt input must
trip the oracle, not pass silently (`kraken_crc.rs:24-38`). Pair precisions
are a pinned 2026-07-07 snapshot of `/0/public/AssetPairs` with the oracle
itself as the tripwire if Kraken ever reprices a pair
(`crates/feed/src/kraken.rs:30-46`). The truncation semantic: Kraken v2
`book@depth` is maintained *at* the subscription depth, so a client that
keeps levels pushed beyond depth accumulates stale deep levels as phantom
liquidity — empirically, checksums only matched with truncation enabled
(D-007), which is why both book implementations carry an optional per-side
depth cap enforced after every mutation
(`crates/lob/src/ladder.rs:36-41` and `:54-64`). Validation went from 2962
consecutive live checksums during development (`kraken_crc.rs:13-16`) to
**41,692,848 verified, 0 mismatches** over the full soak corpus
(ops/soak-report.md).

### 2. What happens on a Binance resnapshot during a gap? Walk the code.

A diff whose `U` (first update id) exceeds `last_u + 1` while `Synced` is a
sequence gap: `finish_depth` truncates the level events already appended
for that frame, emits one `Gap` event carrying the missed count, flips the
instrument to `Unsynced { high_u: Some(final_u) }`, and returns
`Signal::NeedResync` (`crates/feed/src/binance.rs:182-204`). The connection
layer reacts by marking that instrument's REST target due immediately
(`crates/feed/src/conn.rs:559-562`). While `Unsynced`, every subsequent
diff is fully parsed and validated but dropped, and the highest `u` seen is
tracked as a high-water mark (`binance.rs:165-174`). When the REST
`/api/v3/depth` body arrives, `snapshot_inner` rejects any snapshot whose
`lastUpdateId` is below that mark — anchoring there would silently skip
updates (documented sync step 4) — with `Err("stale rest snapshot")`
(`binance.rs:671-678`), and `fetch_rest` retries with per-target
exponential backoff (1 s floor, 60 s cap) without touching the WS session
(`conn.rs:717-726`, `:734-739`). A fresh-enough snapshot emits
`Clear` + `SnapBegin`/`SnapBid`/`SnapAsk`/`SnapEnd` and anchors
`Synced { last_u: lastUpdateId }` (`binance.rs:702-719`); stale
post-snapshot diffs then drop via `final_u <= last_u`
(`binance.rs:177-181`) and the documented first-diff straddle applies via
`U <= last_u + 1 <= u` (`binance.rs:206-209`). Over the 25.05 h soak:
215 Binance REST snaps, 0 gaps, 0 resyncs (ops/soak-report.md).

### 3. Coinbase level2 has no per-message sequence number in your pipeline. How do you claim gap detection there?

Honestly and narrowly: Coinbase documents that dropped messages occur, and
`l2update` carries no per-message sequence, so trade-id and heartbeat
continuity are the only in-band evidence of loss
(`crates/feed/src/coinbase.rs:19-28`). The codec keeps a per-instrument
last-trade-id baseline: a gap-checked `match` that jumps past `last + 1`
emits a `Gap` and requests a resync (`coinbase.rs:132-145`), and every
`heartbeat` cross-checks the venue's `last_trade_id` against the trades we
actually saw, catching drops even in quiet books (`coinbase.rs:148-160`).
The baseline is advance-only, so an out-of-order straggler (which Coinbase
says can legitimately arrive) never re-reports an already-reported gap
(`coinbase.rs:129-131`). Any detected gap triggers a REST `/book?level=2`
re-snapshot of that instrument, and periodic REST cross-snapshots run
regardless (346 for Coinbase during the soak). LIMITATIONS.md states this
plainly: it is strong evidence — differential fast/slow parity,
deterministic replay, periodic snapshots — but weaker than Kraken's
per-update venue checksum, and the soak's Coinbase gap count of 0 is a
claim about detected discontinuities, not a proof of zero loss.

### 4. Seqlocks are famously UB-prone and you're on ARM (Apple Silicon), the poster child for weak memory. Why is your ring correct?

The classic seqlock bug is non-atomic data reads racing the writer — UB in
the C++/Rust memory model even when it "works". This ring never performs a
non-atomic data access: each 64-byte event is stored as `[AtomicU64; 8]`
(`crates/bus/src/ring.rs:37`) and every load/store of it is atomic, per
crossbeam-utils' `SeqLock` recipe (module doc `ring.rs:8-20`, D-009). The
orderings pair exactly: writer does version `+1` (odd), `fence(Release)`,
relaxed data stores, version `+2` with `Release` (`ring.rs:99-105`); reader
does `version.load(Acquire)`, relaxed data loads, `fence(Acquire)`, then a
second version load, accepting only if both reads agree on the expected
even value (`ring.rs:176-191`) — the fences create the acquire/release
edges that weak ARM ordering would otherwise reorder away. The version
encodes the lap (`2 * (s/capacity + 1)`, `ring.rs:59-64`), so a consumer
classifies a slot as unwritten/ready/overwritten without trusting `head`.
A multi-threaded stress test derives every event field from its sequence
and asserts the relationships on every receive, so any torn read fails
loudly (`ring.rs:341-394`) — and all of this ran and was benchmarked on the
ARM machine itself (Apple M5 Max, BENCHMARKS.md host table), with 0 lost
events at every subscriber count in the fan-out tables (`bus_fanout.json`).

### 5. What are your actual crash-recovery guarantees — not the marketing version?

Precise version: capture appends the raw venue payload to a CRC32C-framed
segment *before* parsing it (`crates/feed/src/conn.rs:546-547`, D-001), so
a codec crash cannot lose ground truth; a torn tail costs at most the
record in flight. The tick store's unit of recovery is the block: fixed
header + CRC32 over the stored body (`crates/store/src/block.rs:9-17`), a
CRC or length mismatch decodes as `Torn` (`block.rs:216-218`), and a reader
stops at the first torn/corrupt block with `recover_truncate` chopping the
file back to the last valid block (`crates/store/src/segment.rs:29-32`,
`:641`). The PIT sidecar distinguishes torn from corrupt and is disposable
by design — rebuild from the segment (`crates/store/src/pit.rs:30-34`).
There is no fsync-per-record durability claim: this is a capture pipeline,
not a database WAL; what is claimed is that everything already on disk
stays readable and a torn tail is detected, bounded, and truncatable.
Evidence: the full-corpus replay found **0 torn tails** (expected ≤
restarts + live tail; ops/soak-report.md), and a proptest feeds arbitrary
garbage to the block decoder to prove it never panics
(`block.rs:464-469`).

### 6. Replay determinism is easy to claim and hard to have. Which nondeterminism sources did you actually eliminate?

Enumerated: (1) cross-venue ordering — all venues were stamped by one
capture process's monotonic clock, and replay merges ascending
`recv_mono_ns` with ties broken by (venue, within-file order)
(`crates/replay/src/source.rs:6-7`, D-010), so the merge is a pure function
of the files. (2) codec state — capture built a fresh codec per WS session,
and replay resets codecs at exactly the in-stream `{"event":"connect"}`
NOTE records (`crates/replay/src/run.rs:196-200`), reproducing capture's
parse decisions rather than approximating them. (3) parse policy — the same
fast-path/slow-fallback ladder runs in replay as in capture
(`run.rs:144-168` vs `conn.rs:241-247`). (4) aggregation — the event-stream
digest is a sequential FNV-1a fold over each event's 64 bytes
(`run.rs:78-88`, `:209`), no hash-map iteration order anywhere in the
digest path. Proof, not promise: `replay-verify --twice` replays the corpus
twice and hard-asserts identical digests
(`crates/replay/src/bin/replay-verify.rs:2`, `:110`) — official digests
events `928fae558177d6dc`, books `731fd594dbf3d08c` (ops/soak-report.md) —
and ingest is byte-identical across runs, asserted on whole file bytes
(`crates/replay/src/bin/ingest.rs:383-409`). Scope stated honestly: the
*live capture* is not deterministic (it's the internet); replay *of a
capture* is.

---

## Theme 2 — Performance

### 7. Why is your p999/max so far above p50? A real HFT system wouldn't tolerate that.

Because the numbers are honest about the platform: macOS on a laptop SoC,
no `isolcpus`, no IRQ steering, no core pinning (macOS has no public
affinity API), so p999/max contain scheduler and power-management noise by
construction (LIMITATIONS.md, "Platform"; BENCHMARKS.md methodology).
Concretely: BTreeBook top-of-book updates run p50 41 ns / p999 167 ns but
max 339.54 µs — a ~8,000× p50→max spread that is a descheduling artifact,
not book math (`lob_replay.json`); the ring at 8 subscribers runs p50
416 ns / p999 43.17 µs / max 1.44 ms (`bus_fanout.json`). Two further
mechanical contributors are published rather than hidden: every latency
sample includes one `Instant::now()/elapsed()` pair, measured at 31.0 ns on
the official run and published, not subtracted (BENCHMARKS.md 3b), and the
e2e "total added" p999 of 2.56 ms is dominated by draining the initial
full-book snapshot frame — the steady-state series excluding snapshot
events reads p50 2.29 µs / p999 82.83 µs / max 1.10 ms (`e2e_live.json`,
producer note 6). On a tuned Linux box the tails would compress
substantially (see Q13); the p50s are already transport- and OS-light. The
alternative — quoting trimmed or fitted tails — is exactly what D-011
forbids: percentiles are nearest-rank over raw samples
(`crates/bench/src/percentile.rs:3`, `:51`).

### 8. DuckDB beats your full scan by 42×. Why should anyone use your store?

Correct, and published in bold: ours 9.873 s vs DuckDB 234.9 ms on the
identical 226,404,844-event aggregate — 42× (`store_compare.json`; the
result file's own `winners` field says `full_scan → duckdb`). The reason is
structural: `SCAN_SQL` touches 4 of 11 columns
(`crates/bench/src/compare.rs:71-74`), and DuckDB prunes to exactly those
columns and runs vectorized aggregation kernels, while our reader decodes
*every* column of *every* event back into 64-byte structs
(`crates/store/src/block.rs:241-273`) and folds row-at-a-time
(`compare.rs:274-295`). Closing the gap is legible work, not magic: the
block body is already columnar in a fixed order (`block.rs:19-23`), so
projection pushdown (decode only requested columns) plus batched
aggregation would recover most of it — deferred because the benchmark, not
vibes, decides when that complexity is bought (D-008). What the store
actually won: smallest size (9.10 B/event, 6.84× under raw JSON, vs
DuckDB's 25.89 B/event) and the write path — re-encoding streams at
15.69 M events/s with zstd (`store_write.json`) against DuckDB's 96.347 s
Appender load (~2.35 M/s derived) and SQLite's 162.649 s. Note DuckDB also
edged PIT p50 (43.93 ms vs our 119.44 ms), though our floor is 250 ns vs
its 3.33 ms because index misses are near-free lookups; for a
capture-side, append-only, PIT-correct store, winning ingest and size while
losing analytical scans to a vectorized OLAP engine is the trade the design
chose — and the table says so plainly.

### 9. You beat Parquet-zstd by ~4% on size. Why, and when does Parquet win?

Ours stores the corpus in 2,061,078,204 B (9.10 B/event) vs Parquet-zstd's
2,143,484,948 B (9.47 B/event) — about 4% smaller (`store_compare.json`).
The edge is domain knowledge encoded per column before zstd ever runs:
delta-of-delta for the near-constant-increment receive clocks, plain delta
for venue sequences and prices (which random-walk near the book), zigzag
for quantities, varint for instrument ids, raw byte runs for
kind/venue/flags (`crates/store/src/block.rs:19-23`, encoders invoked at
`block.rs:110-121`, rationale in D-008) — Parquet's general-purpose
encodings can't assume "this i64 column is a monotone nanosecond clock".
Blocks also keep whichever of compressed/raw is smaller
(`block.rs:124-134`). Parquet wins on almost everything else: it loaded in
6.705 s (fastest load in the table), and it brings schema evolution,
nested types, dictionary encoding for strings, and a universe of readers —
none of which a fixed 64-byte single-schema event stream needs. A 4% size
win against the industry format is evidence the domain encodings are real;
it is not an argument to use fbstore for general data.

### 10. Your ladder book was 10× slower in the smoke run but 2.4× in the official run. Which number is true?

Both — they measured different corpus shapes, which is the lesson. The
official full-corpus run (226,404,844 events, 5 measured passes) gives
BTreeBook 24.27 M events/s vs LadderBook 10.06 M/s, a 2.4× gap; the early
smoke signal on a small slice said ~10×, and D-014 records explicitly that
the smoke figure overstated the gap — both numbers live in result files.
The mechanism: real L2 feeds constantly update *deep* levels, where a
contiguous vec pays O(n) memmove and the ladder's best-at-end layout (D-007)
stops helping — visible in the latency split, p50 41 ns vs 42 ns
(essentially tied at the top) but p90 42 ns vs 2.38 µs (`lob_replay.json`).
Corpus mix determines which regime dominates, so a short window with a
different depth distribution produces a different multiple; that is why
LIMITATIONS.md's honesty rules say preliminary smoke figures are never
promoted to official without a quiesced full re-run. The decision followed
the official data: BTreeBook ships as default, LadderBook stays in-tree as
the benchmarked alternative and property-test partner (D-014), and both
must produce identical digests (`digests_match = true` in
`lob_replay.json`).

### 11. Capture peaked at 44 MB RSS. Cute — what breaks at 100× the message rate?

Not the parser, and the numbers say why: the soak averaged ~619 msgs/s
(55,819,963 msgs over 25.05 h, derived from ops/soak-report.md), so 100×
is ~62 k msgs/s aggregate, while the *slowest* venue fast path parses at
2.37 M msgs/s on one core and the aggregate at 3.78 M/s
(`feed_parse.json`) — two orders of magnitude of headroom. What actually
binds, in order: (1) raw-capture disk — 13,898,634,866 payload bytes in
25.05 h becomes ~1.3 TB/day at 100× (derived) against the ~1.5 TiB free that
D-001 budgeted for 30-60 GB/day, so retention/compaction policy breaks
first, and the background `zstd` CLI compactor
(`crates/feed/src/sink.rs:6-7`) starts competing for cores. (2) The
inline-processing coupling: the WS read task appends raw and parses in-line
(`conn.rs:546-557`), so any sink stall — rotation
(`sink.rs:36-39`), filesystem hiccup — backs up into the kernel socket
buffer and, if long enough, gets the venue to drop the connection; at 100×
the buffer drains 100× less slack. (3) REST snapshot bursts, which are
deliberately DOM-parsed (`binance.rs:654-720`, Q22). RSS itself stays flat
because everything is streaming with reused buffers — the 44 MB ceiling
(ops/soak-report.md) is state, not queued data — so the fix at 100× is
operational (dedicated disk path, decoupled sink thread), not
architectural.

### 12. tokio::broadcast lost 25% of events in your own benchmark. Isn't your comparison rigged against it?

No — the loss is tokio::broadcast behaving exactly as documented under an
unthrottled publisher, and the table prints it rather than smoothing it: at
1 subscriber it published 33.06 M/s but delivered 24.65 M/s, dropping
1,249,997 events (25.0%) via its lagged-receiver overwrite semantics
(`bus_fanout.json`). Our ring has the same *semantic family* — overwrite
the oldest, tell the consumer `Lagged { lost }`
(`crates/bus/src/ring.rs:149-154`) — it just didn't outrun its consumers at
its own 11.47 M/s publish rate; crossbeam-channel is the odd one out
because it blocks the producer instead (backpressure, never loss), and the
producer notes flag all three semantics as different BY DESIGN. The
apples-to-apples comparison is the paced-latency phase (identical 500 k/s
absolute schedule, 0 lost everywhere): ring p50 125 ns at 1 sub and 416 ns
at 8 subs vs tokio's 2.08 µs and 3.00 µs, with the stated cost that ring
consumers spin-poll and burn a core each. D-009 picked broadcast-ring
semantics because market data wants every-consumer-sees-everything,
loss-with-knowledge, and a producer that never blocks; the benchmark then
earned the transport its place rather than assuming it.

---

## Theme 3 — Design trade-offs

### 13. Why no io_uring, no kernel bypass, no core pinning? That's table stakes for HFT.

Because the machine is a Mac: no io_uring, no isolcpus/nohz_full, no IRQ
steering, no DPDK/ef_vi, and no public thread-affinity API — stated up
front in LIMITATIONS.md ("Platform") and restated in BENCHMARKS.md's
methodology, so every number is "well-written userspace on a laptop", not
"colo-tuned box". The design keeps the port cheap: transport is isolated
behind the connection layer (D-002), the bus is pure userspace atomics
(`crates/bus/src/ring.rs`), and the store is mmap + sequential decode, none
of which is macOS-shaped. On Linux the expected wins are specific: pinned
cores and nohz would attack exactly the p999/max scheduler tails dissected
in Q7; io_uring or bypass would cut the syscall/wakeup cost that loopback
TCP measured at p50 17.42 µs (`e2e_net.json`, explicitly labeled a floor,
not a NIC). What would *not* change: the venue floors — Coinbase batches
~50 ms, Binance diffs at 100 ms cadence, and WAN RTT p50s are 38.01 ms /
103.99 ms / 245.66 ms (`e2e_rtt.json`) — so kernel-bypass engineering
against public crypto WebSocket feeds optimizes the wrong four orders of
magnitude. The honest framing: this codebase demonstrates the software
layer; the platform tuning is documented future work on a VPS/Linux box,
not an omission that invalidates the p50s.

### 14. A single global 1e-8 fixed-point scale is inflexible. Why not per-instrument scaling or decimals?

One global scale (`SCALE = 100_000_000`,
`crates/proto/src/fixed.rs:8-11`) buys three hot-path properties: any two
mantissas compare and add with no rescaling, book levels sort as plain
`i64`, and the store delta-encodes raw i64 streams without per-column scale
headers (D-003, exploited at `crates/store/src/block.rs:115-116`). The
range does the job for the chosen universe: i64 at 1e-8 covers prices to
~$92e9, and every subscribed pair's venue precision is ≤ 1e-8 (Kraken's
pinned table caps at 8 decimals, `crates/feed/src/kraken.rs:38-46`).
The failure mode is a hard error, not silent rounding: a token finer than
1e-8 raises `ParseError::PrecisionLoss` (`fixed.rs:22-25`) and instrument
metadata is validated at subscribe time, so a 1e-10-tick meme pair is
*rejected*, documented in LIMITATIONS.md, rather than mangled. f64 was
banned outright (non-exact equality breaks level identity and the CRC
oracle), and decimal crates survive as test oracles only, being 2× wider
and slower in the hot loop (D-003). Per-instrument scale is strictly more
general and strictly more complexity everywhere mantissas meet; it's the
recorded revisit path if the universe ever widens.

### 15. Why hand-roll a 64-byte POD event instead of rkyv/FlatBuffers/Cap'n Proto?

Because the payload is a homogeneous stream of fixed-size records — no
variable-length fields, no nesting, no schema evolution requirement — and
for that shape zero-copy is a pointer cast, not a framework
(D-004). The event is `#[repr(C)]`, exactly 64 bytes (one cache line), Pod/
Zeroable, with compile-time size/alignment asserts
(`crates/proto/src/event.rs:126-160`); reading from mmap is a checked
bytemuck cast, and even snapshots are encoded as bracketed runs of the same
record (`SnapBegin`/`SnapBid`/`SnapAsk`/`SnapEnd`, `event.rs:1-6`) so every
consumer — store, bus, replay — handles exactly one record shape. rkyv
earns its complexity on object graphs; here its archived types would
actively obstruct the columnar store, which wants to shred plain fields
into per-column streams (`crates/store/src/block.rs:110-121`). FlatBuffers/
Cap'n Proto add a codegen toolchain and vtable indirection on every field
read in the hottest loop, for schema-evolution machinery a versioned
`BLOCK_VERSION`/`rsvd` byte already covers more cheaply. The measured
consequence of the flat layout: 40.19 M events/s raw encode and 27.64 M/s
full-decode scan (`store_write.json`, `store_scan.json`).

### 16. Why FNV digests when you use SHA-256 elsewhere? Pick a lane.

Different jobs, stated in LIMITATIONS.md: the replay digests exist to
detect *divergence between two runs of the same code on the same input*,
not to resist an adversary, so FNV-1a folded over every event's 64 bytes
in the hot loop (`crates/replay/src/run.rs:78-88`, `:209`) is the right
cost — it runs 226M times per verification pass. There is no attacker in
the threat model who controls one replay but not the other; an accidental
divergence flips the 64-bit digest with overwhelming probability, and it is
double-checked structurally (`--twice` asserts equality,
`replay-verify.rs:110`). Where the claim is byte-identity of artifacts, the
check is *stronger* than any hash: the ingest test asserts whole-file byte
equality of store files and sidecars across runs
(`crates/replay/src/bin/ingest.rs:402-409`), with a note that it's
externally verifiable via `shasum` (`ingest.rs:18`). SHA-256 appears where
provenance is the point: BENCHMARKS.md's footer pins the sha256 of the
exact result-file bytes the tables were rendered from. Cryptographic
hashing inside the per-event replay loop would be paying for a property —
adversarial collision resistance — that the design explicitly does not
claim.

### 17. Why does the Kraken checksum function live in `proto` and not `feed`? Smells like layering laziness.

The opposite — it's the most deliberate placement in the tree (D-006).
`kraken_book_crc32` is pure format math (mantissas + precisions → CRC32),
so putting it in `proto` lets both `crates/lob` and `crates/replay` verify
books with zero transport dependency; the feed-only alternative would force
a `lob → feed` edge, inverting the layering, and duplicating it was
rejected because two implementations of a correctness oracle is how oracles
rot. The same discipline runs through the store: `pit.rs` deliberately
ships no `pit_book()` convenience because that would drag `flashbook-store`
into depending on `flashbook-lob` — the fold lives above, in bench/replay
(`crates/store/src/pit.rs:36-40`). What stays venue-flavored in `feed` is
exactly the venue-coupled part: the pinned pair-precision table
(`crates/feed/src/kraken.rs:38-46`), which replay passes *into* the oracle
(`crates/replay/src/run.rs:258-268`). The payoff is that the strongest
correctness check in the system runs in three different crates against one
implementation validated once (2962 live checksums, then 41,692,848 at 0
mismatches, ops/soak-report.md).

### 18. You preach zero-allocation hot paths, then run the whole feed on tokio + tokio-tungstenite. Explain.

Measure where the budget goes: at soak rates the transport is idle-loop
noise and the hard requirement is 24 h of zero-crash operation with TLS,
reconnects, keepalive, and per-venue resubscribe — which is what the most
battle-tested async WS stack buys (D-002). The zero-alloc discipline
applies where the cycles are: the codec fast paths (0 allocs/frame measured
for Kraken over 10k frames, `feed_alloc.json`) and the bus publish
(`ring.rs:90-111`, never blocks, never allocates), both of which are
transport-independent. The e2e decomposition confirms the split: parse p50
209 ns and publish p50 125 ns against venue-path/WAN context measured in
tens of milliseconds (`e2e_live.json`, `e2e_rtt.json`) — replacing
tungstenite could not move the total by anything visible. The transport
earned its keep operationally: 34 reconnects auto-recovered across venues
with 0 crashes and 0 parse errors over 25.05 h (ops/soak-report.md).
D-002 also records the tripwire honestly: if the 3e decomposition had shown
WS read dominating, the plan was to bench `fastwebsockets` head-to-head and
publish the numbers — it didn't, so no speculative rewrite.

---

## Theme 4 — Honesty & methodology

### 19. Is the serde_json baseline fair, or a strawman you built to beat?

It is the production fallback path, verbatim: `slow` in the table is
`parse_slow` on every frame, and `fast` *includes* its real serde_json
fallback on structure errors — the producer notes in BENCHMARKS.md say
exactly this, and the fallback wiring is in the capture path
(`crates/feed/src/conn.rs:241-247`) and replay
(`crates/replay/src/run.rs:144-168`). Both paths emit the same normalized
events through the same `finish_depth`/`finish_trade` state machines
(`binance.rs:150-213`, `:215-268` shared by `fast_depth` at `:429` and
`slow_inner` at `:567`), and the harness asserts fast/slow event counts
equal per venue over the full corpus before quoting anything — a
differential guarantee, not a vibe. The 4.91× aggregate gap
(3.78 M/s vs 769.5 k/s, `feed_parse.json`) is fundamentally borrowed vs
owned: the fast path scans payload bytes in place with precomputed memmem
finders and borrowing cursors (`binance.rs:61`, `:95-106`), while
`parse_slow` builds an owned `serde_json::Value` DOM
(`binance.rs:499`) — 80.96 allocs and 5,391 B per frame on Coinbase
(`feed_alloc.json`). Fair pushback: a typed `#[derive(Deserialize)]` parse
would beat the Value DOM; but the fallback is Value-based deliberately,
because its job is salvaging frames whose shape the fast path rejected.
The published claim is "fast path vs this system's real fallback", stated
as such — not "fastest possible serde_json".

### 20. Your own harness caught an i64 overflow in sum(qty) and DuckDB doing float division. Why should I trust the rest of your numbers?

Invert the burden: those two catches are the parity oracle *working*, and
they're documented at the crime scene (`crates/bench/src/compare.rs:59-70`).
`sum(qty)` over 226M events reached ~1.06e19, past `i64::MAX` — DuckDB
refuses the down-cast and SQLite raises — so the sum is split into two
never-overflowing BIGINT sums recombined exactly as i128
(`compare.rs:71-74`, `:304-313`); the split uses bit ops precisely because
DuckDB's integer `/` is Postgres-style float division, which silently
rounded the hi terms and was caught by the parity assertion on the first
full-corpus run. That is the methodology: every backend must return
*identical* results before any timing is quoted — the official run records
`full_scan_equal=true, pit_tops_equal=true, divergences 0, failures []`
(`store_compare.json`). A benchmark harness with no differential checks
would have published a wrong sum at full speed and nobody would ever have
known; this one converts silent corruption into a loud pre-publication
failure. The same pattern guards everything else quoted here: fast/slow
parse equality (Q19), cross-representation book digests
(`digests_match = true`, `lob_replay.json`), and 41.7M venue CRCs. Trust
the numbers *because* the harness has visibly caught bugs — including its
own.

### 21. Your paced-latency benchmarks — did you handle coordinated omission or just measure when convenient?

Handled by construction, with the residual stated: the pacer runs an
absolute schedule — message *i* is sent at `start + i/rate`, spin-waited —
so a stall doesn't silently shift the schedule and hide the backlog
(producer notes in BENCHMARKS.md 3d for both the in-process and loopback
phases; LIMITATIONS.md says this "bounds but does not eliminate"
coordinated-omission effects). When the schedule *did* slip, the report
says so instead of lying: the loopback run is headlined "**Sustained: NO**
— the ACHIEVED rate 89.6 k/s is what the latencies below were measured at,
not the 200.0 k/s target" (`e2e_net.json`) — achieved rate is always
published next to target. Blocked-producer semantics are disclosed too: a
full crossbeam channel makes the paced producer catch up in a burst
afterwards, noted verbatim in the producer notes. Percentiles are
nearest-rank over raw sample arrays, never interpolated or fitted
(`crates/bench/src/percentile.rs:3`, `:51`, D-011), warmups are counted and
discarded, and small-n saturation at max is labeled (the RTT table says
its p999 = max because n = 59 by design). The honest gap that remains:
sampling is stride-based with a 2M cap per subscriber — deterministic and
disclosed, but a cap is a cap.

### 22. "Zero allocations per frame" — except your own table shows Binance at 3.011 allocs/frame. Which is it?

The claim has boundaries and the table prints them instead of rounding them
away (`feed_alloc.json`). Measured with dhat over fresh codecs: Kraken is
*exactly* 0 allocs / 0 bytes over 10,000 frames — the pure WS fast path;
Coinbase shows 2 blocks / 192 B over 10,000 frames (0.0002/frame), which is
one-time per-codec state growth landing inside the measurement window
because the window deliberately uses a fresh codec rather than a pre-warmed
one. Binance's 3.011/frame is dominated by `parse_rest_snapshot`, which
parses REST depth bodies via `serde_json::Value` on BOTH paths *by design*
(`crates/feed/src/binance.rs:654-720`; the run's 5 REST snaps are reported
per-pass precisely so this is attributable). That's a deliberate trade:
REST snapshots are rare (561 in 25 h against 55.8M WS frames,
ops/soak-report.md — about one in 100,000 records) and correctness-critical
during resync, so DOM-parse robustness wins there while the per-frame WS
path stays allocation-free (`binance.rs:61`). So the precise claim is:
steady-state WS fast path targets and (for Kraken) measurably achieves
zero; one-time lazy init and snapshot handling allocate, are measured,
and are attributed — the producer notes say "the numbers here are the real
measured deltas, whatever they are".

### 23. Your PIT query "beats" SQL on correctness — isn't that just your harness declaring itself right?

No — it's a documented semantic difference with the disagreement *counted
and published*, and the SQL engines were given our answer to keep the race
fair. The naive SQL anchor — `max(recv_mono) WHERE kind = 4 AND recv_mono
<= t` (`crates/bench/src/compare.rs:404-406`) — can legitimately select a
dangling `SnapBegin` whose `SnapEnd` never arrived, i.e. fold a
half-written book; `SnapshotIndex` only indexes *complete* brackets with no
intervening `Clear` or restarted `SnapBegin` for that instrument
(`crates/store/src/pit.rs:8-15`). Rather than let that skew either
correctness or timing, the SQL PIT functions fold from ours' validated
anchor and separately report what the naive anchor said and whether it
diverged (`compare.rs:30-37`, `SqlPit::anchor_diverged` at
`compare.rs:117-121`). On the official 200-query run: anchor hits 186/200,
divergences 0, `pit_tops_equal=true` (`store_compare.json`) — so on this
corpus the naive anchor happened to agree, and the table still gives DuckDB
the p50 win (43.93 ms vs 119.44 ms). The claim, precisely: our anchor
semantics are provably safe against incomplete snapshots and the harness
was built so that if SQL's semantics ever differ, the number is published,
not exploited.

---

## Theme 5 — Ops

### 24. Your 24h-continuous gate reads NOT MET. Why would you publish a failed gate in your own showcase?

Because the report is generated from committed telemetry and the honesty
rules are mechanical, not aspirational (D-011; `ops/gen-soak-report.py`
renders `ops/soak/stats.jsonl`). The truthful decomposition: the capture
*process* ran one session for a 25.05 h span with **0 restarts, 0 crashes,
0 gaps, 0 parse errors, 55,819,963 msgs** — but the *machine* slept 18
times (stats-cadence holes > 2 min, worst 7,287 s), so the longest
continuous hole-free window is 11.2 h and the gate as specified reads
NOT MET (ops/soak-report.md, "Goal gates"). `caffeinate` only holds sleep
on AC power, and the laptop was unplugged/closed at times — outside
software control, and the reconnect machinery recovered every hole with
fresh snapshots (34 reconnects total; LIMITATIONS.md, "Soak reality").
Deleting the gate, redefining it post-hoc, or summing windows into a fake
"24h" would each be exactly the number-laundering this project's rules
exist to prevent; the fix is stated instead: rerun on a VPS, pending
access. A reviewer should read a published NOT MET sitting next to the
gates and benchmark wins that did land as calibration — it's what makes
the wins believable.

### 25. Your e2e benchmark shells out to `openssl s_client` for TLS. Seriously?

Seriously, and it's load-bearing for hermeticity rather than a hack: the
bench crate deliberately carries no TLS/WS dependency, so live mode
terminates TLS in an `openssl s_client -quiet -ign_eof` child and speaks a
minimal hand-rolled RFC 6455 client over its pipes
(`crates/bench/src/bin/bench-e2e.rs:50-54`, `:426`, spawn at `:665-681`).
The measured decomposition is untouched by this choice because it starts at
`t0` = frame-fully-read: the extra pipe hop sits *before* t0 and inflates
only the unmeasured receive path, while parse/publish/deliver/total-added
all begin at t0 — stated in the file, in the result's producer notes, and
in LIMITATIONS.md. The one number it does touch is disclosed where it's
quoted: WS ping/pong RTT includes the pipe hops in both directions, "adds
microseconds against millisecond WANs" (p50s 38.01-245.66 ms,
`e2e_rtt.json`). The production capture path shares none of this — it runs
tokio-tungstenite over rustls end-to-end (D-002) and is the path the 25 h
soak and all parse benchmarks exercise. Bench-only transport, pre-t0,
disclosed three times: the decomposition stands.

---

*Answers verified against commit-time line numbers; regenerate line refs when code moves.*
