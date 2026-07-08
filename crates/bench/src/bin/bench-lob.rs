//! bench-lob: LOB replay benchmark — LadderBook vs BTreeBook on real
//! captured data (Phase 3b).
//!
//! Three phases, all **single-threaded** (one core applies the whole event
//! stream; no pipeline overlap — this isolates pure book-apply cost):
//!
//! 1. **Parse (uncounted):** one `replay_books` pass over the raw capture
//!    collecting every normalized [`Event`] into a `Vec`. Codec/parse cost
//!    is thus paid once, outside every timed region.
//! 2. **Throughput, per representation:** one warmup pass then N measured
//!    passes (5, or 2 with `--quick`) applying the pre-parsed events into a
//!    fresh venue-routed pair of [`BookSet`]s. Reported as events/s per
//!    pass; `ws_frames` from the replay outcome is recorded alongside so a
//!    msgs/s-equivalent can be derived honestly (msgs/s = ws_frames /
//!    pass_seconds — frames, not events, are what a feed handler receives).
//!    The final combined book digest is asserted identical across passes
//!    AND across representations (the cross-implementation guarantee,
//!    verified here on real data, not just property tests).
//! 3. **Top-of-book latency:** one pass per representation timing EACH
//!    apply with an `Instant` pair, keeping samples only for applies that
//!    returned `Mutated { top_changed: true }`. The per-op cost of an empty
//!    `Instant::now()`/`elapsed()` pair is measured and published as
//!    `timer_overhead_ns` — every latency sample includes roughly that much
//!    measurement overhead, so sub-100ns numbers must be read with it in
//!    mind.
//!
//! Usage: `bench-lob --data <raw dir> [--quick] [--results-dir DIR]
//!         [--kraken-depth N] [--overwrite]`
//!
//! Writes `<results-dir>/lob_replay.json` via [`flashbook_bench::results`].
//! Exit codes: 0 ok, 1 digest mismatch (correctness failure), 2 usage/IO.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use flashbook_bench::{Percentiles, ResultFile, write_result};
use flashbook_lob::{Apply, BTreeBook, BookSet, L2Book, LadderBook};
use flashbook_proto::event::Venue;
use flashbook_proto::{Event, Registry};
use flashbook_replay::replay_books;

/// Measured passes when running full vs `--quick`.
const PASSES_FULL: usize = 5;
/// Measured passes under `--quick` (smoke runs on a busy machine).
const PASSES_QUICK: usize = 2;
/// Iterations used to calibrate the `Instant` pair overhead.
const TIMER_CALIBRATION_ITERS: u64 = 1_000_000;

/// Parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    /// Raw capture directory (`replay_books` root).
    data: PathBuf,
    /// Fewer measured passes; marks the result file as non-official.
    quick: bool,
    /// Where `lob_replay.json` is written.
    results_dir: PathBuf,
    /// Kraken venue depth cap (100 for smoke/soak captures).
    kraken_depth: usize,
    /// Allow clobbering an existing result file.
    overwrite: bool,
}

/// Parse CLI args (everything after argv[0]). Pure; returns a usage error
/// string on bad input.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Config, String> {
    let mut data: Option<PathBuf> = None;
    let mut quick = false;
    let mut results_dir = PathBuf::from("bench/results");
    let mut kraken_depth: usize = 100;
    let mut overwrite = false;

    let mut args = args.peekable();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data" => data = args.next().map(PathBuf::from),
            "--quick" => quick = true,
            "--results-dir" => {
                results_dir = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or("--results-dir needs a path")?;
            }
            "--kraken-depth" => {
                kraken_depth = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or("--kraken-depth needs an integer")?;
            }
            "--overwrite" => overwrite = true,
            "--help" | "-h" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown arg: {other}\n{USAGE}")),
        }
    }
    let data = data.ok_or_else(|| format!("--data <dir> required\n{USAGE}"))?;
    Ok(Config {
        data,
        quick,
        results_dir,
        kraken_depth,
        overwrite,
    })
}

const USAGE: &str = "usage: bench-lob --data <raw dir> [--quick] [--results-dir DIR] [--kraken-depth N] [--overwrite]";

/// Measured passes for the throughput phase.
fn measured_passes(quick: bool) -> usize {
    if quick { PASSES_QUICK } else { PASSES_FULL }
}

/// Arithmetic mean of `xs` (0.0 for empty — callers guard non-empty).
fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        0.0
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

/// Winner by higher mean throughput. Reported only — no code path changes
/// based on it in this phase.
fn winner_name(ladder_mean_eps: f64, btree_mean_eps: f64) -> &'static str {
    if ladder_mean_eps >= btree_mean_eps {
        "ladder"
    } else {
        "btree"
    }
}

/// Fresh venue-routed book pair mirroring `replay_books`: Kraken events go
/// to a depth-capped set, everything else to an unlimited set.
struct RoutedBooks<B: L2Book> {
    other: BookSet<B>,
    kraken: BookSet<B>,
}

impl<B: L2Book> RoutedBooks<B> {
    fn new(make: fn(Option<usize>) -> B, kraken_depth: Option<usize>) -> Self {
        Self {
            other: BookSet::new(None, make),
            kraken: BookSet::new(kraken_depth, make),
        }
    }

    #[inline]
    fn apply(&mut self, ev: &Event) -> Apply {
        if ev.venue == Venue::Kraken as u8 {
            self.kraken.apply(ev)
        } else {
            self.other.apply(ev)
        }
    }

    /// Combined digest, same fold as `replay_books` (`books ^
    /// kraken.rotate_left(1)`) so it is directly comparable to the parse
    /// pass's `books_digest`.
    fn combined_digest(&self) -> u64 {
        self.other.combined_digest() ^ self.kraken.combined_digest().rotate_left(1)
    }
}

/// Apply every event once into fresh books; return the final combined
/// digest. This is the body of one throughput pass.
fn apply_all<B: L2Book>(
    events: &[Event],
    make: fn(Option<usize>) -> B,
    kraken_depth: Option<usize>,
) -> u64 {
    let mut books = RoutedBooks::new(make, kraken_depth);
    for ev in events {
        std::hint::black_box(books.apply(ev));
    }
    books.combined_digest()
}

/// One warmup + `passes` measured passes. Returns (pass seconds, final
/// digest). Errors if any pass's digest differs from the warmup's.
fn throughput_passes<B: L2Book>(
    events: &[Event],
    make: fn(Option<usize>) -> B,
    kraken_depth: Option<usize>,
    passes: usize,
) -> Result<(Vec<f64>, u64), String> {
    let digest = apply_all(events, make, kraken_depth); // warmup, untimed
    let mut secs = Vec::with_capacity(passes);
    for p in 0..passes {
        let t0 = Instant::now();
        let d = apply_all(events, make, kraken_depth);
        secs.push(t0.elapsed().as_secs_f64());
        if d != digest {
            return Err(format!(
                "digest mismatch across passes: warmup {digest:016x} vs pass {p} {d:016x}"
            ));
        }
    }
    Ok((secs, digest))
}

/// One instrumented pass: per-apply latency samples (ns), kept only for
/// applies that changed the top of book (`Mutated {{ top_changed: true }}`).
/// Every sample includes the cost of one `Instant` pair (see
/// `timer_overhead_ns` in the result file).
fn tob_latency_samples<B: L2Book>(
    events: &[Event],
    make: fn(Option<usize>) -> B,
    kraken_depth: Option<usize>,
) -> Vec<u64> {
    let mut books = RoutedBooks::new(make, kraken_depth);
    let mut samples = Vec::with_capacity(events.len() / 4);
    for ev in events {
        let t0 = Instant::now();
        let apply = books.apply(ev);
        let dt = t0.elapsed();
        if matches!(apply, Apply::Mutated { top_changed: true }) {
            samples.push(u64::try_from(dt.as_nanos()).unwrap_or(u64::MAX));
        }
    }
    samples
}

/// Measure the mean per-iteration cost (ns) of an empty
/// `Instant::now()`/`elapsed()` pair — the floor baked into every latency
/// sample above.
fn timer_overhead_ns(iters: u64) -> f64 {
    let mut sink = 0u64;
    let t0 = Instant::now();
    for _ in 0..iters {
        let t = Instant::now();
        sink = sink.wrapping_add(u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    let total = t0.elapsed().as_secs_f64() * 1e9;
    std::hint::black_box(sink);
    total / iters as f64
}

/// LadderBook constructor for [`BookSet`].
fn make_ladder(depth: Option<usize>) -> LadderBook {
    depth.map_or_else(LadderBook::new, LadderBook::with_max_depth)
}

/// BTreeBook constructor for [`BookSet`].
fn make_btree(depth: Option<usize>) -> BTreeBook {
    depth.map_or_else(BTreeBook::new, BTreeBook::with_max_depth)
}

/// Percentiles as JSON (`null` when there were no samples).
fn percentiles_json(samples: &[u64]) -> serde_json::Value {
    Percentiles::from_samples(samples)
        .map(|p| serde_json::to_value(p).expect("percentiles serialize"))
        .unwrap_or(serde_json::Value::Null)
}

fn main() -> ExitCode {
    let cfg = match parse_args(std::env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let kdepth = Some(cfg.kraken_depth);
    let passes = measured_passes(cfg.quick);

    // Phase 1: parse (uncounted) — one replay collecting every event.
    let registry = Registry::builtin();
    let mut events: Vec<Event> = Vec::new();
    let outcome =
        match replay_books::<LadderBook>(&cfg.data, &registry, make_ladder, kdepth, |ev| {
            events.push(*ev);
        }) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("replay failed: {e}");
                return ExitCode::from(2);
            }
        };
    if events.is_empty() {
        eprintln!("no events in corpus at {}", cfg.data.display());
        return ExitCode::from(2);
    }
    let span_s =
        outcome.last_mono_ns.saturating_sub(outcome.first_mono_ns) as f64 / 1_000_000_000.0;
    eprintln!(
        "corpus: {} events from {} ws frames ({} records, {:.1}s span), parse_errors={}, \
         checksums ok/mismatch={}/{}",
        outcome.events,
        outcome.ws_frames,
        outcome.records,
        span_s,
        outcome.parse_errors,
        outcome.checksums_ok,
        outcome.checksum_mismatches,
    );

    // Phase 2: throughput per representation.
    let (ladder_secs, ladder_digest) = match throughput_passes(&events, make_ladder, kdepth, passes)
    {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ladder: {e}");
            return ExitCode::from(1);
        }
    };
    let (btree_secs, btree_digest) = match throughput_passes(&events, make_btree, kdepth, passes) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("btree: {e}");
            return ExitCode::from(1);
        }
    };

    // Cross-representation + cross-phase digest guarantee, on real data.
    let digests_match = ladder_digest == btree_digest && ladder_digest == outcome.books_digest;
    if !digests_match {
        eprintln!(
            "DIGEST MISMATCH: ladder {ladder_digest:016x}, btree {btree_digest:016x}, \
             parse pass {:016x}",
            outcome.books_digest
        );
    }

    let eps = |secs: &[f64]| -> Vec<f64> { secs.iter().map(|s| events.len() as f64 / s).collect() };
    let ladder_eps = eps(&ladder_secs);
    let btree_eps = eps(&btree_secs);
    let (ladder_mean, btree_mean) = (mean(&ladder_eps), mean(&btree_eps));
    let winner = winner_name(ladder_mean, btree_mean);
    eprintln!(
        "throughput (events/s, {passes} passes): ladder mean {ladder_mean:.0}, \
         btree mean {btree_mean:.0} -> winner: {winner}"
    );

    // Phase 3: top-of-book apply latency + timer calibration.
    let overhead_ns = timer_overhead_ns(TIMER_CALIBRATION_ITERS);
    let ladder_tob = tob_latency_samples(&events, make_ladder, kdepth);
    let btree_tob = tob_latency_samples(&events, make_btree, kdepth);
    eprintln!(
        "tob latency samples: ladder n={}, btree n={}, timer overhead ~{overhead_ns:.1}ns/pair",
        ladder_tob.len(),
        btree_tob.len()
    );

    let config = serde_json::json!({
        "data": cfg.data.display().to_string(),
        "quick": cfg.quick,
        "kraken_depth": cfg.kraken_depth,
        "events": outcome.events,
        "ws_frames": outcome.ws_frames,
        "records": outcome.records,
        "raw_payload_bytes": outcome.raw_payload_bytes,
        "span_mono_s": span_s,
        "parse_errors": outcome.parse_errors,
        "checksums_ok": outcome.checksums_ok,
        "checksum_mismatches": outcome.checksum_mismatches,
        "warmup_passes": 1,
        "measured_passes": passes,
        "threads": 1,
    });
    let metrics = serde_json::json!({
        "ladder": {
            "pass_seconds": ladder_secs,
            "events_per_s": ladder_eps,
            "mean_events_per_s": ladder_mean,
        },
        "btree": {
            "pass_seconds": btree_secs,
            "events_per_s": btree_eps,
            "mean_events_per_s": btree_mean,
        },
        "tob_latency": {
            "ladder": percentiles_json(&ladder_tob),
            "btree": percentiles_json(&btree_tob),
        },
        "timer_overhead_ns": overhead_ns,
        "digests_match": digests_match,
        "winner": winner,
    });
    let notes = format!(
        "Single-threaded book-apply benchmark over pre-parsed events (parse cost excluded). \
         msgs/s-equivalent derives as ws_frames / pass_seconds. Latency samples each include \
         one Instant::now()/elapsed() pair (~{overhead_ns:.1}ns measured on this run); only \
         applies returning Mutated{{top_changed:true}} are kept. Winner is reported, not acted \
         on (representation choice is a later phase).{}",
        if cfg.quick {
            " QUICK RUN: reduced passes on a possibly busy machine; numbers are NOT official."
        } else {
            ""
        }
    );

    let result = ResultFile::new("lob_replay", config, metrics, &notes);
    match write_result(&cfg.results_dir, &result, cfg.overwrite) {
        Ok(path) => println!("{}", path.display()),
        Err(e) => {
            eprintln!("writing result failed: {e}");
            return ExitCode::from(2);
        }
    }

    if digests_match {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flashbook_proto::event::EventKind;

    fn ev(kind: EventKind, venue: Venue, instrument: u32, price: i64, qty: i64) -> Event {
        Event {
            recv_mono_ns: 0,
            recv_wall_ns: 0,
            venue_ts_ns: 0,
            venue_seq: 0,
            price,
            qty,
            aux: 0,
            instrument,
            kind: kind as u8,
            venue: venue as u8,
            flags: 0,
            rsvd: 0,
        }
    }

    /// A tiny two-venue stream: snapshot then deltas, exercising both the
    /// kraken-routed and default book sets.
    fn synthetic_events() -> Vec<Event> {
        use EventKind::*;
        vec![
            ev(SnapBegin, Venue::Coinbase, 1, 0, 0),
            ev(SnapBid, Venue::Coinbase, 1, 100_00000000, 5_00000000),
            ev(SnapAsk, Venue::Coinbase, 1, 101_00000000, 5_00000000),
            ev(SnapEnd, Venue::Coinbase, 1, 0, 0),
            // best-bid qty change -> top_changed
            ev(BidSet, Venue::Coinbase, 1, 100_00000000, 7_00000000),
            // deeper level -> not top_changed
            ev(BidSet, Venue::Coinbase, 1, 99_00000000, 3_00000000),
            // remove best ask -> top_changed
            ev(AskSet, Venue::Coinbase, 1, 101_00000000, 0),
            // kraken instrument routed to the capped set
            ev(SnapBegin, Venue::Kraken, 2, 0, 0),
            ev(SnapBid, Venue::Kraken, 2, 50_00000000, 1_00000000),
            ev(SnapEnd, Venue::Kraken, 2, 0, 0),
        ]
    }

    #[test]
    fn parse_args_full_defaults_and_errors() {
        let full = parse_args(
            [
                "--data",
                "data/smoke",
                "--quick",
                "--results-dir",
                "bench/results/tmp",
                "--kraken-depth",
                "25",
                "--overwrite",
            ]
            .iter()
            .map(ToString::to_string),
        )
        .unwrap();
        assert_eq!(
            full,
            Config {
                data: PathBuf::from("data/smoke"),
                quick: true,
                results_dir: PathBuf::from("bench/results/tmp"),
                kraken_depth: 25,
                overwrite: true,
            }
        );

        let defaults = parse_args(["--data", "d"].iter().map(ToString::to_string)).unwrap();
        assert!(!defaults.quick);
        assert!(!defaults.overwrite);
        assert_eq!(defaults.kraken_depth, 100);
        assert_eq!(defaults.results_dir, PathBuf::from("bench/results"));

        assert!(parse_args(std::iter::empty()).is_err(), "--data required");
        assert!(
            parse_args(["--bogus".to_string()].into_iter()).is_err(),
            "unknown arg rejected"
        );
        assert!(
            parse_args(
                ["--data", "d", "--kraken-depth", "nope"]
                    .iter()
                    .map(ToString::to_string)
            )
            .is_err(),
            "non-integer depth rejected"
        );
    }

    #[test]
    fn measured_passes_quick_vs_full() {
        assert_eq!(measured_passes(true), 2);
        assert_eq!(measured_passes(false), 5);
    }

    #[test]
    fn mean_and_winner() {
        assert!((mean(&[1.0, 2.0, 3.0]) - 2.0).abs() < 1e-12);
        assert_eq!(mean(&[]), 0.0);
        assert_eq!(winner_name(2.0, 1.0), "ladder");
        assert_eq!(winner_name(1.0, 2.0), "btree");
        assert_eq!(winner_name(1.0, 1.0), "ladder", "ties break to ladder");
    }

    #[test]
    fn apply_all_digest_identical_across_representations_and_repeats() {
        let events = synthetic_events();
        let a = apply_all(&events, make_ladder, Some(100));
        let b = apply_all(&events, make_ladder, Some(100));
        let c = apply_all(&events, make_btree, Some(100));
        assert_eq!(a, b, "same representation must be deterministic");
        assert_eq!(a, c, "ladder and btree must agree");
        assert_ne!(a, 0);
    }

    #[test]
    fn throughput_passes_report_and_agree() {
        let events = synthetic_events();
        let (secs, digest) = throughput_passes(&events, make_ladder, Some(100), 2).unwrap();
        assert_eq!(secs.len(), 2);
        assert!(secs.iter().all(|&s| s > 0.0));
        assert_eq!(digest, apply_all(&events, make_btree, Some(100)));
    }

    #[test]
    fn tob_samples_only_for_top_changes_and_match_across_impls() {
        let events = synthetic_events();
        let ladder = tob_latency_samples(&events, make_ladder, Some(100));
        let btree = tob_latency_samples(&events, make_btree, Some(100));
        // snapshot levels (3) + best-bid qty change + best-ask removal = 5;
        // the deeper BidSet and the SnapBegin/SnapEnd brackets don't count.
        assert_eq!(ladder.len(), 5);
        assert_eq!(
            ladder.len(),
            btree.len(),
            "top-changed classification must agree across impls"
        );
    }

    #[test]
    fn timer_overhead_is_sane() {
        let ns = timer_overhead_ns(10_000);
        assert!(ns > 0.0 && ns < 10_000.0, "instant pair ~ns..us, got {ns}");
    }

    #[test]
    fn percentiles_json_null_on_empty() {
        assert_eq!(percentiles_json(&[]), serde_json::Value::Null);
        let v = percentiles_json(&[1, 2, 3]);
        assert_eq!(v["n"], 3);
    }
}
