//! bench-feed: JSON -> Event normalization throughput (BENCHMARKS.md 3a).
//!
//! Replays captured raw WS frames through each venue codec twice: once via
//! the production fast path (`parse`, including its real `parse_slow`
//! fallback on `Structure` errors) and once via the pure serde_json slow
//! path (`parse_slow` for every frame — the published "naive baseline").
//! REST snapshot records are applied identically on both paths so the
//! Binance codec exercises its synced emission path.
//!
//! Everything is single-threaded on one core: "msgs/sec/core" in the result
//! file means single-thread msgs/s on one P-core equivalent (macOS provides
//! no thread pinning, so the scheduler picks the core).
//!
//! Differential guarantee: fast and slow event counts over the full loaded
//! corpus are asserted equal per venue; a mismatch aborts the run.
//!
//! Usage:
//!   bench-feed --data <raw dir> [--quick] [--results-dir DIR] [--overwrite]
//!   bench-feed --data <raw dir> --alloc-check [--results-dir DIR] [--overwrite]
//!
//! `--alloc-check` needs the dhat allocator compiled in:
//!   cargo run --release -p flashbook-bench --features alloc-profile \
//!       --bin bench-feed -- --alloc-check --data ... --results-dir ...
//!
//! Writes `feed_parse.json` (throughput) or `feed_alloc.json`
//! (allocations/frame) via [`flashbook_bench::ResultFile`].

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use flashbook_bench::{ResultFile, write_result};
use flashbook_feed::binance::BinanceCodec;
use flashbook_feed::coinbase::CoinbaseCodec;
use flashbook_feed::conn::parse_rest_envelope;
use flashbook_feed::kraken::KrakenCodec;
use flashbook_feed::{CodecError, SymbolTable, VenueCodec};
use flashbook_proto::rawlog::rkind;
use flashbook_proto::{Event, Registry, Venue};
use flashbook_replay::MergedStream;

/// Heap-profiling allocator, only when built with `--features alloc-profile`
/// (dhat needs debug symbols; the release profile keeps `debug = true`).
#[cfg(feature = "alloc-profile")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// WS frames loaded per venue in `--quick` mode.
const QUICK_FRAME_CAP: usize = 100_000;
/// WS frames loaded per venue in full mode.
const FULL_FRAME_CAP: usize = 1_000_000;
/// WS frames per venue for the `--alloc-check` sample.
#[cfg(feature = "alloc-profile")]
const ALLOC_SAMPLE_FRAMES: usize = 10_000;
/// Warmup runs before measurement (both modes).
const WARMUP_RUNS: usize = 1;

/// Parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Args {
    /// Raw capture root (`<root>/<venue>/<segment>` layout). Defaults to
    /// `data/raw` (the soak capture) to match `bench/run-all.sh`, which
    /// invokes every harness without arguments.
    data: PathBuf,
    /// Quick smoke mode: fewer frames, fewer runs. Never official numbers.
    quick: bool,
    /// Where result JSON files land.
    results_dir: PathBuf,
    /// Run the dhat allocation measurement instead of the throughput bench.
    alloc_check: bool,
    /// Allow clobbering an existing result file.
    overwrite: bool,
}

const USAGE: &str = "usage: bench-feed [--data <raw dir>] [--quick] [--alloc-check] \
     [--results-dir DIR] [--overwrite]";

/// Parse outcome: run with these args, or just print usage (exit 0 — the
/// `bench/run-all.sh` probe runs `--help` and skips bins that fail it).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Cli {
    /// Run the benchmark.
    Run(Args),
    /// `--help`/`-h`: print usage, exit success.
    Help,
}

/// Parse CLI arguments (everything after argv[0]).
fn parse_args(args: impl Iterator<Item = String>) -> Result<Cli, String> {
    let mut out = Args {
        data: PathBuf::from("data/raw"),
        quick: false,
        results_dir: PathBuf::from("bench/results"),
        alloc_check: false,
        overwrite: false,
    };
    let mut args = args;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data" => match args.next() {
                Some(v) => out.data = PathBuf::from(v),
                None => return Err("--data needs a directory".into()),
            },
            "--results-dir" => match args.next() {
                Some(v) => out.results_dir = PathBuf::from(v),
                None => return Err("--results-dir needs a directory".into()),
            },
            "--quick" => out.quick = true,
            "--alloc-check" => out.alloc_check = true,
            "--overwrite" => out.overwrite = true,
            "--help" | "-h" => return Ok(Cli::Help),
            other => return Err(format!("unknown arg: {other}\n{USAGE}")),
        }
    }
    Ok(Cli::Run(out))
}

/// WS-frame cap per venue for the throughput bench.
fn frame_cap(quick: bool) -> usize {
    if quick {
        QUICK_FRAME_CAP
    } else {
        FULL_FRAME_CAP
    }
}

/// Measured runs per (venue, path) for the throughput bench.
fn measured_runs(quick: bool) -> usize {
    if quick { 2 } else { 5 }
}

/// Throughput in count/second from a count and an elapsed time in ns.
fn per_sec(count: u64, elapsed_ns: u64) -> f64 {
    if elapsed_ns == 0 {
        return 0.0;
    }
    count as f64 * 1e9 / elapsed_ns as f64
}

/// Nearest-rank median (lower median for even n), consistent with
/// [`flashbook_bench::Percentiles`]' p50. Panics on an empty slice.
fn median_ns(samples: &[u64]) -> u64 {
    assert!(!samples.is_empty(), "median of zero samples");
    let mut s = samples.to_vec();
    s.sort_unstable();
    s[s.len().div_ceil(2) - 1]
}

/// Aggregate throughput over `(count, elapsed_ns)` parts: total count over
/// total time (venues run sequentially, so times add).
fn aggregate_per_sec(parts: &[(u64, u64)]) -> f64 {
    let count: u64 = parts.iter().map(|p| p.0).sum();
    let ns: u64 = parts.iter().map(|p| p.1).sum();
    per_sec(count, ns)
}

/// One raw record kept for replay through the codecs.
struct Rec {
    /// Record kind ([`rkind`]): WS_TEXT / WS_BINARY / REST_SNAPSHOT.
    rkind: u8,
    /// Monotonic receive timestamp from capture.
    mono: u64,
    /// Wall receive timestamp from capture.
    wall: u64,
    /// Verbatim payload bytes.
    payload: Vec<u8>,
}

/// Load per-venue records (indexed `venue as usize - 1`) in deterministic
/// merged order, capped at `cap` WS frames per venue. REST snapshot records
/// stay in-position (the Binance codec needs the anchor to reach its synced
/// emission path); NOTE and unknown records are dropped.
fn load_records(root: &Path, cap: usize) -> Result<[Vec<Rec>; 3]> {
    let mut stream = MergedStream::new(root).context("open capture root")?;
    let mut per: [Vec<Rec>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    let mut ws = [0usize; 3];
    while let Some(r) = stream.next().context("read merged record")? {
        let idx = r.venue as usize - 1;
        if idx >= per.len() {
            continue;
        }
        if ws[idx] >= cap {
            if ws.iter().all(|&c| c >= cap) {
                break;
            }
            continue;
        }
        match r.rkind {
            rkind::WS_TEXT | rkind::WS_BINARY => ws[idx] += 1,
            rkind::REST_SNAPSHOT => {}
            _ => continue,
        }
        per[idx].push(Rec {
            rkind: r.rkind,
            mono: r.recv_mono_ns,
            wall: r.recv_wall_ns,
            payload: r.payload,
        });
    }
    Ok(per)
}

/// Fresh codec for `venue` over the builtin registry's symbol universe.
fn make_codec(venue: Venue, registry: &Registry) -> Box<dyn VenueCodec> {
    let table = SymbolTable::new(
        registry
            .for_venue(venue)
            .map(|m| (m.venue_symbol.clone(), m.id)),
    );
    match venue {
        Venue::Coinbase => Box::new(CoinbaseCodec::new(table)),
        Venue::Binance => Box::new(BinanceCodec::new(table)),
        Venue::Kraken => Box::new(KrakenCodec::new(table)),
    }
}

/// Which parse path a pass exercises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathKind {
    /// Production fast path: `parse`, with the real `parse_slow` fallback
    /// on `Structure` errors (counted).
    Fast,
    /// serde_json reference path: `parse_slow` for every frame.
    Slow,
}

impl PathKind {
    /// Stable key for reports.
    fn name(self) -> &'static str {
        match self {
            PathKind::Fast => "fast",
            PathKind::Slow => "slow",
        }
    }
}

/// Counters from one timed pass over one venue's records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PassResult {
    /// WS frames fed to the codec (the msgs/s denominator).
    ws_frames: u64,
    /// REST snapshot records applied (in run time, not in msgs/s).
    rest_snapshots: u64,
    /// Sum of WS payload bytes (the bytes/s numerator).
    ws_payload_bytes: u64,
    /// Events emitted (the fast==slow differential check).
    events: u64,
    /// Fast-path frames that fell back to `parse_slow` (Fast only).
    fallbacks: u64,
    /// Frames/snapshots no path could parse.
    parse_errors: u64,
    /// Wall time for the whole pass.
    elapsed_ns: u64,
}

/// Run `records` through `codec` on `path`, reusing `out` between frames.
/// Times the whole loop (REST snapshot application included).
fn run_pass(
    codec: &mut dyn VenueCodec,
    records: &[Rec],
    path: PathKind,
    out: &mut Vec<Event>,
) -> PassResult {
    let mut pr = PassResult::default();
    let t0 = Instant::now();
    for rec in records {
        out.clear();
        match rec.rkind {
            rkind::WS_TEXT | rkind::WS_BINARY => {
                pr.ws_frames += 1;
                pr.ws_payload_bytes += rec.payload.len() as u64;
                let res = match path {
                    PathKind::Fast => match codec.parse(&rec.payload, rec.mono, rec.wall, out) {
                        Err(CodecError::Structure(_)) => {
                            out.clear();
                            pr.fallbacks += 1;
                            codec.parse_slow(&rec.payload, rec.mono, rec.wall, out)
                        }
                        r => r,
                    },
                    PathKind::Slow => codec.parse_slow(&rec.payload, rec.mono, rec.wall, out),
                };
                if res.is_err() {
                    out.clear();
                    pr.parse_errors += 1;
                }
            }
            rkind::REST_SNAPSHOT => {
                pr.rest_snapshots += 1;
                match parse_rest_envelope(&rec.payload) {
                    Ok((instrument, body)) => {
                        if codec
                            .parse_rest_snapshot(instrument, body, rec.mono, rec.wall, out)
                            .is_err()
                        {
                            out.clear();
                            pr.parse_errors += 1;
                        }
                    }
                    Err(_) => pr.parse_errors += 1,
                }
            }
            _ => {}
        }
        pr.events += out.len() as u64;
    }
    pr.elapsed_ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
    pr
}

/// One path's measured summary for one venue.
struct PathSummary {
    /// Elapsed ns of each measured run.
    run_ns: Vec<u64>,
    /// Nearest-rank median of `run_ns`.
    median_ns: u64,
    /// Counters (identical across runs — asserted).
    pass: PassResult,
}

/// Warmup + N measured fresh-codec runs of one (venue, path) pair.
/// Asserts every run emits the identical event count (fresh codec + fixed
/// records means each run is fully deterministic).
fn measure_path(
    venue: Venue,
    registry: &Registry,
    records: &[Rec],
    path: PathKind,
    runs: usize,
    out: &mut Vec<Event>,
) -> Result<PathSummary> {
    let mut run_ns = Vec::with_capacity(runs);
    let mut last: Option<PassResult> = None;
    for i in 0..WARMUP_RUNS + runs {
        let mut codec = make_codec(venue, registry);
        let pr = run_pass(codec.as_mut(), records, path, out);
        if let Some(prev) = last {
            ensure!(
                prev.events == pr.events,
                "{} {} path: run event counts diverged ({} vs {}) — nondeterministic parse",
                venue.name(),
                path.name(),
                prev.events,
                pr.events,
            );
        }
        if i >= WARMUP_RUNS {
            run_ns.push(pr.elapsed_ns);
        }
        last = Some(pr);
    }
    let pass = last.expect("at least one run");
    let median_ns = median_ns(&run_ns);
    Ok(PathSummary {
        run_ns,
        median_ns,
        pass,
    })
}

/// JSON block for one path summary.
fn path_json(s: &PathSummary) -> serde_json::Value {
    serde_json::json!({
        "run_ns": s.run_ns,
        "median_run_ns": s.median_ns,
        "msgs_per_s": per_sec(s.pass.ws_frames, s.median_ns),
        "events_per_s": per_sec(s.pass.events, s.median_ns),
        "bytes_per_s": per_sec(s.pass.ws_payload_bytes, s.median_ns),
        "events": s.pass.events,
        "fallbacks": s.pass.fallbacks,
        "parse_errors": s.pass.parse_errors,
    })
}

/// The throughput benchmark: load, measure fast + slow per venue, assert
/// the differential event-count guarantee, write `feed_parse.json`.
fn run_bench(args: &Args) -> Result<()> {
    let cap = frame_cap(args.quick);
    let runs = measured_runs(args.quick);
    let registry = Registry::builtin();
    eprintln!(
        "bench-feed: loading {} (cap {cap} WS frames/venue)...",
        args.data.display()
    );
    let per_venue = load_records(&args.data, cap)?;

    let mut venues_json = Vec::new();
    let mut agg = std::collections::BTreeMap::<&str, Vec<(u64, u64)>>::new();
    let mut agg_events_fast: Vec<(u64, u64)> = Vec::new();
    let mut agg_bytes_fast: Vec<(u64, u64)> = Vec::new();

    for venue in Venue::ALL {
        let records = &per_venue[venue as usize - 1];
        if records.is_empty() {
            eprintln!("bench-feed: {}: no records, skipping", venue.name());
            continue;
        }
        let mut out: Vec<Event> = Vec::with_capacity(4096);
        let fast = measure_path(venue, &registry, records, PathKind::Fast, runs, &mut out)?;
        let slow = measure_path(venue, &registry, records, PathKind::Slow, runs, &mut out)?;
        // The differential guarantee: identical event streams implies (at
        // minimum) identical event counts over the full corpus.
        ensure!(
            fast.pass.events == slow.pass.events,
            "{}: fast/slow event counts differ ({} vs {}) — differential FAILURE",
            venue.name(),
            fast.pass.events,
            slow.pass.events,
        );
        let fast_msgs = per_sec(fast.pass.ws_frames, fast.median_ns);
        let slow_msgs = per_sec(slow.pass.ws_frames, slow.median_ns);
        let mult = if slow_msgs > 0.0 {
            fast_msgs / slow_msgs
        } else {
            0.0
        };
        eprintln!(
            "bench-feed: {:8} fast {:>10.0} msgs/s | slow {:>10.0} msgs/s | x{:.2} | {} frames, {} events, {} fallbacks",
            venue.name(),
            fast_msgs,
            slow_msgs,
            mult,
            fast.pass.ws_frames,
            fast.pass.events,
            fast.pass.fallbacks,
        );
        agg.entry("fast")
            .or_default()
            .push((fast.pass.ws_frames, fast.median_ns));
        agg.entry("slow")
            .or_default()
            .push((slow.pass.ws_frames, slow.median_ns));
        agg_events_fast.push((fast.pass.events, fast.median_ns));
        agg_bytes_fast.push((fast.pass.ws_payload_bytes, fast.median_ns));
        venues_json.push(serde_json::json!({
            "venue": venue.name(),
            "ws_frames": fast.pass.ws_frames,
            "rest_snapshots": fast.pass.rest_snapshots,
            "ws_payload_bytes": fast.pass.ws_payload_bytes,
            "fast": path_json(&fast),
            "slow": path_json(&slow),
            "fast_over_slow": mult,
        }));
    }
    ensure!(
        !venues_json.is_empty(),
        "no venues had records under --data"
    );

    let agg_fast = aggregate_per_sec(&agg["fast"]);
    let agg_slow = aggregate_per_sec(&agg["slow"]);
    let metrics = serde_json::json!({
        "venues": venues_json,
        "aggregate": {
            "fast_msgs_per_s": agg_fast,
            "slow_msgs_per_s": agg_slow,
            "fast_over_slow": if agg_slow > 0.0 { agg_fast / agg_slow } else { 0.0 },
            "fast_events_per_s": aggregate_per_sec(&agg_events_fast),
            "fast_bytes_per_s": aggregate_per_sec(&agg_bytes_fast),
        },
    });
    let config = serde_json::json!({
        "data": args.data.display().to_string(),
        "quick": args.quick,
        "ws_frame_cap_per_venue": cap,
        "measured_runs": runs,
        "warmup_runs": WARMUP_RUNS,
    });
    let mut notes = String::from(
        "Single-threaded: one codec instance parses one venue's records in capture order. \
         'msgs/sec/core' means single-thread msgs/s on one P-core equivalent; macOS does not \
         pin threads, the scheduler picks the core. fast = production fast path including its \
         real parse_slow fallback on Structure errors; slow = parse_slow on every frame (the \
         published naive serde_json baseline). REST snapshots are applied identically on both \
         paths and included in run time but excluded from the msgs/s denominator (WS frames \
         only). Differential guarantee: fast and slow event counts asserted equal per venue \
         over the full loaded corpus. Timing: median of measured fresh-codec runs.",
    );
    if args.quick {
        notes.push_str(" QUICK SMOKE RUN on a busy machine: NOT official numbers.");
    }
    let file = ResultFile::new("feed_parse", config, metrics, &notes);
    let path =
        write_result(&args.results_dir, &file, args.overwrite).context("write feed_parse.json")?;
    eprintln!(
        "bench-feed: aggregate fast {agg_fast:.0} msgs/s, slow {agg_slow:.0} msgs/s -> {}",
        path.display()
    );
    Ok(())
}

/// dhat block/byte deltas for one measured pass.
#[cfg(feature = "alloc-profile")]
fn alloc_json(
    before: &dhat::HeapStats,
    after: &dhat::HeapStats,
    pr: &PassResult,
) -> serde_json::Value {
    let d_blocks = after.total_blocks - before.total_blocks;
    let d_bytes = after.total_bytes - before.total_bytes;
    serde_json::json!({
        "ws_frames": pr.ws_frames,
        "rest_snapshots": pr.rest_snapshots,
        "events": pr.events,
        "fallbacks": pr.fallbacks,
        "parse_errors": pr.parse_errors,
        "total_blocks": d_blocks,
        "total_bytes": d_bytes,
        "blocks_per_frame": d_blocks as f64 / pr.ws_frames.max(1) as f64,
        "bytes_per_frame": d_bytes as f64 / pr.ws_frames.max(1) as f64,
    })
}

/// The allocation measurement: run a fixed 10k-frame/venue sample through
/// each path once with dhat counting, write `feed_alloc.json`.
#[cfg(feature = "alloc-profile")]
fn run_alloc_check(args: &Args) -> Result<()> {
    let _profiler = dhat::Profiler::builder().testing().build();
    let registry = Registry::builtin();
    eprintln!(
        "bench-feed: alloc-check loading {} (cap {ALLOC_SAMPLE_FRAMES} WS frames/venue)...",
        args.data.display()
    );
    let per_venue = load_records(&args.data, ALLOC_SAMPLE_FRAMES)?;

    let mut venues_json = Vec::new();
    for venue in Venue::ALL {
        let records = &per_venue[venue as usize - 1];
        if records.is_empty() {
            continue;
        }
        let mut paths = serde_json::Map::new();
        for path in [PathKind::Fast, PathKind::Slow] {
            let mut out: Vec<Event> = Vec::with_capacity(4096);
            // Warmup with a throwaway codec so the reused `out` buffer's
            // growth is amortized out of the measured window.
            {
                let mut warm = make_codec(venue, &registry);
                let _ = run_pass(warm.as_mut(), records, path, &mut out);
            }
            let mut codec = make_codec(venue, &registry);
            let before = dhat::HeapStats::get();
            let pr = run_pass(codec.as_mut(), records, path, &mut out);
            let after = dhat::HeapStats::get();
            eprintln!(
                "bench-feed: {:8} {:4} path: {} blocks / {} bytes over {} frames ({:.4} blocks/frame, {:.2} bytes/frame)",
                venue.name(),
                path.name(),
                after.total_blocks - before.total_blocks,
                after.total_bytes - before.total_bytes,
                pr.ws_frames,
                (after.total_blocks - before.total_blocks) as f64 / pr.ws_frames.max(1) as f64,
                (after.total_bytes - before.total_bytes) as f64 / pr.ws_frames.max(1) as f64,
            );
            paths.insert(path.name().to_string(), alloc_json(&before, &after, &pr));
        }
        venues_json.push(serde_json::json!({
            "venue": venue.name(),
            "paths": paths,
        }));
    }
    ensure!(
        !venues_json.is_empty(),
        "no venues had records under --data"
    );

    let config = serde_json::json!({
        "data": args.data.display().to_string(),
        "sample_ws_frames_per_venue": ALLOC_SAMPLE_FRAMES,
        "allocator": "dhat 0.3 (testing profiler)",
    });
    let notes = "dhat heap deltas around one measured pass per (venue, path). The measured \
                 pass uses a FRESH codec (a warmup pass on a separate codec instance amortizes \
                 only the shared Event out-buffer), so one-time per-codec lazy allocations — \
                 per-symbol state, internal scratch on first use, error/fallback paths — land \
                 inside the window and are averaged over the 10k-frame sample. Steady-state \
                 target for the fast path is 0 allocations/frame; the numbers here are the \
                 real measured deltas, whatever they are. REST snapshot records are parsed \
                 in-position on both paths and included in the deltas: the Binance fast-path \
                 delta is dominated by parse_rest_snapshot, which parses the depth body via \
                 serde_json::Value on BOTH paths by design (rest_snapshots is reported per \
                 pass so this is attributable); Coinbase's tiny fast-path residue is one-time \
                 per-codec state growth. Kraken (no REST resync in-sample) is the pure \
                 WS-frame fast path.";
    let file = ResultFile::new(
        "feed_alloc",
        config,
        serde_json::json!({ "venues": venues_json }),
        notes,
    );
    let path =
        write_result(&args.results_dir, &file, args.overwrite).context("write feed_alloc.json")?;
    eprintln!("bench-feed: alloc-check -> {}", path.display());
    Ok(())
}

/// Stub when the dhat allocator isn't compiled in.
#[cfg(not(feature = "alloc-profile"))]
fn run_alloc_check(_args: &Args) -> Result<()> {
    anyhow::bail!(
        "--alloc-check needs the dhat allocator; rebuild with:\n  cargo run --release \
         -p flashbook-bench --features alloc-profile --bin bench-feed -- --alloc-check ..."
    )
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(Cli::Run(a)) => a,
        Ok(Cli::Help) => {
            eprintln!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let res = if args.alloc_check {
        run_alloc_check(&args)
    } else {
        run_bench(&args)
    };
    match res {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bench-feed: {e:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(s: &[&str]) -> impl Iterator<Item = String> + use<> {
        s.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn run_args(cli: Result<Cli, String>) -> Args {
        match cli.unwrap() {
            Cli::Run(a) => a,
            Cli::Help => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_args_full_and_defaults() {
        let a = run_args(parse_args(argv(&[
            "--data",
            "data/smoke",
            "--quick",
            "--results-dir",
            "bench/results/tmp",
            "--overwrite",
        ])));
        assert_eq!(a.data, PathBuf::from("data/smoke"));
        assert!(a.quick);
        assert!(a.overwrite);
        assert!(!a.alloc_check);
        assert_eq!(a.results_dir, PathBuf::from("bench/results/tmp"));

        // bench/run-all.sh contract: bare invocation works, defaulting to
        // the soak capture root and the official results dir.
        let d = run_args(parse_args(argv(&[])));
        assert!(!d.quick && !d.alloc_check && !d.overwrite);
        assert_eq!(d.data, PathBuf::from("data/raw"));
        assert_eq!(d.results_dir, PathBuf::from("bench/results"));

        let c = run_args(parse_args(argv(&["--data", "x", "--alloc-check"])));
        assert!(c.alloc_check);
    }

    #[test]
    fn parse_args_help_and_bad_input() {
        // bench/run-all.sh probes with --help and requires exit 0.
        assert_eq!(parse_args(argv(&["--help"])).unwrap(), Cli::Help);
        assert_eq!(parse_args(argv(&["-h"])).unwrap(), Cli::Help);
        assert!(
            parse_args(argv(&["--data"]))
                .unwrap_err()
                .contains("--data needs")
        );
        assert!(
            parse_args(argv(&["--data", "x", "--bogus"]))
                .unwrap_err()
                .contains("unknown arg: --bogus")
        );
    }

    #[test]
    fn per_sec_math() {
        assert!((per_sec(1_000, 1_000_000_000) - 1_000.0).abs() < 1e-9);
        assert!((per_sec(500, 250_000_000) - 2_000.0).abs() < 1e-9);
        assert_eq!(per_sec(123, 0), 0.0, "zero elapsed must not divide");
        assert_eq!(per_sec(0, 1_000), 0.0);
    }

    #[test]
    fn median_is_nearest_rank() {
        assert_eq!(median_ns(&[42]), 42);
        assert_eq!(median_ns(&[3, 1, 2]), 2);
        // even n: lower median (nearest-rank p50), matching Percentiles
        assert_eq!(median_ns(&[10, 20]), 10);
        assert_eq!(median_ns(&[4, 1, 3, 2]), 2);
    }

    #[test]
    fn aggregate_per_sec_sums_counts_and_time() {
        // 100/s for 1s and 300/s for 1s -> 200/s overall
        let agg = aggregate_per_sec(&[(100, 1_000_000_000), (300, 1_000_000_000)]);
        assert!((agg - 200.0).abs() < 1e-9);
        assert_eq!(aggregate_per_sec(&[]), 0.0);
    }

    #[test]
    fn caps_and_runs_per_mode() {
        assert_eq!(frame_cap(true), 100_000);
        assert_eq!(frame_cap(false), 1_000_000);
        assert_eq!(measured_runs(true), 2);
        assert_eq!(measured_runs(false), 5);
    }
}
