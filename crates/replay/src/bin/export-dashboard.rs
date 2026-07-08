//! export-dashboard: generate the static JSON dataset the dashboard serves.
//!
//! Reads a sealed store segment (plus its `.snapidx` and `.ingest.json`
//! sidecars), the live soak `stats.jsonl`, and any committed
//! `bench/results/*.json`, and writes four JSON files plus a README into
//! `apps/dashboard/public/data`:
//!
//! - `meta.json`: corpus counters, soak summary (last stats lines), and the
//!   instrument universe.
//! - `books.json`: top-of-book + top-10 depth time series over the last
//!   `--window-mins` of the store's wall-clock span, sampled every
//!   `--step-s` seconds via the PIT snapshot index folded into a
//!   [`LadderBook`].
//! - `lag.json`: per-minute soak counter deltas, venue path latency
//!   percentiles (recv_wall - venue_ts), and the queue honesty note.
//! - `bench.json`: headline rows extracted from benchmark result files
//!   (`available: false` until Phase 6 produces them).
//!
//! All prices/quantities are converted to `f64` via
//! [`flashbook_proto::fixed::fixed_to_f64`] — lossy, display-only; the
//! store keeps exact 1e-8 mantissas.
//!
//! Usage: `export-dashboard --store <path.fbstore> [--snapidx <path>]
//!   [--stats ops/soak/stats.jsonl] [--bench-results bench/results]
//!   [--out apps/dashboard/public/data] [--window-mins 60] [--step-s 2]
//!   [--book-instruments 1,6,11]`
//!
//! Exit codes: 0 ok, 2 usage/IO error.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use flashbook_lob::{L2Book, LadderBook};
use flashbook_proto::event::{Event, Side, Venue};
use flashbook_proto::fixed::fixed_to_f64;
use flashbook_proto::instrument::Registry;
use flashbook_store::pit::{SnapEntry, SnapshotIndex, pit_scan};
use flashbook_store::segment::StoreReader;
use serde_json::{Value, json};

const USAGE: &str = "usage: export-dashboard --store <path.fbstore> [--snapidx <path>] \
     [--stats ops/soak/stats.jsonl] [--bench-results bench/results] \
     [--out apps/dashboard/public/data] [--window-mins 60] [--step-s 2] \
     [--book-instruments 1,6,11]";

/// Hard cap on points per book series; the step widens to stay under it.
const MAX_POINTS_PER_SERIES: u64 = 2000;
/// Book depth shipped per side per point.
const BOOK_DEPTH: usize = 10;
/// Kraken books are venue-capped at this depth (matches capture subscribe).
const KRAKEN_DEPTH_CAP: usize = 100;
/// Max latency samples kept per venue (stride thinning beyond this).
const MAX_LAT_SAMPLES: usize = 200_000;
/// Max per-minute delta rows kept per venue (bounds lag.json for long soaks).
const MAX_MINUTES_PER_VENUE: usize = 1440;
/// Max fallback rows extracted per bench result file.
const MAX_BENCH_ROWS_PER_FILE: usize = 32;

/// The honest queueing story shipped verbatim in `lag.json`.
const QUEUE_NOTE: &str = "capture has no internal queues by design: the hot path is inline \
     (socket read -> parse -> normalize -> append), so the kernel socket buffer is the only \
     queue; segment_bytes is the write-side gauge (current segment file size).";

/// Parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    /// Sealed store segment to export from.
    store: PathBuf,
    /// Snapshot-index sidecar (defaults to `<store>.snapidx`).
    snapidx: Option<PathBuf>,
    /// Soak stats file (missing => `soak.present = false`).
    stats: PathBuf,
    /// Benchmark results directory (empty/missing => `available = false`).
    bench_results: PathBuf,
    /// Output directory for the JSON dataset.
    out: PathBuf,
    /// Book window length in minutes (last N minutes of the store span).
    window_mins: u64,
    /// Requested sampling step in seconds (widened to honor the point cap).
    step_s: u64,
    /// Instrument ids to export book series for.
    instruments: Vec<u32>,
}

/// Parse CLI args (everything after argv[0]). Pure; returns a usage error
/// string on bad input.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Config, String> {
    let mut store: Option<PathBuf> = None;
    let mut snapidx: Option<PathBuf> = None;
    let mut stats = PathBuf::from("ops/soak/stats.jsonl");
    let mut bench_results = PathBuf::from("bench/results");
    let mut out = PathBuf::from("apps/dashboard/public/data");
    let mut window_mins: u64 = 60;
    let mut step_s: u64 = 2;
    let mut instruments: Vec<u32> = vec![1, 6, 11];

    let mut args = args.peekable();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--store" => store = args.next().map(PathBuf::from),
            "--snapidx" => snapidx = args.next().map(PathBuf::from),
            "--stats" => {
                stats = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or("--stats needs a path")?
            }
            "--bench-results" => {
                bench_results = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or("--bench-results needs a path")?;
            }
            "--out" => out = args.next().map(PathBuf::from).ok_or("--out needs a path")?,
            "--window-mins" => {
                window_mins = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .filter(|&v| v > 0)
                    .ok_or("--window-mins needs a positive integer")?;
            }
            "--step-s" => {
                step_s = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .filter(|&v| v > 0)
                    .ok_or("--step-s needs a positive integer")?;
            }
            "--book-instruments" => {
                instruments = args
                    .next()
                    .as_deref()
                    .map(parse_instrument_list)
                    .transpose()?
                    .ok_or("--book-instruments needs a comma-separated id list")?;
            }
            "--help" | "-h" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown arg: {other}\n{USAGE}")),
        }
    }
    let store = store.ok_or_else(|| format!("--store <path> required\n{USAGE}"))?;
    Ok(Config {
        store,
        snapidx,
        stats,
        bench_results,
        out,
        window_mins,
        step_s,
        instruments,
    })
}

/// Parse `1,6,11` into instrument ids (pure).
fn parse_instrument_list(s: &str) -> Result<Vec<u32>, String> {
    let ids: Vec<u32> = s
        .split(',')
        .filter(|t| !t.trim().is_empty())
        .map(|t| {
            t.trim()
                .parse::<u32>()
                .map_err(|_| format!("bad instrument id: {t}"))
        })
        .collect::<Result<_, _>>()?;
    if ids.is_empty() {
        return Err("--book-instruments list is empty".to_string());
    }
    Ok(ids)
}

/// Effective sampling step: the requested step widened so the window never
/// yields more than `max_points` samples (pure).
fn effective_step_s(window_s: u64, step_s: u64, max_points: u64) -> u64 {
    step_s.max(window_s.div_ceil(max_points.max(1)))
}

/// p50/p90/p99 summary of a sample set.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Pcts {
    p50: f64,
    p90: f64,
    p99: f64,
    n: usize,
}

/// Nearest-rank percentiles over unsorted samples (pure; `None` if empty).
fn percentiles(samples: &mut [f64]) -> Option<Pcts> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable_by(f64::total_cmp);
    let rank = |q: f64| -> f64 {
        let idx = ((q / 100.0 * samples.len() as f64).ceil() as usize)
            .saturating_sub(1)
            .min(samples.len() - 1);
        samples[idx]
    };
    Some(Pcts {
        p50: rank(50.0),
        p90: rank(90.0),
        p99: rank(99.0),
        n: samples.len(),
    })
}

/// u64 field lookup on a JSON object, defaulting to 0 (pure).
fn uval(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// Parse stats.jsonl text into one JSON object per parseable line (pure).
fn parse_stats_lines(text: &str) -> Vec<Value> {
    text.lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v.get("venue").and_then(Value::as_str).is_some())
        .collect()
}

/// Build the `soak` object of meta.json from parsed stats lines: the LAST
/// line per venue plus the last `total` line (summed from per-venue lines
/// if a total line is absent). `restarts` is folded in verbatim (pure).
fn soak_summary(lines: &[Value], restarts: u64) -> Value {
    if lines.is_empty() {
        return json!({ "present": false });
    }
    // Last line per venue, preserving first-seen venue order.
    let mut order: Vec<String> = Vec::new();
    let mut last: BTreeMap<String, &Value> = BTreeMap::new();
    for l in lines {
        let venue = l["venue"].as_str().unwrap_or_default().to_string();
        if !order.contains(&venue) {
            order.push(venue.clone());
        }
        last.insert(venue, l);
    }
    let per_venue: Vec<Value> = order
        .iter()
        .filter(|v| v.as_str() != "total")
        .filter_map(|v| last.get(v))
        .map(|l| {
            json!({
                "venue": l["venue"],
                "msgs": uval(l, "msgs"),
                "events": uval(l, "events"),
                "gaps": uval(l, "gaps"),
                "reconnects": uval(l, "reconnects"),
                "parse_errors": uval(l, "parse_errors"),
            })
        })
        .collect();
    let sum = |key: &str| -> u64 {
        order
            .iter()
            .filter(|v| v.as_str() != "total")
            .filter_map(|v| last.get(v))
            .map(|l| uval(l, key))
            .sum()
    };
    let total: Value = last.get("total").map_or_else(
        || {
            json!({
                "msgs": sum("msgs"), "events": sum("events"), "gaps": sum("gaps"),
                "reconnects": sum("reconnects"), "fallbacks": sum("fallbacks"),
                "parse_errors": sum("parse_errors"), "rest_snaps": sum("rest_snaps"),
                "rss_max_mb": order.iter().filter_map(|v| last.get(v))
                    .map(|l| uval(l, "rss_max_mb")).max().unwrap_or(0),
                "uptime_s": order.iter().filter_map(|v| last.get(v))
                    .map(|l| uval(l, "uptime_s")).max().unwrap_or(0),
            })
        },
        |l| (*l).clone(),
    );
    json!({
        "present": true,
        "uptime_s": uval(&total, "uptime_s"),
        "msgs": uval(&total, "msgs"),
        "events": uval(&total, "events"),
        "gaps": uval(&total, "gaps"),
        "reconnects": uval(&total, "reconnects"),
        "fallbacks": uval(&total, "fallbacks"),
        "parse_errors": uval(&total, "parse_errors"),
        "rest_snaps": uval(&total, "rest_snaps"),
        "rss_max_mb": uval(&total, "rss_max_mb"),
        "restarts": restarts,
        "per_venue": per_venue,
    })
}

/// Per-minute counter deltas between consecutive per-venue stats lines.
/// Counters in stats.jsonl are cumulative; a counter that shrank (capture
/// restart) contributes its new cumulative value as the delta. Keeps at
/// most `max_per_venue` most-recent rows per venue (pure).
fn per_minute_deltas(lines: &[Value], max_per_venue: usize) -> Vec<Value> {
    let mut prev: BTreeMap<String, &Value> = BTreeMap::new();
    let mut per_venue: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let delta = |cur: &Value, prev: &Value, key: &str| -> u64 {
        let (c, p) = (uval(cur, key), uval(prev, key));
        if c >= p { c - p } else { c }
    };
    for l in lines {
        let venue = l["venue"].as_str().unwrap_or_default().to_string();
        if venue == "total" || venue.is_empty() {
            continue;
        }
        if let Some(p) = prev.get(&venue) {
            per_venue.entry(venue.clone()).or_default().push(json!({
                "t_unix_s": uval(l, "ts_wall_ns") / 1_000_000_000,
                "venue": venue,
                "msgs": delta(l, p, "msgs"),
                "events": delta(l, p, "events"),
                "gaps": delta(l, p, "gaps"),
                "reconnects": delta(l, p, "reconnects"),
                "segment_bytes": uval(l, "current_segment_bytes"),
            }));
        }
        prev.insert(venue, l);
    }
    let mut rows: Vec<Value> = Vec::new();
    for (_, mut v) in per_venue {
        if v.len() > max_per_venue {
            v.drain(..v.len() - max_per_venue);
        }
        rows.extend(v);
    }
    // Interleave venues chronologically for the dashboard.
    rows.sort_by_key(|r| {
        (
            uval(r, "t_unix_s"),
            r["venue"].as_str().unwrap_or("").to_string(),
        )
    });
    rows
}

/// Flatten every numeric leaf of a JSON tree into dotted-key rows (pure).
fn flatten_numbers(prefix: &str, v: &Value, out: &mut Vec<(String, f64)>) {
    match v {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                out.push((prefix.to_string(), f));
            }
        }
        Value::Object(m) => {
            for (k, child) in m {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten_numbers(&key, child, out);
            }
        }
        Value::Array(a) => {
            for (i, child) in a.iter().enumerate() {
                flatten_numbers(&format!("{prefix}.{i}"), child, out);
            }
        }
        _ => {}
    }
}

/// True when a flattened metric key is one of the headline metrics the
/// dashboard leads with (pure heuristic; schema-tolerant by substring).
fn is_headline(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    [
        "msgs_per_s",
        "events_per_s",
        "multiplier",
        "bytes_per_event",
        "gb_per_s",
        "total_added",
    ]
    .iter()
    .any(|p| k.contains(p))
        || k.split(['.', '_'])
            .any(|tok| matches!(tok, "p50" | "p90" | "p99" | "p999"))
}

/// Best-effort display unit for a flattened metric key (pure).
fn unit_for(key: &str) -> &'static str {
    let k = key.to_ascii_lowercase();
    if k.contains("msgs_per_s") {
        return "msgs/s";
    }
    if k.contains("events_per_s") {
        return "events/s";
    }
    if k.contains("gb_per_s") {
        return "GB/s";
    }
    if k.contains("bytes_per_event") {
        return "B/event";
    }
    if k.contains("multiplier") {
        return "x";
    }
    for tok in k.split(['.', '_']).rev() {
        match tok {
            "ns" => return "ns",
            "us" => return "us",
            "ms" => return "ms",
            "s" => return "s",
            "mb" => return "MB",
            "gb" => return "GB",
            "bytes" => return "bytes",
            "pct" => return "%",
            _ => {}
        }
    }
    ""
}

/// Extract bench.json rows from one result file's JSON (pure).
///
/// Headline metrics (throughputs, multipliers, bytes/event, GB/s,
/// percentile latencies) are selected by key heuristics; when a file
/// exposes none of them, every numeric metric leaf is included, capped at
/// [`MAX_BENCH_ROWS_PER_FILE`].
fn bench_rows(file_name: &str, root: &Value) -> Vec<Value> {
    let section = root
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_else(|| file_name.trim_end_matches(".json"))
        .to_string();
    let mut leaves = Vec::new();
    flatten_numbers("", root.get("metrics").unwrap_or(&Value::Null), &mut leaves);
    let mut headline: Vec<&(String, f64)> = leaves.iter().filter(|(k, _)| is_headline(k)).collect();
    if headline.is_empty() {
        headline = leaves.iter().collect();
    }
    headline
        .into_iter()
        .take(MAX_BENCH_ROWS_PER_FILE)
        .map(|(k, v)| {
            json!({
                "section": section,
                "metric": k,
                "value": v,
                "unit": unit_for(k),
                "source_file": file_name,
            })
        })
        .collect()
}

/// Folds a PIT event stream into a book and samples it on wall-clock step
/// boundaries within `[start_wall, end_wall]`.
///
/// Steps are aligned to multiples of `step_ns` (round unix timestamps).
/// A step is emitted only when the book is synced with both sides present
/// — steps before the first complete snapshot are skipped per contract.
struct BookSampler {
    book: LadderBook,
    step_ns: u64,
    next_wall: u64,
    end_wall: u64,
    points: Vec<Value>,
    scratch: Vec<(i64, i64)>,
}

impl BookSampler {
    fn new(depth_cap: Option<usize>, start_wall: u64, end_wall: u64, step_ns: u64) -> Self {
        Self {
            book: depth_cap.map_or_else(LadderBook::new, LadderBook::with_max_depth),
            step_ns,
            next_wall: start_wall.div_ceil(step_ns) * step_ns,
            end_wall,
            points: Vec::new(),
            scratch: Vec::new(),
        }
    }

    /// Apply one event, emitting samples for every step boundary crossed
    /// before it (the sample at time t reflects all events with wall <= t).
    fn on_event(&mut self, e: &Event) {
        while self.next_wall <= self.end_wall && e.recv_wall_ns > self.next_wall {
            self.sample();
            self.next_wall += self.step_ns;
        }
        self.book.apply(e);
    }

    /// Emit the remaining step samples and return the point list.
    fn finish(mut self) -> Vec<Value> {
        while self.next_wall <= self.end_wall {
            self.sample();
            self.next_wall += self.step_ns;
        }
        self.points
    }

    fn sample(&mut self) {
        if !self.book.is_synced() {
            return;
        }
        let (Some((bp, _)), Some((ap, _))) = (self.book.best_bid(), self.book.best_ask()) else {
            return;
        };
        let (bid, ask) = (fixed_to_f64(bp), fixed_to_f64(ap));
        let ladder = |book: &LadderBook, side: Side, scratch: &mut Vec<(i64, i64)>| -> Value {
            book.top_n_into(side, BOOK_DEPTH, scratch);
            Value::Array(
                scratch
                    .iter()
                    .map(|&(p, q)| json!([fixed_to_f64(p), fixed_to_f64(q)]))
                    .collect(),
            )
        };
        let bid10 = ladder(&self.book, Side::Bid, &mut self.scratch);
        let ask10 = ladder(&self.book, Side::Ask, &mut self.scratch);
        self.points.push(json!({
            "t": self.next_wall / 1_000_000_000,
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2.0,
            "bid10": bid10,
            "ask10": ask10,
        }));
    }
}

/// Latency sample reservoir: keeps every `stride`-th sample and doubles the
/// stride (halving kept samples) whenever the cap is reached, so at most
/// [`MAX_LAT_SAMPLES`] evenly-strided samples survive.
#[derive(Default)]
struct StridedSamples {
    samples: Vec<f64>,
    stride: u64,
    seen: u64,
}

impl StridedSamples {
    fn push(&mut self, v: f64) {
        if self.stride == 0 {
            self.stride = 1;
        }
        if self.seen.is_multiple_of(self.stride) {
            self.samples.push(v);
            if self.samples.len() >= MAX_LAT_SAMPLES {
                let mut i = 0usize;
                self.samples.retain(|_| {
                    let keep = i.is_multiple_of(2);
                    i += 1;
                    keep
                });
                self.stride *= 2;
            }
        }
        self.seen += 1;
    }
}

/// `<store>.ingest.json` sidecar path (matches the ingest tool's naming).
fn ingest_sidecar_path(store: &Path) -> PathBuf {
    let mut s = store.as_os_str().to_owned();
    s.push(".ingest.json");
    PathBuf::from(s)
}

/// Default `<store>.snapidx` sidecar path.
fn snapidx_default_path(store: &Path) -> PathBuf {
    let mut s = store.as_os_str().to_owned();
    s.push(".snapidx");
    PathBuf::from(s)
}

/// Load the snapshot index sidecar, rebuilding from the segment when the
/// sidecar is missing, torn, or corrupt (it is derived data).
fn load_or_build_index(reader: &StoreReader, path: &Path) -> Result<SnapshotIndex, String> {
    match SnapshotIndex::load(path) {
        Ok(idx) => Ok(idx),
        Err(_) => SnapshotIndex::build(reader).map_err(|e| format!("snapshot index build: {e}")),
    }
}

/// First indexed snapshot for `instrument`, if any (fallback when no
/// snapshot exists at or before the window start).
fn first_entry_for(index: &SnapshotIndex, instrument: u32) -> Option<SnapEntry> {
    index
        .entries()
        .iter()
        .find(|e| e.instrument == instrument)
        .copied()
}

/// README.md shipped next to the dataset.
fn readme_text(cfg: &Config) -> String {
    format!(
        "# Dashboard data\n\n\
         These files (`meta.json`, `books.json`, `lag.json`, `bench.json`) are GENERATED by\n\
         `export-dashboard` from real captured market data — do not edit by hand. Prices and\n\
         quantities are lossy `f64` conversions of the store's exact 1e-8 fixed-point\n\
         mantissas, for display only.\n\n\
         Regenerate (from the repo root; ingest a capture first if needed):\n\n\
         ```sh\n\
         cargo run --release -p flashbook-replay --bin ingest -- \\\n\
        \x20    --data data/smoke --out data/tmp/smoke.fbstore --zstd 3\n\
         cargo run --release -p flashbook-replay --bin export-dashboard -- \\\n\
        \x20    --store data/tmp/smoke.fbstore --stats {stats} \\\n\
        \x20    --bench-results {bench} --out {out} \\\n\
        \x20    --window-mins {win} --step-s {step} --book-instruments {ids}\n\
         ```\n",
        stats = cfg.stats.display(),
        bench = cfg.bench_results.display(),
        out = cfg.out.display(),
        win = cfg.window_mins,
        step = cfg.step_s,
        ids = cfg
            .instruments
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(","),
    )
}

/// Run one export. Returns the list of written file paths.
fn run(cfg: &Config) -> Result<Vec<PathBuf>, String> {
    let registry = Registry::builtin();
    let reader = StoreReader::open(&cfg.store)
        .map_err(|e| format!("open store {}: {e}", cfg.store.display()))?;
    let blocks = reader.blocks();
    let (Some(first_blk), Some(last_blk)) = (blocks.first(), blocks.last()) else {
        return Err(format!("store {} has no blocks", cfg.store.display()));
    };
    let (first_wall, last_wall) = (first_blk.min_recv_wall, last_blk.max_recv_wall);
    let (first_mono, last_mono) = (first_blk.min_recv_mono, last_blk.max_recv_mono);
    // Wall->mono anchor from the newest block: mono is process-relative, so
    // this offset is exact for the capture process that wrote the window
    // tail and approximate across capture restarts (dashboard tolerance).
    let wall_off = last_wall.saturating_sub(last_mono);

    let sidecar = cfg
        .snapidx
        .clone()
        .unwrap_or_else(|| snapidx_default_path(&cfg.store));
    let index = load_or_build_index(&reader, &sidecar)?;

    // --- window ---
    let window_ns = cfg.window_mins.saturating_mul(60_000_000_000);
    let start_wall = first_wall.max(last_wall.saturating_sub(window_ns));
    let window_s = (last_wall - start_wall).div_ceil(1_000_000_000);
    let step_s = effective_step_s(window_s, cfg.step_s, MAX_POINTS_PER_SERIES);
    let step_ns = step_s * 1_000_000_000;
    let start_mono = start_wall
        .saturating_sub(wall_off)
        .clamp(first_mono, last_mono);

    // --- books.json ---
    let mut series = Vec::new();
    for &id in &cfg.instruments {
        let meta = registry
            .get(id)
            .ok_or_else(|| format!("unknown instrument id {id}"))?;
        let depth_cap = (meta.venue == Venue::Kraken).then_some(KRAKEN_DEPTH_CAP);
        let entry = index
            .latest_at(id, start_mono)
            .copied()
            .or_else(|| first_entry_for(&index, id));
        let points = match entry {
            Some(entry) => {
                let mut sampler = BookSampler::new(depth_cap, start_wall, last_wall, step_ns);
                pit_scan(&reader, &entry, last_mono, |e| sampler.on_event(e))
                    .map_err(|e| format!("pit scan instrument {id}: {e}"))?;
                sampler.finish()
            }
            None => Vec::new(),
        };
        series.push(json!({
            "instrument": id,
            "label": format!("{}:{}", meta.venue.name(), meta.venue_symbol),
            "points": points,
        }));
    }
    let books = json!({
        "window": { "start_wall_ns": start_wall, "end_wall_ns": last_wall, "step_s": step_s },
        "series": series,
    });

    // --- lag.json ---
    // Venue path latency (recv_wall - venue_ts) over the books window,
    // strided to at most MAX_LAT_SAMPLES samples per venue.
    let mut per_venue_lat: BTreeMap<u8, StridedSamples> = BTreeMap::new();
    reader
        .scan_mono_range(start_mono, last_mono, |e| {
            if e.venue_ts_ns != 0 {
                per_venue_lat
                    .entry(e.venue)
                    .or_default()
                    .push((e.recv_wall_ns as f64 - e.venue_ts_ns as f64) / 1e6);
            }
        })
        .map_err(|e| format!("latency scan: {e}"))?;
    let venue_path_ms: Vec<Value> = per_venue_lat
        .iter_mut()
        .filter_map(|(&vb, s)| {
            let name = Venue::try_from(vb).map(Venue::name).unwrap_or("unknown");
            percentiles(&mut s.samples).map(
                |p| json!({ "venue": name, "p50": p.p50, "p90": p.p90, "p99": p.p99, "n": p.n }),
            )
        })
        .collect();

    let stats_text = std::fs::read_to_string(&cfg.stats).ok();
    let stats_lines = stats_text
        .as_deref()
        .map(parse_stats_lines)
        .unwrap_or_default();
    let lag = json!({
        "per_minute": per_minute_deltas(&stats_lines, MAX_MINUTES_PER_VENUE),
        "venue_path_ms": venue_path_ms,
        "queue_note": QUEUE_NOTE,
    });

    // --- meta.json ---
    let ingest: Value = std::fs::read_to_string(ingest_sidecar_path(&cfg.store))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(Value::Null);
    let events = match uval(&ingest, "events") {
        0 => reader.n_events(),
        n => n,
    };
    let store_bytes = match uval(&ingest, "store_bytes") {
        0 => std::fs::metadata(&cfg.store).map(|m| m.len()).unwrap_or(0),
        n => n,
    };
    let span_s = ingest
        .get("span_s")
        .and_then(Value::as_f64)
        .unwrap_or(last_mono.saturating_sub(first_mono) as f64 / 1e9);
    let bytes_per_event = ingest
        .get("bytes_per_event")
        .and_then(Value::as_f64)
        .unwrap_or(if events == 0 {
            0.0
        } else {
            store_bytes as f64 / events as f64
        });
    let restarts = cfg
        .stats
        .parent()
        .map(|d| d.join("restarts.log"))
        .and_then(|p| std::fs::read_to_string(p).ok())
        .map(|s| s.lines().count() as u64)
        .unwrap_or(0);
    let instruments_json: Vec<Value> = registry
        .all()
        .iter()
        .map(|m| {
            json!({
                "id": m.id,
                "venue": m.venue.name(),
                "venue_symbol": m.venue_symbol,
                "canonical": m.canonical,
            })
        })
        .collect();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let meta = json!({
        "generated_unix_ms": now_ms,
        "corpus": {
            "events": events,
            "ws_frames": uval(&ingest, "ws_frames"),
            "span_s": span_s,
            "first_wall_ns": first_wall,
            "last_wall_ns": last_wall,
            "raw_payload_bytes": uval(&ingest, "raw_payload_bytes"),
            "store_bytes": store_bytes,
            "bytes_per_event": bytes_per_event,
            "checksums_ok": uval(&ingest, "checksums_ok"),
            "checksum_mismatches": uval(&ingest, "checksum_mismatches"),
            "parse_errors": uval(&ingest, "parse_errors"),
            "gaps": uval(&ingest, "gaps"),
        },
        "soak": soak_summary(&stats_lines, restarts),
        "instruments": instruments_json,
    });

    // --- bench.json ---
    let mut bench_files: Vec<PathBuf> = std::fs::read_dir(&cfg.bench_results)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.is_file() && p.extension().is_some_and(|x| x == "json"))
                .collect()
        })
        .unwrap_or_default();
    bench_files.sort();
    let mut generated_from: Vec<String> = Vec::new();
    let mut rows: Vec<Value> = Vec::new();
    for p in &bench_files {
        let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Ok(text) = std::fs::read_to_string(p) else {
            continue;
        };
        let Ok(root) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        generated_from.push(name.to_string());
        rows.extend(bench_rows(name, &root));
    }
    let bench = json!({
        "available": !rows.is_empty(),
        "generated_from": generated_from,
        "rows": rows,
    });

    // --- write ---
    std::fs::create_dir_all(&cfg.out).map_err(|e| format!("create {}: {e}", cfg.out.display()))?;
    let mut written = Vec::new();
    for (name, doc) in [
        ("meta.json", &meta),
        ("books.json", &books),
        ("lag.json", &lag),
        ("bench.json", &bench),
    ] {
        let path = cfg.out.join(name);
        let bytes = serde_json::to_string(doc).map_err(|e| format!("encode {name}: {e}"))?;
        std::fs::write(&path, bytes).map_err(|e| format!("write {}: {e}", path.display()))?;
        written.push(path);
    }
    let readme = cfg.out.join("README.md");
    std::fs::write(&readme, readme_text(cfg))
        .map_err(|e| format!("write {}: {e}", readme.display()))?;
    written.push(readme);
    Ok(written)
}

fn main() -> ExitCode {
    let cfg = match parse_args(std::env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    match run(&cfg) {
        Ok(written) => {
            for p in written {
                let bytes = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
                println!("{}\t{} bytes", p.display(), bytes);
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("export-dashboard failed: {e}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flashbook_proto::event::EventKind;

    #[test]
    fn parse_args_full_defaults_and_errors() {
        let full = parse_args(
            [
                "--store",
                "s.fbstore",
                "--snapidx",
                "s.idx",
                "--stats",
                "st.jsonl",
                "--bench-results",
                "br",
                "--out",
                "o",
                "--window-mins",
                "30",
                "--step-s",
                "5",
                "--book-instruments",
                "2,7,12",
            ]
            .iter()
            .map(ToString::to_string),
        )
        .unwrap();
        assert_eq!(
            full,
            Config {
                store: PathBuf::from("s.fbstore"),
                snapidx: Some(PathBuf::from("s.idx")),
                stats: PathBuf::from("st.jsonl"),
                bench_results: PathBuf::from("br"),
                out: PathBuf::from("o"),
                window_mins: 30,
                step_s: 5,
                instruments: vec![2, 7, 12],
            }
        );

        let defaults = parse_args(["--store", "s"].iter().map(ToString::to_string)).unwrap();
        assert_eq!(defaults.snapidx, None);
        assert_eq!(defaults.stats, PathBuf::from("ops/soak/stats.jsonl"));
        assert_eq!(defaults.bench_results, PathBuf::from("bench/results"));
        assert_eq!(defaults.out, PathBuf::from("apps/dashboard/public/data"));
        assert_eq!(defaults.window_mins, 60);
        assert_eq!(defaults.step_s, 2);
        assert_eq!(defaults.instruments, vec![1, 6, 11]);

        assert!(parse_args(std::iter::empty()).is_err(), "--store required");
        assert!(
            parse_args(
                ["--store", "s", "--step-s", "0"]
                    .iter()
                    .map(ToString::to_string)
            )
            .is_err(),
            "zero step rejected"
        );
        assert!(
            parse_args(
                ["--store", "s", "--book-instruments", "1,x"]
                    .iter()
                    .map(ToString::to_string)
            )
            .is_err(),
            "bad id rejected"
        );
        assert!(
            parse_args(["--bogus".to_string()].into_iter()).is_err(),
            "unknown arg rejected"
        );
        assert_eq!(parse_instrument_list("1, 6 ,11").unwrap(), vec![1, 6, 11]);
        assert!(parse_instrument_list(",").is_err(), "empty list rejected");
    }

    #[test]
    fn effective_step_honors_point_cap() {
        // 3600 s window, 2 s step, cap 2000 -> 1800 points, step unchanged.
        assert_eq!(effective_step_s(3600, 2, 2000), 2);
        // 86400 s window, 2 s step, cap 2000 -> widened to ceil(86400/2000)=44.
        assert_eq!(effective_step_s(86_400, 2, 2000), 44);
        // Requested step already wider than the cap requires.
        assert_eq!(effective_step_s(100, 30, 2000), 30);
        // Degenerate cap never divides by zero.
        assert_eq!(effective_step_s(100, 1, 0), 100);
    }

    #[test]
    fn percentiles_nearest_rank() {
        assert_eq!(percentiles(&mut []), None);
        let mut one = vec![7.5];
        let p = percentiles(&mut one).unwrap();
        assert_eq!((p.p50, p.p90, p.p99, p.n), (7.5, 7.5, 7.5, 1));
        // 1..=100 (shuffled-ish order): nearest-rank pXX == XX exactly.
        let mut v: Vec<f64> = (1..=100).rev().map(f64::from).collect();
        let p = percentiles(&mut v).unwrap();
        assert_eq!((p.p50, p.p90, p.p99, p.n), (50.0, 90.0, 99.0, 100));
    }

    fn stats_line(t_ns: u64, venue: &str, msgs: u64, events: u64, seg: u64, up: u64) -> String {
        format!(
            r#"{{"ts_wall_ns":{t_ns},"venue":"{venue}","msgs":{msgs},"bytes":1,"events":{events},"gaps":0,"resyncs":0,"reconnects":1,"fallbacks":2,"parse_errors":0,"rest_snaps":3,"rss_mb":28,"rss_max_mb":39,"segments":4,"current_segment_bytes":{seg},"uptime_s":{up}}}"#
        )
    }

    #[test]
    fn soak_summary_uses_last_lines_and_handles_missing() {
        assert_eq!(soak_summary(&[], 0), json!({ "present": false }));

        let text = [
            stats_line(1_000_000_000, "coinbase", 10, 100, 5, 60),
            stats_line(1_000_000_000, "kraken", 20, 200, 6, 60),
            stats_line(1_000_000_000, "total", 30, 300, 11, 60),
            stats_line(61_000_000_000, "coinbase", 15, 150, 7, 120),
            stats_line(61_000_000_000, "kraken", 24, 260, 8, 120),
            stats_line(61_000_000_000, "total", 39, 410, 15, 120),
            "not json".to_string(),
        ]
        .join("\n");
        let lines = parse_stats_lines(&text);
        assert_eq!(lines.len(), 6, "bad line skipped");
        let s = soak_summary(&lines, 2);
        assert_eq!(s["present"], json!(true));
        assert_eq!(s["msgs"], json!(39), "last total line");
        assert_eq!(s["events"], json!(410));
        assert_eq!(s["uptime_s"], json!(120));
        assert_eq!(s["rss_max_mb"], json!(39));
        assert_eq!(s["restarts"], json!(2));
        let pv = s["per_venue"].as_array().unwrap();
        assert_eq!(pv.len(), 2, "total excluded from per_venue");
        assert_eq!(pv[0]["venue"], json!("coinbase"), "first-seen order");
        assert_eq!(pv[0]["msgs"], json!(15), "last per-venue line");
        assert_eq!(pv[1]["events"], json!(260));

        // No total line: totals are summed from per-venue last lines.
        let no_total = parse_stats_lines(&stats_line(1, "kraken", 5, 50, 1, 60));
        let s = soak_summary(&no_total, 0);
        assert_eq!(s["msgs"], json!(5));
        assert_eq!(s["uptime_s"], json!(60));
    }

    #[test]
    fn per_minute_deltas_are_counter_diffs_with_reset_tolerance() {
        let text = [
            stats_line(60_000_000_000, "coinbase", 10, 100, 5, 60),
            stats_line(60_000_000_000, "total", 10, 100, 5, 60),
            stats_line(120_000_000_000, "coinbase", 25, 160, 9, 120),
            // counter reset (capture restart): cumulative fell to 4.
            stats_line(180_000_000_000, "coinbase", 4, 40, 2, 60),
        ]
        .join("\n");
        let rows = per_minute_deltas(&parse_stats_lines(&text), 100);
        assert_eq!(
            rows.len(),
            2,
            "first line per venue has no delta; total skipped"
        );
        assert_eq!(rows[0]["t_unix_s"], json!(120));
        assert_eq!(rows[0]["venue"], json!("coinbase"));
        assert_eq!(rows[0]["msgs"], json!(15));
        assert_eq!(rows[0]["events"], json!(60));
        assert_eq!(rows[0]["segment_bytes"], json!(9), "gauge, not delta");
        assert_eq!(rows[1]["msgs"], json!(4), "reset: new cumulative stands in");

        // Per-venue cap keeps the most recent rows.
        let capped = per_minute_deltas(&parse_stats_lines(&text), 1);
        assert_eq!(capped.len(), 1);
        assert_eq!(capped[0]["t_unix_s"], json!(180));
    }

    #[test]
    fn bench_rows_extract_headlines_with_fallback() {
        let root = json!({
            "schema": 1,
            "name": "feed_decode",
            "metrics": {
                "fast": { "msgs_per_s": 2.5e6, "multiplier": 41.0 },
                "path_ns": { "p50": 100, "p99": 950 },
                "warmup_iters": 3,
            },
        });
        let rows = bench_rows("feed_decode.json", &root);
        let metrics: Vec<&str> = rows.iter().map(|r| r["metric"].as_str().unwrap()).collect();
        assert!(metrics.contains(&"fast.msgs_per_s"), "{metrics:?}");
        assert!(metrics.contains(&"fast.multiplier"), "{metrics:?}");
        assert!(metrics.contains(&"path_ns.p99"), "{metrics:?}");
        assert!(!metrics.contains(&"warmup_iters"), "non-headline excluded");
        let r = rows
            .iter()
            .find(|r| r["metric"] == "fast.msgs_per_s")
            .unwrap();
        assert_eq!(r["section"], json!("feed_decode"));
        assert_eq!(r["unit"], json!("msgs/s"));
        assert_eq!(r["source_file"], json!("feed_decode.json"));
        let r = rows.iter().find(|r| r["metric"] == "path_ns.p99").unwrap();
        assert_eq!(r["unit"], json!("ns"));

        // No headline keys at all -> every numeric leaf ships (capped).
        let plain = json!({ "metrics": { "foo": 1, "bar": { "baz": 2 } } });
        let rows = bench_rows("x.json", &plain);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["section"], json!("x"), "filename stem fallback");
    }

    /// Synthetic event at wall-time `t_s` seconds.
    fn ev(kind: EventKind, t_s: u64, price: i64, qty: i64) -> Event {
        Event {
            recv_mono_ns: t_s * 1_000_000_000,
            recv_wall_ns: t_s * 1_000_000_000,
            price,
            qty,
            instrument: 1,
            kind: kind as u8,
            venue: Venue::Coinbase as u8,
            ..Event::ZERO
        }
    }

    #[test]
    fn book_sampler_skips_unsynced_steps_and_samples_book_state() {
        let s = flashbook_proto::SCALE; // 1.0 in mantissa units
        // Window [100 s, 110 s], step 2 s. Snapshot completes at t=103.
        let mut b = BookSampler::new(None, 100_000_000_000, 110_000_000_000, 2_000_000_000);
        for e in [
            ev(EventKind::SnapBegin, 101, 0, 0),
            ev(EventKind::SnapBid, 101, 100 * s, 3 * s),
            ev(EventKind::SnapBid, 101, 99 * s, s),
            ev(EventKind::SnapAsk, 101, 101 * s, 2 * s),
            ev(EventKind::SnapEnd, 103, 0, 0),
            // Improves the bid; first visible at the t=106 sample.
            ev(EventKind::BidSet, 105, 100 * s + s / 2, s),
        ] {
            b.on_event(&e);
        }
        let points = b.finish();
        // Steps at 100,102 skipped (unsynced); 104,106,108,110 sampled.
        let ts: Vec<u64> = points.iter().map(|p| p["t"].as_u64().unwrap()).collect();
        assert_eq!(ts, vec![104, 106, 108, 110]);
        assert_eq!(points[0]["bid"], json!(100.0));
        assert_eq!(points[0]["ask"], json!(101.0));
        assert_eq!(points[0]["mid"], json!(100.5));
        assert_eq!(points[1]["bid"], json!(100.5), "delta applied before t=106");
        let bid10 = points[1]["bid10"].as_array().unwrap();
        assert_eq!(bid10.len(), 3);
        assert_eq!(bid10[0], json!([100.5, 1.0]), "best bid first (descending)");
        assert_eq!(bid10[2], json!([99.0, 1.0]));
        let ask10 = points[1]["ask10"].as_array().unwrap();
        assert_eq!(ask10[0], json!([101.0, 2.0]), "best ask first (ascending)");
    }

    #[test]
    fn strided_samples_stay_bounded_and_evenly_strided() {
        let mut s = StridedSamples::default();
        for i in 0..1_000_000u64 {
            s.push(i as f64);
        }
        assert!(s.samples.len() <= MAX_LAT_SAMPLES);
        assert!(
            s.samples.len() >= MAX_LAT_SAMPLES / 4,
            "{}",
            s.samples.len()
        );
        // Evenly strided: consecutive kept samples differ by the stride.
        let d = s.samples[1] - s.samples[0];
        assert!(s.samples.windows(2).all(|w| w[1] - w[0] == d));
    }

    #[test]
    fn unit_heuristics_do_not_confuse_msgs_with_ms() {
        assert_eq!(unit_for("fast.msgs_per_s"), "msgs/s");
        assert_eq!(unit_for("msgs"), "", "msgs is not milliseconds");
        assert_eq!(unit_for("scan.gb_per_s"), "GB/s");
        assert_eq!(unit_for("pit.p99_us"), "us");
        assert_eq!(unit_for("total_added.p99_ms"), "ms");
        assert_eq!(unit_for("store.bytes_per_event"), "B/event");
        assert!(is_headline("one_sub.p99_ns"));
        assert!(is_headline("total_added.p99"));
        assert!(!is_headline("warmup_iters"));
        assert!(!is_headline("samples"));
    }
}
