//! bench-store: tick-store benchmarks over an ingested segment file —
//! write throughput, full-scan throughput, point-in-time query latency,
//! and (feature `compare`) the DuckDB/SQLite/Parquet head-to-head.
//!
//! Phases (all single-threaded):
//!
//! 1. **Load (uncounted):** open the store, decode every event into memory
//!    once. All timed phases start from decoded events or the mmap'd file.
//! 2. **Write throughput** (`store_write.json`): re-encode the events
//!    through a fresh [`StoreWriter`] to a scratch file, timed append+seal,
//!    in both raw and zstd modes. Reported as events/s and logical MB/s
//!    (events * 64 B / seconds).
//! 3. **Full scan** (`store_scan.json`): [`StoreReader::scan`] over every
//!    event, 1 warmup + N measured passes. Reported as events/s plus GB/s
//!    both logical (events * 64 B / s) and physical (file bytes / s).
//! 4. **PIT** (`store_pit.json`): N seeded-random `(instrument, t)` queries
//!    (1000, or 100 with `--quick`) through [`SnapshotIndex::latest_at`] +
//!    [`pit_scan`] folded into a [`LadderBook`]; per-query latency
//!    percentiles over ALL queries (anchor misses included — they are real
//!    query outcomes) with the anchor-hit rate reported alongside.
//! 5. **Compare** (feature `compare`, `store_compare.json`): identical data
//!    loaded into DuckDB (Appender) and SQLite (single tx + index), Parquet
//!    written via DuckDB COPY; sizes, load seconds, full-scan seconds and
//!    PIT latency per backend — with the three backends' results asserted
//!    EQUAL first. Losses are reported as plainly as wins (per-metric
//!    winners are computed and published, whoever they are).
//!
//! Usage: `bench-store --store <path> [--quick] [--results-dir DIR]
//!         [--overwrite]`
//!
//! `--quick` reduces pass/query counts and marks every result file
//! non-official (smoke runs on a busy machine). Exit codes: 0 ok, 1
//! correctness failure (backend disagreement), 2 usage/IO.

use std::collections::BTreeSet;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use flashbook_bench::{Percentiles, ResultFile, write_result};
use flashbook_lob::{L2Book, LadderBook};
use flashbook_proto::Event;
use flashbook_store::pit::{SnapshotIndex, pit_scan};
use flashbook_store::segment::{StoreReader, StoreWriter};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const USAGE: &str = "usage: bench-store --store <path> [--quick] [--results-dir DIR] [--overwrite]";

/// Fixed RNG seed: PIT query sets are reproducible across runs/backends.
const PIT_SEED: u64 = 0xF1A5_B00C;
/// PIT queries, full run.
const PIT_N_FULL: usize = 1000;
/// PIT queries with `--quick`.
const PIT_N_QUICK: usize = 100;
/// PIT queries per SQL backend in the compare phase (full run).
#[cfg(feature = "compare")]
const PIT_COMPARE_FULL: usize = 200;
/// Compare-phase PIT queries with `--quick`.
#[cfg(feature = "compare")]
const PIT_COMPARE_QUICK: usize = 20;

/// Parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    /// Ingested segment file.
    store: PathBuf,
    /// Fewer passes/queries; marks results non-official.
    quick: bool,
    /// Where the `store_*.json` result files go.
    results_dir: PathBuf,
    /// Allow clobbering existing result files.
    overwrite: bool,
}

/// Parse CLI args (everything after argv[0]).
fn parse_args(args: impl Iterator<Item = String>) -> Result<Config, String> {
    let mut store: Option<PathBuf> = None;
    let mut quick = false;
    let mut results_dir = PathBuf::from("bench/results");
    let mut overwrite = false;

    let mut args = args.peekable();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--store" => store = args.next().map(PathBuf::from),
            "--quick" => quick = true,
            "--results-dir" => {
                results_dir = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or("--results-dir needs a path")?;
            }
            "--overwrite" => overwrite = true,
            "--help" | "-h" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown arg: {other}\n{USAGE}")),
        }
    }
    let store = store.ok_or_else(|| format!("--store <path> required\n{USAGE}"))?;
    Ok(Config {
        store,
        quick,
        results_dir,
        overwrite,
    })
}

/// Store knobs recorded by ingest's meta JSON; defaults when the store was
/// written by something else: (block_events 8192, zstd level 3).
fn meta_knobs(meta: &[u8]) -> (usize, i32) {
    let v: serde_json::Value = serde_json::from_slice(meta).unwrap_or(serde_json::Value::Null);
    let block_events = v["block_events"]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .filter(|&n| n > 0)
        .unwrap_or(8192);
    let zstd = v["zstd_level"]
        .as_i64()
        .and_then(|n| i32::try_from(n).ok())
        .filter(|&l| l != 0)
        .unwrap_or(3);
    (block_events, zstd)
}

/// `raw_payload_bytes` from ingest's `<store>.ingest.json` sidecar, if any.
fn raw_baseline_bytes(store: &Path) -> Option<u64> {
    let mut s = store.as_os_str().to_owned();
    s.push(".ingest.json");
    let text = std::fs::read_to_string(PathBuf::from(s)).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    v["raw_payload_bytes"].as_u64()
}

/// Arithmetic mean (0.0 for empty).
fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        0.0
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

/// Scratch directory under the OS temp dir (bench has no tempfile dep in
/// its non-dev tree); recreated empty, best-effort removed by the caller.
fn scratch_dir(tag: &str) -> Result<PathBuf, String> {
    let d = std::env::temp_dir().join(format!(
        "flashbook-bench-store-{}-{tag}",
        std::process::id()
    ));
    if d.exists() {
        std::fs::remove_dir_all(&d).map_err(|e| format!("clear scratch {}: {e}", d.display()))?;
    }
    std::fs::create_dir_all(&d).map_err(|e| format!("create scratch {}: {e}", d.display()))?;
    Ok(d)
}

/// Deterministic PIT query set: `n` random `(instrument, t_mono)` pairs
/// with `t` uniform in `[t_lo, t_hi]`, instruments uniform over the given
/// set. Same seed => same queries, on every backend and every run.
fn gen_queries(instruments: &[u32], t_lo: u64, t_hi: u64, n: usize, seed: u64) -> Vec<(u32, u64)> {
    assert!(!instruments.is_empty(), "need at least one instrument");
    assert!(t_lo <= t_hi, "bad t range");
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| {
            let inst = instruments[rng.random_range(0..instruments.len())];
            (inst, rng.random_range(t_lo..=t_hi))
        })
        .collect()
}

/// One write-throughput mode: re-encode `events` through a fresh
/// [`StoreWriter`] `passes` times; returns metrics JSON.
fn write_bench_mode(
    events: &[Event],
    block_events: usize,
    zstd: Option<i32>,
    passes: usize,
    scratch: &Path,
    name: &str,
) -> Result<serde_json::Value, String> {
    let logical_bytes = events.len() as u64 * 64;
    let mut secs: Vec<f64> = Vec::with_capacity(passes);
    let mut stored_bytes = 0u64;
    for p in 0..passes {
        let path = scratch.join(format!("rewrite-{name}-{p}.fbstore"));
        let t0 = Instant::now();
        let mut w = StoreWriter::create(&path, b"{}", block_events, zstd)
            .map_err(|e| format!("write bench create: {e}"))?;
        for e in events {
            w.append(e)
                .map_err(|e| format!("write bench append: {e}"))?;
        }
        stored_bytes = w.seal().map_err(|e| format!("write bench seal: {e}"))?;
        secs.push(t0.elapsed().as_secs_f64());
        let _ = std::fs::remove_file(&path);
    }
    let eps: Vec<f64> = secs.iter().map(|s| events.len() as f64 / s).collect();
    let mbps: Vec<f64> = secs
        .iter()
        .map(|s| logical_bytes as f64 / s / 1e6)
        .collect();
    Ok(serde_json::json!({
        "zstd_level": zstd,
        "pass_seconds": secs,
        "events_per_s": eps,
        "mean_events_per_s": mean(&eps),
        "mb_per_s_logical": mbps,
        "mean_mb_per_s_logical": mean(&mbps),
        "stored_bytes": stored_bytes,
        "bytes_per_event": stored_bytes as f64 / events.len() as f64,
    }))
}

/// One full-scan pass: touch every event (count + fold a few fields so the
/// decode cannot be optimized away). Returns (seconds, events seen).
fn scan_pass(reader: &StoreReader) -> Result<(f64, u64), String> {
    let t0 = Instant::now();
    let mut n = 0u64;
    let mut acc = 0u64;
    reader
        .scan(|e| {
            n += 1;
            acc ^= e.recv_mono_ns ^ (e.qty as u64).rotate_left(17);
        })
        .map_err(|e| format!("scan: {e}"))?;
    black_box(acc);
    Ok((t0.elapsed().as_secs_f64(), n))
}

/// PIT benchmark, ours: run every query, sample per-query latency (ns),
/// count anchor hits. Misses are timed too — they are real query outcomes.
fn pit_bench_ours(
    reader: &StoreReader,
    index: &SnapshotIndex,
    queries: &[(u32, u64)],
) -> Result<(Vec<u64>, u64), String> {
    let mut samples = Vec::with_capacity(queries.len());
    let mut hits = 0u64;
    for &(inst, t) in queries {
        let t0 = Instant::now();
        if let Some(entry) = index.latest_at(inst, t) {
            let mut book = LadderBook::new();
            pit_scan(reader, entry, t, |e| {
                book.apply(e);
            })
            .map_err(|e| format!("pit_scan({inst}, {t}): {e}"))?;
            black_box((book.best_bid(), book.best_ask()));
            hits += 1;
        }
        samples.push(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    Ok((samples, hits))
}

/// Percentiles as JSON (`null` when there were no samples).
fn percentiles_json(samples: &[u64]) -> serde_json::Value {
    Percentiles::from_samples(samples)
        .map(|p| serde_json::to_value(p).expect("percentiles serialize"))
        .unwrap_or(serde_json::Value::Null)
}

/// Name of the backend with the smallest mean — published whoever wins.
#[cfg_attr(not(feature = "compare"), allow(dead_code))]
fn winner<'a>(named_means: &[(&'a str, f64)]) -> &'a str {
    named_means
        .iter()
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .map_or("none", |(n, _)| n)
}

/// The `--quick` disclaimer appended to every note.
fn quick_note(quick: bool) -> &'static str {
    if quick {
        " QUICK RUN: reduced passes/queries on a possibly busy machine; numbers are NOT official."
    } else {
        ""
    }
}

/// The DuckDB/SQLite/Parquet head-to-head. Returns (metrics JSON, parity
/// ok). Everything runs on scratch files; sizes are measured after the
/// connections close (DuckDB checkpoints its WAL on close).
#[cfg(feature = "compare")]
#[allow(clippy::too_many_arguments)]
fn run_compare(
    reader: &StoreReader,
    index: &SnapshotIndex,
    store_bytes: u64,
    snapidx_bytes: Option<u64>,
    raw_json_bytes: Option<u64>,
    instruments: &[u32],
    t_range: (u64, u64),
    quick: bool,
) -> Result<(serde_json::Value, bool), String> {
    use flashbook_bench::compare::{
        full_scan_duckdb, full_scan_ours, full_scan_sqlite, load_duckdb, load_sqlite, pit_duckdb,
        pit_ours, pit_sqlite, write_parquet_via_duckdb,
    };

    let scratch = scratch_dir("compare")?;
    let duck_path = scratch.join("events.duckdb");
    let sqlite_path = scratch.join("events.sqlite");
    let parquet_path = scratch.join("events.parquet");
    let mut parity_ok = true;
    let mut parity_failures: Vec<String> = Vec::new();

    let (duck_load_s, duck) =
        load_duckdb(reader, &duck_path).map_err(|e| format!("duckdb load: {e:#}"))?;
    let (sqlite_load_s, sqlite) =
        load_sqlite(reader, &sqlite_path).map_err(|e| format!("sqlite load: {e:#}"))?;
    eprintln!("compare: loads done (duckdb {duck_load_s:.2}s, sqlite {sqlite_load_s:.2}s)");

    // Full scans: N runs each, identical aggregate, results asserted equal.
    let runs = if quick { 1 } else { 3 };
    let (mut ours_s, mut duck_s, mut sqlite_s) = (Vec::new(), Vec::new(), Vec::new());
    let (mut rows_ours, mut rows_duck, mut rows_sqlite) = (Vec::new(), Vec::new(), Vec::new());
    for _ in 0..runs {
        let (s, r) = full_scan_ours(reader).map_err(|e| format!("scan ours: {e:#}"))?;
        ours_s.push(s);
        rows_ours = r;
        let (s, r) = full_scan_duckdb(&duck).map_err(|e| format!("scan duckdb: {e:#}"))?;
        duck_s.push(s);
        rows_duck = r;
        let (s, r) = full_scan_sqlite(&sqlite).map_err(|e| format!("scan sqlite: {e:#}"))?;
        sqlite_s.push(s);
        rows_sqlite = r;
    }
    if rows_ours != rows_duck || rows_ours != rows_sqlite {
        parity_ok = false;
        parity_failures.push(format!(
            "full-scan rows disagree: ours={rows_ours:?} duckdb={rows_duck:?} sqlite={rows_sqlite:?}"
        ));
    }

    // PIT parity + latency on the same seeded query set for all backends.
    let n = if quick {
        PIT_COMPARE_QUICK
    } else {
        PIT_COMPARE_FULL
    };
    let queries = gen_queries(instruments, t_range.0, t_range.1, n, PIT_SEED);
    let (mut pit_ours_ns, mut pit_duck_ns, mut pit_sqlite_ns) =
        (Vec::new(), Vec::new(), Vec::new());
    let mut anchor_divergences = 0u64;
    let mut hits = 0u64;
    for &(inst, t) in &queries {
        let (s, top) = pit_ours(reader, index, inst, t).map_err(|e| format!("pit ours: {e:#}"))?;
        pit_ours_ns.push((s * 1e9) as u64);
        if top.anchor_mono.is_some() {
            hits += 1;
        }
        let d = pit_duckdb(&duck, inst, t, top.anchor_mono)
            .map_err(|e| format!("pit duckdb: {e:#}"))?;
        pit_duck_ns.push((d.seconds * 1e9) as u64);
        let q = pit_sqlite(&sqlite, inst, t, top.anchor_mono)
            .map_err(|e| format!("pit sqlite: {e:#}"))?;
        pit_sqlite_ns.push((q.seconds * 1e9) as u64);
        if d.anchor_diverged || q.anchor_diverged {
            anchor_divergences += 1;
        }
        if d.top != top || q.top != top {
            parity_ok = false;
            if parity_failures.len() < 5 {
                parity_failures.push(format!(
                    "pit({inst}, {t}) tops disagree: ours={top:?} duckdb={:?} sqlite={:?}",
                    d.top, q.top
                ));
            }
        }
    }

    let (parquet_s, parquet_bytes) =
        write_parquet_via_duckdb(&duck, &parquet_path).map_err(|e| format!("parquet: {e:#}"))?;

    // Close connections before measuring on-disk sizes.
    drop(duck);
    drop(sqlite);
    let duck_bytes = std::fs::metadata(&duck_path)
        .map_err(|e| e.to_string())?
        .len();
    let sqlite_bytes = std::fs::metadata(&sqlite_path)
        .map_err(|e| e.to_string())?
        .len();

    let scan_winner = winner(&[
        ("ours", mean(&ours_s)),
        ("duckdb", mean(&duck_s)),
        ("sqlite", mean(&sqlite_s)),
    ]);
    let pit_p50 = |ns: &[u64]| Percentiles::from_samples(ns).map_or(f64::MAX, |p| p.p50 as f64);
    let pit_winner = winner(&[
        ("ours", pit_p50(&pit_ours_ns)),
        ("duckdb", pit_p50(&pit_duck_ns)),
        ("sqlite", pit_p50(&pit_sqlite_ns)),
    ]);
    let ours_total = store_bytes + snapidx_bytes.unwrap_or(0);
    let size_winner = winner(&[
        ("ours", ours_total as f64),
        ("duckdb", duck_bytes as f64),
        ("sqlite", sqlite_bytes as f64),
        ("parquet_zstd", parquet_bytes as f64),
    ]);

    let metrics = serde_json::json!({
        "parity": {
            "full_scan_equal": rows_ours == rows_duck && rows_ours == rows_sqlite,
            "pit_tops_equal": parity_failures.iter().all(|f| !f.starts_with("pit")),
            "failures": parity_failures,
            "pit_anchor_divergences": anchor_divergences,
            "pit_anchor_hits": hits,
            "pit_queries": queries.len(),
        },
        "load_seconds": {
            "duckdb_appender": duck_load_s,
            "sqlite_tx_insert_index_analyze": sqlite_load_s,
        },
        "sizes_bytes": {
            "ours_store": store_bytes,
            "ours_snapidx": snapidx_bytes,
            "ours_total": ours_total,
            "duckdb": duck_bytes,
            "sqlite": sqlite_bytes,
            "parquet_zstd": parquet_bytes,
            "raw_json": raw_json_bytes,
        },
        "full_scan_seconds": {
            "ours": ours_s,
            "duckdb": duck_s,
            "sqlite": sqlite_s,
            "mean_ours": mean(&ours_s),
            "mean_duckdb": mean(&duck_s),
            "mean_sqlite": mean(&sqlite_s),
        },
        "pit_latency_ns": {
            "ours": percentiles_json(&pit_ours_ns),
            "duckdb": percentiles_json(&pit_duck_ns),
            "sqlite": percentiles_json(&pit_sqlite_ns),
        },
        "parquet_write_seconds": parquet_s,
        "winners": {
            "full_scan": scan_winner,
            "pit_p50": pit_winner,
            "smallest_size": size_winner,
        },
    });

    let _ = std::fs::remove_dir_all(&scratch);
    Ok((metrics, parity_ok))
}

fn main() -> ExitCode {
    let cfg = match parse_args(std::env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };

    // Phase 1: load (uncounted).
    let reader = match StoreReader::open(&cfg.store) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("open store {}: {e}", cfg.store.display());
            return ExitCode::from(2);
        }
    };
    let store_bytes = match std::fs::metadata(&cfg.store) {
        Ok(m) => m.len(),
        Err(e) => {
            eprintln!("stat store: {e}");
            return ExitCode::from(2);
        }
    };
    let mut events: Vec<Event> =
        Vec::with_capacity(usize::try_from(reader.n_events()).unwrap_or(0));
    let mut instruments: BTreeSet<u32> = BTreeSet::new();
    if let Err(e) = reader.scan(|e| {
        events.push(*e);
        instruments.insert(e.instrument);
    }) {
        eprintln!("decode store: {e}");
        return ExitCode::from(2);
    }
    if events.is_empty() {
        eprintln!("store {} has no events", cfg.store.display());
        return ExitCode::from(2);
    }
    let instruments: Vec<u32> = instruments.into_iter().collect();
    let (block_events, zstd_level) = meta_knobs(reader.meta());
    let raw_json_bytes = raw_baseline_bytes(&cfg.store);
    let t_range = (
        reader.blocks().first().map_or(0, |b| b.min_recv_mono),
        reader.blocks().last().map_or(0, |b| b.max_recv_mono),
    );
    eprintln!(
        "store: {} events, {} bytes, {} instruments, blocks {}, knobs ({block_events}, zstd {zstd_level})",
        events.len(),
        store_bytes,
        instruments.len(),
        reader.n_blocks(),
    );

    let base_config = serde_json::json!({
        "store": cfg.store.display().to_string(),
        "quick": cfg.quick,
        "events": events.len(),
        "store_bytes": store_bytes,
        "block_events": block_events,
        "zstd_level": zstd_level,
        "threads": 1,
    });
    let emit = |name: &str, metrics: serde_json::Value, notes: &str| -> bool {
        let r = ResultFile::new(name, base_config.clone(), metrics, notes);
        match write_result(&cfg.results_dir, &r, cfg.overwrite) {
            Ok(path) => {
                println!("{}", path.display());
                true
            }
            Err(e) => {
                eprintln!("writing {name} failed: {e}");
                false
            }
        }
    };

    // Phase 2: write throughput (raw and zstd modes).
    let write_passes = if cfg.quick { 1 } else { 3 };
    let scratch = match scratch_dir("write") {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let write_metrics = (|| -> Result<serde_json::Value, String> {
        let raw = write_bench_mode(&events, block_events, None, write_passes, &scratch, "raw")?;
        let z = write_bench_mode(
            &events,
            block_events,
            Some(zstd_level),
            write_passes,
            &scratch,
            "zstd",
        )?;
        Ok(serde_json::json!({ "raw": raw, "zstd": z }))
    })();
    let _ = std::fs::remove_dir_all(&scratch);
    match write_metrics {
        Ok(m) => {
            let notes = format!(
                "Re-encoding the store's {} decoded events through StoreWriter (append+seal, \
                 block_events={block_events}) to a scratch file, {write_passes} pass(es) per \
                 mode. MB/s is logical (events * 64 B / s); encode cost only, source events \
                 pre-decoded in memory.{}",
                events.len(),
                quick_note(cfg.quick)
            );
            if !emit("store_write", m, &notes) {
                return ExitCode::from(2);
            }
        }
        Err(e) => {
            eprintln!("write bench failed: {e}");
            return ExitCode::from(2);
        }
    }

    // Phase 3: full scan.
    let scan_passes = if cfg.quick { 2 } else { 5 };
    let scan_result = (|| -> Result<serde_json::Value, String> {
        let _ = scan_pass(&reader)?; // warmup (page cache, branch warm)
        let mut secs = Vec::with_capacity(scan_passes);
        let mut n_seen = 0u64;
        for _ in 0..scan_passes {
            let (s, n) = scan_pass(&reader)?;
            secs.push(s);
            n_seen = n;
        }
        let logical = n_seen as f64 * 64.0;
        let eps: Vec<f64> = secs.iter().map(|s| n_seen as f64 / s).collect();
        let gbl: Vec<f64> = secs.iter().map(|s| logical / s / 1e9).collect();
        let gbp: Vec<f64> = secs.iter().map(|s| store_bytes as f64 / s / 1e9).collect();
        Ok(serde_json::json!({
            "events": n_seen,
            "pass_seconds": secs,
            "events_per_s": eps,
            "mean_events_per_s": mean(&eps),
            "gb_per_s_logical": gbl,
            "mean_gb_per_s_logical": mean(&gbl),
            "gb_per_s_physical": gbp,
            "mean_gb_per_s_physical": mean(&gbp),
        }))
    })();
    match scan_result {
        Ok(m) => {
            let notes = format!(
                "Sequential decode of every block via StoreReader::scan (mmap, per-block CRC + \
                 column decode), 1 warmup + {scan_passes} measured passes. Logical GB/s = events \
                 * 64 B / s; physical GB/s = file bytes / s.{}",
                quick_note(cfg.quick)
            );
            if !emit("store_scan", m, &notes) {
                return ExitCode::from(2);
            }
        }
        Err(e) => {
            eprintln!("scan bench failed: {e}");
            return ExitCode::from(2);
        }
    }

    // Phase 4: PIT queries.
    let mut snapidx_path = cfg.store.as_os_str().to_owned();
    snapidx_path.push(".snapidx");
    let snapidx_path = PathBuf::from(snapidx_path);
    let snapidx_bytes = std::fs::metadata(&snapidx_path).ok().map(|m| m.len());
    let (index, index_source) = match SnapshotIndex::load(&snapidx_path) {
        Ok(i) => (i, "sidecar"),
        Err(e) => {
            eprintln!("snapidx sidecar unusable ({e}); rebuilding from store");
            match SnapshotIndex::build(&reader) {
                Ok(i) => (i, "rebuilt"),
                Err(e) => {
                    eprintln!("snapshot index build failed: {e}");
                    return ExitCode::from(2);
                }
            }
        }
    };
    let pit_n = if cfg.quick { PIT_N_QUICK } else { PIT_N_FULL };
    let queries = gen_queries(&instruments, t_range.0, t_range.1, pit_n, PIT_SEED);
    match pit_bench_ours(&reader, &index, &queries) {
        Ok((samples, hits)) => {
            let m = serde_json::json!({
                "queries": queries.len(),
                "anchor_hits": hits,
                "anchor_hit_rate": hits as f64 / queries.len() as f64,
                "snapshots_indexed": index.len(),
                "index_source": index_source,
                "latency_ns": percentiles_json(&samples),
            });
            let notes = format!(
                "{pit_n} seeded-random (instrument, t) queries (seed {PIT_SEED:#x}), t uniform \
                 over the store's recv_mono span, instruments uniform over the {} seen. Each \
                 query = SnapshotIndex::latest_at + pit_scan folded into an unbounded \
                 LadderBook (top-of-book out). Percentiles cover ALL queries; anchor misses \
                 (no complete snapshot at or before t) are timed as the near-free lookups they \
                 are, and the hit rate is published alongside.{}",
                instruments.len(),
                quick_note(cfg.quick)
            );
            if !emit("store_pit", m, &notes) {
                return ExitCode::from(2);
            }
        }
        Err(e) => {
            eprintln!("pit bench failed: {e}");
            return ExitCode::from(2);
        }
    }

    // Phase 5: the head-to-head (feature "compare").
    #[cfg(feature = "compare")]
    {
        match run_compare(
            &reader,
            &index,
            store_bytes,
            snapidx_bytes,
            raw_json_bytes,
            &instruments,
            t_range,
            cfg.quick,
        ) {
            Ok((m, parity_ok)) => {
                let notes = format!(
                    "Identical events loaded into DuckDB (Appender, no index) and SQLite \
                     (single tx + prepared INSERT; load-only pragmas journal_mode=MEMORY, \
                     synchronous=OFF; then CREATE INDEX idx_inst_mono + ANALYZE, charged to \
                     load). Parquet via DuckDB COPY (FORMAT PARQUET, COMPRESSION ZSTD). \
                     Full-scan aggregate and PIT top-of-book asserted EQUAL across backends \
                     before any timing is quoted; SQL PIT folds from ours' validated anchor \
                     when the naive kind=4 anchor picks an incomplete snapshot (divergences \
                     published). Losses are reported as plainly as wins — see winners.{}",
                    quick_note(cfg.quick)
                );
                if !emit("store_compare", m, &notes) {
                    return ExitCode::from(2);
                }
                if !parity_ok {
                    eprintln!("BACKEND PARITY FAILURE: see store_compare.json parity.failures");
                    return ExitCode::from(1);
                }
            }
            Err(e) => {
                eprintln!("compare failed: {e}");
                return ExitCode::from(2);
            }
        }
    }
    #[cfg(not(feature = "compare"))]
    {
        let _ = (snapidx_bytes, raw_json_bytes);
        eprintln!("compare feature off: store_compare.json skipped");
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_full_defaults_and_errors() {
        let full = parse_args(
            [
                "--store",
                "x.fbstore",
                "--quick",
                "--results-dir",
                "bench/results/tmp",
                "--overwrite",
            ]
            .iter()
            .map(ToString::to_string),
        )
        .unwrap();
        assert_eq!(
            full,
            Config {
                store: PathBuf::from("x.fbstore"),
                quick: true,
                results_dir: PathBuf::from("bench/results/tmp"),
                overwrite: true,
            }
        );
        let defaults = parse_args(["--store", "s"].iter().map(ToString::to_string)).unwrap();
        assert!(!defaults.quick && !defaults.overwrite);
        assert_eq!(defaults.results_dir, PathBuf::from("bench/results"));
        assert!(parse_args(std::iter::empty()).is_err(), "--store required");
        assert!(parse_args(["--nope".to_string()].into_iter()).is_err());
    }

    #[test]
    fn gen_queries_is_seeded_and_in_range() {
        let insts = [3u32, 7, 9];
        let a = gen_queries(&insts, 100, 200, 50, 42);
        let b = gen_queries(&insts, 100, 200, 50, 42);
        assert_eq!(a, b, "same seed => same queries");
        assert_eq!(a.len(), 50);
        assert!(
            a.iter()
                .all(|(i, t)| insts.contains(i) && (100..=200).contains(t))
        );
        let c = gen_queries(&insts, 100, 200, 50, 43);
        assert_ne!(a, c, "different seed => different queries");
        // Degenerate time range still works.
        let d = gen_queries(&insts, 5, 5, 3, 1);
        assert!(d.iter().all(|&(_, t)| t == 5));
    }

    #[test]
    fn meta_knobs_parses_ingest_meta_and_defaults() {
        let (b, z) = meta_knobs(br#"{"block_events":1024,"zstd_level":7}"#);
        assert_eq!((b, z), (1024, 7));
        // zstd_level null (raw store) and missing fields fall back sanely.
        let (b, z) = meta_knobs(br#"{"block_events":512,"zstd_level":null}"#);
        assert_eq!((b, z), (512, 3));
        let (b, z) = meta_knobs(b"not json");
        assert_eq!((b, z), (8192, 3));
        let (b, z) = meta_knobs(br#"{"block_events":0}"#);
        assert_eq!((b, z), (8192, 3), "zero block_events rejected");
    }

    #[test]
    fn mean_and_winner_helpers() {
        assert!((mean(&[1.0, 2.0, 3.0]) - 2.0).abs() < 1e-12);
        assert_eq!(mean(&[]), 0.0);
        assert_eq!(winner(&[("a", 2.0), ("b", 1.0), ("c", 3.0)]), "b");
        assert_eq!(winner(&[]), "none");
    }
}
