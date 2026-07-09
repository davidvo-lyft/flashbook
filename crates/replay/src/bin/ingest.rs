//! ingest: replay a raw capture into one columnar store segment plus its
//! point-in-time snapshot-index sidecar.
//!
//! Pipeline: raw records -> venue codecs -> normalized events (the exact
//! deterministic `replay_books` pass, Kraken CRC oracle live) -> every event
//! appended to a [`StoreWriter`] -> `seal()` -> [`SnapshotIndex::build`] over
//! the sealed file, saved at `<out>.snapidx`.
//!
//! Outputs:
//! - `<out>`: the sealed segment file (meta JSON records the knobs).
//! - `<out>.snapidx`: the snapshot-index sidecar.
//! - `<out>.ingest.json`: the same stats JSON printed to stdout (bench-store
//!   reads `raw_payload_bytes` from here as the raw-JSON size baseline).
//!
//! Determinism: replay is deterministic, block/zstd encoding is
//! deterministic, and the meta JSON has sorted keys — running ingest twice
//! with the same capture and knobs produces byte-identical store files
//! (asserted by a test below; also verifiable with `shasum`).
//!
//! Usage: `ingest --data <raw dir> --out <store path> [--block-events 8192]
//!         [--zstd 3] [--kraken-depth 100]` (`--zstd 0` stores raw blocks)
//!
//! Exit codes: 0 ok, 1 correctness failure (Kraken checksum mismatches),
//! 2 usage/IO/replay/store error.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use flashbook_lob::BTreeBook;
use flashbook_proto::Registry;
use flashbook_replay::replay_books;
use flashbook_store::pit::SnapshotIndex;
use flashbook_store::segment::{SegmentError, StoreReader, StoreWriter};

const USAGE: &str = "usage: ingest --data <raw dir> --out <store path> \
     [--block-events 8192] [--zstd 3] [--kraken-depth 100]";

/// Parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    /// Raw capture directory (`replay_books` root).
    data: PathBuf,
    /// Output segment path (must not exist; segments are immutable).
    out: PathBuf,
    /// Events per block.
    block_events: usize,
    /// zstd level; `None` stores raw column bytes (CLI `--zstd 0`).
    zstd: Option<i32>,
    /// Kraken venue depth cap (100 for smoke/soak captures).
    kraken_depth: usize,
}

/// Ingest counters for the stats JSON line.
#[derive(Debug, Clone, PartialEq)]
struct IngestStats {
    events: u64,
    raw_payload_bytes: u64,
    store_bytes: u64,
    snapidx_bytes: u64,
    checksums_ok: u64,
    checksum_mismatches: u64,
    parse_errors: u64,
    span_s: f64,
}

/// Round a derived float to 6 decimals for serialization. The raw integer
/// counters are the evidence; derived ratios are display-grade, and fixing
/// their precision makes the JSON stable across platforms (CI caught a
/// 1-ULP divergence on x86_64 in the full-precision form).
fn round6(v: f64) -> f64 {
    (v * 1e6).round() / 1e6
}

impl IngestStats {
    /// Stats as the (sorted-key, hence deterministic) JSON line.
    fn to_json(&self) -> serde_json::Value {
        let bytes_per_event = if self.events == 0 {
            0.0
        } else {
            round6(self.store_bytes as f64 / self.events as f64)
        };
        let ratio_vs_raw_json = if self.store_bytes == 0 {
            0.0
        } else {
            round6(self.raw_payload_bytes as f64 / self.store_bytes as f64)
        };
        serde_json::json!({
            "events": self.events,
            "raw_payload_bytes": self.raw_payload_bytes,
            "store_bytes": self.store_bytes,
            "snapidx_bytes": self.snapidx_bytes,
            "bytes_per_event": bytes_per_event,
            "ratio_vs_raw_json": ratio_vs_raw_json,
            "checksums_ok": self.checksums_ok,
            "checksum_mismatches": self.checksum_mismatches,
            "parse_errors": self.parse_errors,
            "span_s": round6(self.span_s),
        })
    }
}

/// Parse CLI args (everything after argv[0]). Pure; returns a usage error
/// string on bad input.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Config, String> {
    let mut data: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut block_events: usize = 8192;
    let mut zstd_level: i32 = 3;
    let mut kraken_depth: usize = 100;

    let mut args = args.peekable();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data" => data = args.next().map(PathBuf::from),
            "--out" => out = args.next().map(PathBuf::from),
            "--block-events" => {
                block_events = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or("--block-events needs an integer")?;
            }
            "--zstd" => {
                zstd_level = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or("--zstd needs an integer level (0 = raw)")?;
            }
            "--kraken-depth" => {
                kraken_depth = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or("--kraken-depth needs an integer")?;
            }
            "--help" | "-h" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown arg: {other}\n{USAGE}")),
        }
    }
    let data = data.ok_or_else(|| format!("--data <dir> required\n{USAGE}"))?;
    let out = out.ok_or_else(|| format!("--out <path> required\n{USAGE}"))?;
    Ok(Config {
        data,
        out,
        block_events,
        zstd: if zstd_level == 0 {
            None
        } else {
            Some(zstd_level)
        },
        kraken_depth,
    })
}

/// `<out>.snapidx`: the snapshot-index sidecar path for a store file.
fn snapidx_path(out: &Path) -> PathBuf {
    let mut s = out.as_os_str().to_owned();
    s.push(".snapidx");
    PathBuf::from(s)
}

/// `<out>.ingest.json`: the ingest-stats sidecar path for a store file.
fn stats_path(out: &Path) -> PathBuf {
    let mut s = out.as_os_str().to_owned();
    s.push(".ingest.json");
    PathBuf::from(s)
}

/// Book constructor for `replay_books` (BTreeBook: the measured winner, D-014).
fn make_book(depth: Option<usize>) -> BTreeBook {
    depth.map_or_else(BTreeBook::new, BTreeBook::with_max_depth)
}

/// Run one ingest: replay `cfg.data`, write the segment, seal, build and
/// save the snapshot index, write the stats sidecar. Hard errors (IO,
/// replay, out-of-order appends) come back as `Err`; correctness counters
/// (checksum mismatches, parse errors) are reported in the stats for the
/// caller to gate on.
fn run(cfg: &Config) -> Result<IngestStats, String> {
    let registry = Registry::builtin();
    // Sorted keys (serde_json maps are BTreeMaps) => deterministic meta
    // bytes => byte-identical store files across runs.
    let meta = serde_json::json!({
        "source": cfg.data.display().to_string(),
        "kraken_depth": cfg.kraken_depth,
        "block_events": cfg.block_events,
        "zstd_level": cfg.zstd,
        "tool": "ingest",
    });
    let meta_bytes = serde_json::to_vec(&meta).map_err(|e| format!("meta encode: {e}"))?;

    if let Some(parent) = cfg.out.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| format!("create out dir: {e}"))?;
    }
    let mut writer = StoreWriter::create(&cfg.out, &meta_bytes, cfg.block_events, cfg.zstd)
        .map_err(|e| format!("create store {}: {e}", cfg.out.display()))?;

    let mut write_err: Option<SegmentError> = None;
    let outcome = replay_books::<BTreeBook>(
        &cfg.data,
        &registry,
        make_book,
        Some(cfg.kraken_depth),
        |ev| {
            if write_err.is_none()
                && let Err(e) = writer.append(ev)
            {
                write_err = Some(e);
            }
        },
    )
    .map_err(|e| format!("replay failed: {e}"))?;
    if let Some(e) = write_err {
        return Err(format!("store append failed: {e}"));
    }
    if outcome.events == 0 {
        return Err(format!("no events in corpus at {}", cfg.data.display()));
    }

    let store_bytes = writer.seal().map_err(|e| format!("seal failed: {e}"))?;

    let reader = StoreReader::open(&cfg.out).map_err(|e| format!("reopen sealed store: {e}"))?;
    if reader.n_events() != outcome.events {
        return Err(format!(
            "event count mismatch after seal: replayed {}, stored {}",
            outcome.events,
            reader.n_events()
        ));
    }
    let index = SnapshotIndex::build(&reader).map_err(|e| format!("snapshot index: {e}"))?;
    let sidecar = snapidx_path(&cfg.out);
    index
        .save(&sidecar)
        .map_err(|e| format!("save snapidx: {e}"))?;
    let snapidx_bytes = std::fs::metadata(&sidecar)
        .map_err(|e| format!("stat snapidx: {e}"))?
        .len();

    let stats = IngestStats {
        events: outcome.events,
        raw_payload_bytes: outcome.raw_payload_bytes,
        store_bytes,
        snapidx_bytes,
        checksums_ok: outcome.checksums_ok,
        checksum_mismatches: outcome.checksum_mismatches,
        parse_errors: outcome.parse_errors,
        span_s: outcome.last_mono_ns.saturating_sub(outcome.first_mono_ns) as f64 / 1_000_000_000.0,
    };
    let stats_json =
        serde_json::to_string(&stats.to_json()).map_err(|e| format!("stats encode: {e}"))?;
    std::fs::write(stats_path(&cfg.out), format!("{stats_json}\n"))
        .map_err(|e| format!("write stats sidecar: {e}"))?;
    Ok(stats)
}

fn main() -> ExitCode {
    let cfg = match parse_args(std::env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let stats = match run(&cfg) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ingest failed: {e}");
            return ExitCode::from(2);
        }
    };
    println!("{}", stats.to_json());
    if stats.checksum_mismatches > 0 {
        eprintln!(
            "CRC ORACLE FAILURE: {} checksum mismatches",
            stats.checksum_mismatches
        );
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use flashbook_proto::rawlog::{RawLogWriter, rkind};

    fn kraken_fixture_lines(n: usize) -> Vec<String> {
        let raw = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../feed/fixtures/kraken/live-btc-eth.ndjson"
        ))
        .expect("kraken fixture present");
        raw.lines().take(n).map(ToString::to_string).collect()
    }

    /// Synthetic single-venue capture from real Kraken fixture lines with
    /// fabricated strictly-increasing receive timestamps.
    fn synth_capture(root: &Path) {
        let vdir = root.join("kraken");
        std::fs::create_dir_all(&vdir).unwrap();
        let mut w =
            RawLogWriter::create(&vdir.join("kraken-1000.fbraw"), 3, 1000, 1000, b"{}").unwrap();
        w.append(rkind::NOTE, 1, 1, br#"{"event":"connect","attempt":0}"#)
            .unwrap();
        for (i, l) in kraken_fixture_lines(800).iter().enumerate() {
            let t = 10 + i as u64 * 10;
            w.append(rkind::WS_TEXT, t, t + 1, l.as_bytes()).unwrap();
        }
        w.finish().unwrap();
    }

    fn cfg(root: &Path, out: &Path, zstd: Option<i32>) -> Config {
        Config {
            data: root.to_path_buf(),
            out: out.to_path_buf(),
            block_events: 512,
            zstd,
            kraken_depth: 10, // fixture was captured at depth 10
        }
    }

    #[test]
    fn parse_args_full_defaults_and_errors() {
        let full = parse_args(
            [
                "--data",
                "data/smoke",
                "--out",
                "tmp/x.fbstore",
                "--block-events",
                "1024",
                "--zstd",
                "0",
                "--kraken-depth",
                "25",
            ]
            .iter()
            .map(ToString::to_string),
        )
        .unwrap();
        assert_eq!(
            full,
            Config {
                data: PathBuf::from("data/smoke"),
                out: PathBuf::from("tmp/x.fbstore"),
                block_events: 1024,
                zstd: None,
                kraken_depth: 25,
            }
        );

        let defaults = parse_args(
            ["--data", "d", "--out", "o"]
                .iter()
                .map(ToString::to_string),
        )
        .unwrap();
        assert_eq!(defaults.block_events, 8192);
        assert_eq!(defaults.zstd, Some(3));
        assert_eq!(defaults.kraken_depth, 100);

        assert!(parse_args(std::iter::empty()).is_err(), "--data required");
        assert!(
            parse_args(["--data", "d"].iter().map(ToString::to_string)).is_err(),
            "--out required"
        );
        assert!(
            parse_args(["--bogus".to_string()].into_iter()).is_err(),
            "unknown arg rejected"
        );
        assert!(
            parse_args(
                ["--data", "d", "--out", "o", "--zstd", "nope"]
                    .iter()
                    .map(ToString::to_string)
            )
            .is_err(),
            "non-integer zstd rejected"
        );
    }

    #[test]
    fn ingest_twice_is_byte_identical_and_store_verifies() {
        let dir = tempfile::tempdir().unwrap();
        synth_capture(dir.path());
        let out_a = dir.path().join("a.fbstore");
        let out_b = dir.path().join("b.fbstore");

        let sa = run(&cfg(dir.path(), &out_a, Some(3))).unwrap();
        let sb = run(&cfg(dir.path(), &out_b, Some(3))).unwrap();
        assert_eq!(sa, sb, "stats must match across runs");
        assert!(sa.events > 500, "{sa:?}");
        assert_eq!(sa.parse_errors, 0, "{sa:?}");
        assert_eq!(sa.checksum_mismatches, 0, "{sa:?}");
        assert!(sa.checksums_ok > 100, "oracle ran: {sa:?}");
        assert!(
            sa.raw_payload_bytes > sa.store_bytes,
            "columnar+zstd must beat raw JSON: {sa:?}"
        );

        // The determinism gate: byte-identical store files and sidecars.
        let bytes_a = std::fs::read(&out_a).unwrap();
        let bytes_b = std::fs::read(&out_b).unwrap();
        assert_eq!(bytes_a, bytes_b, "store files must be byte-identical");
        assert_eq!(
            std::fs::read(snapidx_path(&out_a)).unwrap(),
            std::fs::read(snapidx_path(&out_b)).unwrap(),
            "snapidx sidecars must be byte-identical"
        );

        // Sealed store verifies fully and matches the reported counters.
        let reader = StoreReader::open(&out_a).unwrap();
        assert!(reader.sealed());
        assert_eq!(reader.verify().unwrap(), sa.events);
        assert_eq!(std::fs::metadata(&out_a).unwrap().len(), sa.store_bytes);

        // The sidecar loads and matches a fresh rebuild.
        let idx = SnapshotIndex::load(&snapidx_path(&out_a)).unwrap();
        assert_eq!(idx, SnapshotIndex::build(&reader).unwrap());
        assert!(!idx.is_empty(), "fixture contains snapshots");

        // Stats sidecar holds the same JSON the binary prints.
        let side: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(stats_path(&out_a)).unwrap()).unwrap();
        assert_eq!(side, sa.to_json());
    }

    #[test]
    fn zstd_store_is_smaller_than_raw_store() {
        let dir = tempfile::tempdir().unwrap();
        synth_capture(dir.path());
        let out_raw = dir.path().join("raw.fbstore");
        let out_z = dir.path().join("z.fbstore");
        let s_raw = run(&cfg(dir.path(), &out_raw, None)).unwrap();
        let s_z = run(&cfg(dir.path(), &out_z, Some(3))).unwrap();
        assert_eq!(s_raw.events, s_z.events);
        assert!(
            s_z.store_bytes < s_raw.store_bytes,
            "zstd {} !< raw {}",
            s_z.store_bytes,
            s_raw.store_bytes
        );
        // Both decode to the same events.
        let mut a = Vec::new();
        let mut b = Vec::new();
        StoreReader::open(&out_raw)
            .unwrap()
            .scan(|e| a.push(*e))
            .unwrap();
        StoreReader::open(&out_z)
            .unwrap()
            .scan(|e| b.push(*e))
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn refuses_existing_out_file() {
        let dir = tempfile::tempdir().unwrap();
        synth_capture(dir.path());
        let out = dir.path().join("x.fbstore");
        std::fs::write(&out, b"already here").unwrap();
        let err = run(&cfg(dir.path(), &out, Some(3))).unwrap_err();
        assert!(err.contains("create store"), "{err}");
    }
}
