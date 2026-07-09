//! Replay a captured segment tree and print the outcome as JSON; with
//! `--twice`, replay again and hard-assert byte-identical digests (the
//! goal's determinism gate). Exit codes: 0 ok, 1 determinism/oracle
//! failure, 2 usage/IO.
//!
//! Usage: replay-verify --data <dir> [--kraken-depth N] [--twice]
//!        [--fail-on-crc-mismatch]

use std::path::PathBuf;
use std::process::ExitCode;

use flashbook_lob::BTreeBook;
use flashbook_proto::Registry;
use flashbook_replay::replay_books;

fn main() -> ExitCode {
    let mut data: Option<PathBuf> = None;
    let mut kraken_depth: usize = 100;
    let mut twice = false;
    let mut fail_on_crc = false;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data" => data = args.next().map(PathBuf::from),
            "--kraken-depth" => {
                kraken_depth = match args.next().and_then(|v| v.parse().ok()) {
                    Some(v) => v,
                    None => {
                        eprintln!("--kraken-depth needs an integer");
                        return ExitCode::from(2);
                    }
                }
            }
            "--twice" => twice = true,
            "--fail-on-crc-mismatch" => fail_on_crc = true,
            "--help" | "-h" => {
                eprintln!(
                    "usage: replay-verify --data <dir> [--kraken-depth N] [--twice] [--fail-on-crc-mismatch]"
                );
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown arg: {other}");
                return ExitCode::from(2);
            }
        }
    }
    let Some(data) = data else {
        eprintln!("--data <dir> required");
        return ExitCode::from(2);
    };

    let registry = Registry::builtin();
    let run = || {
        replay_books::<BTreeBook>(
            &data,
            &registry,
            |d| d.map_or_else(BTreeBook::new, BTreeBook::with_max_depth),
            Some(kraken_depth),
            |_| {},
        )
    };

    let a = match run() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("replay failed: {e}");
            return ExitCode::from(2);
        }
    };

    println!(
        "{}",
        serde_json::json!({
            "records": a.records,
            "ws_frames": a.ws_frames,
            "rest_snapshots": a.rest_snapshots,
            "notes": a.notes,
            "codec_resets": a.codec_resets,
            "events": a.events,
            "fallbacks": a.fallbacks,
            "parse_errors": a.parse_errors,
            "gaps": a.gaps,
            "torn_tails": a.torn_tails,
            "checksums_ok": a.checksums_ok,
            "checksum_mismatches": a.checksum_mismatches,
            "checksums_skipped": a.checksums_skipped,
            "event_stream_digest": format!("{:016x}", a.event_stream_digest),
            "books_digest": format!("{:016x}", a.books_digest),
            "span_mono_s": (a.last_mono_ns.saturating_sub(a.first_mono_ns)) / 1_000_000_000,
        })
    );

    if twice {
        let b = match run() {
            Ok(o) => o,
            Err(e) => {
                eprintln!("second replay failed: {e}");
                return ExitCode::from(2);
            }
        };
        if a != b {
            eprintln!(
                "DETERMINISM FAILURE: digests differ (events {:016x} vs {:016x}, books {:016x} vs {:016x})",
                a.event_stream_digest, b.event_stream_digest, a.books_digest, b.books_digest
            );
            return ExitCode::from(1);
        }
        eprintln!("determinism: OK (two replays byte-identical)");
    }

    if fail_on_crc && a.checksum_mismatches > 0 {
        eprintln!("CRC ORACLE FAILURE: {} mismatches", a.checksum_mismatches);
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}
