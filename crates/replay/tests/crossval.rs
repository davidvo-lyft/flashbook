//! REST cross-validation tests: replaying a captured REST snapshot for a
//! SYNCED Coinbase/Binance book scores the live reconstructed book against
//! the REST body's top-10 (both sides) *before* the snapshot is applied.
//!
//! Synthetic captures pin the scoring math exactly (100% on identity, ~50%
//! on a half-mutated book, skipped while unsynced, Kraken never scored);
//! the data/smoke replay confirms the mechanism on a real capture. The
//! check is statistical — REST bodies are timing-skewed vs the WS stream —
//! so real-world overlap is expected to be high but not 100%.

use std::path::Path;

use flashbook_feed::conn::rest_envelope;
use flashbook_lob::LadderBook;
use flashbook_proto::Registry;
use flashbook_proto::rawlog::{RawLogWriter, rkind};
use flashbook_replay::{ReplayOutcome, replay_books};

/// Instrument ids from `Registry::builtin()`.
const CB_BTC: u32 = 1; // Coinbase BTC-USD
const BN_BTC: u32 = 6; // Binance BTCUSDT
const KR_BTC: u32 = 11; // Kraken BTC/USD

/// One price level: price/qty as decimal strings (venue wire format).
type Level = (String, String);

/// Ten bid levels descending from 100.0 and ten ask levels ascending from
/// 100.1, with a per-level qty derived from `qty_salt` so tests can mutate
/// a controlled subset.
fn ten_levels(qty_salt: &dyn Fn(usize) -> String) -> (Vec<Level>, Vec<Level>) {
    let bids = (0..10)
        .map(|i| (format!("{:.2}", 100.0 - 0.01 * i as f64), qty_salt(i)))
        .collect();
    let asks = (0..10)
        .map(|i| (format!("{:.2}", 100.1 + 0.01 * i as f64), qty_salt(i)))
        .collect();
    (bids, asks)
}

/// Coinbase `/products/<id>/book?level=2` body: `[price, size, num_orders]`
/// string-string-number triples plus the `sequence` the codec anchors on.
fn coinbase_body(bids: &[Level], asks: &[Level], sequence: u64) -> Vec<u8> {
    let side = |levels: &[Level]| {
        levels
            .iter()
            .map(|(p, q)| format!("[\"{p}\",\"{q}\",1]"))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "{{\"bids\":[{}],\"asks\":[{}],\"sequence\":{sequence},\
         \"auction_mode\":false,\"time\":\"2026-07-08T00:00:00.000000Z\"}}",
        side(bids),
        side(asks)
    )
    .into_bytes()
}

/// Binance `/api/v3/depth` body: `[price, qty]` string pairs.
fn binance_body(bids: &[Level], asks: &[Level], last_update_id: u64) -> Vec<u8> {
    let side = |levels: &[Level]| {
        levels
            .iter()
            .map(|(p, q)| format!("[\"{p}\",\"{q}\"]"))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "{{\"lastUpdateId\":{last_update_id},\"bids\":[{}],\"asks\":[{}]}}",
        side(bids),
        side(asks)
    )
    .into_bytes()
}

/// Write a one-venue synthetic capture: a connect NOTE followed by REST
/// snapshot records (fabricated, strictly ordered receive timestamps).
fn synth_rest_capture(root: &Path, venue: &str, venue_id: u8, instrument: u32, bodies: &[Vec<u8>]) {
    let vdir = root.join(venue);
    std::fs::create_dir_all(&vdir).unwrap();
    let path = vdir.join(format!("{venue}-1000.fbraw"));
    let mut w = RawLogWriter::create(&path, venue_id, 1000, 1000, b"{}").unwrap();
    w.append(rkind::NOTE, 1, 1, br#"{"event":"connect","attempt":0}"#)
        .unwrap();
    for (i, body) in bodies.iter().enumerate() {
        let env = rest_envelope(instrument, "SYM", "https://example.test/book", body);
        let t = 10 + i as u64 * 10;
        w.append(rkind::REST_SNAPSHOT, t, t + 1, &env).unwrap();
    }
    w.finish().unwrap();
}

fn replay(root: &Path) -> ReplayOutcome {
    let registry = Registry::builtin();
    replay_books::<LadderBook>(
        root,
        &registry,
        |d| d.map_or_else(LadderBook::new, LadderBook::with_max_depth),
        Some(100),
        |_| {},
    )
    .unwrap()
}

#[test]
fn coinbase_exact_match_scores_100_percent() {
    let dir = tempfile::tempdir().unwrap();
    let (bids, asks) = ten_levels(&|i| format!("1.{i}"));
    // First snapshot syncs the book (skipped: nothing to validate against);
    // the identical second snapshot must score a perfect overlap.
    let bodies = vec![
        coinbase_body(&bids, &asks, 500),
        coinbase_body(&bids, &asks, 501),
    ];
    synth_rest_capture(dir.path(), "coinbase", 1, CB_BTC, &bodies);

    let out = replay(dir.path());
    assert_eq!(out.parse_errors, 0, "{out:?}");
    assert_eq!(out.crossval_snapshots, 2, "{out:?}");
    assert_eq!(out.crossval_scored, 1, "{out:?}");
    assert_eq!(out.crossval_top10_overlap_p50, 100, "{out:?}");
    assert_eq!(out.crossval_top10_overlap_p90, 100, "{out:?}");
    assert_eq!(out.crossval_worst_overlap, 100, "{out:?}");
    assert_eq!(out.crossval_price_overlap_p50, 100, "{out:?}");
}

#[test]
fn half_mutated_qtys_score_50_percent_exact_100_percent_price() {
    let dir = tempfile::tempdir().unwrap();
    let (bids_a, asks_a) = ten_levels(&|i| format!("1.{i}"));
    // Same prices, but qty changed on 5 of 10 levels per side: exact
    // (price, qty) overlap 50%, price-only overlap still 100%.
    let mutated = |i: usize| {
        if i.is_multiple_of(2) {
            format!("9.{i}")
        } else {
            format!("1.{i}")
        }
    };
    let (bids_b, asks_b) = ten_levels(&mutated);
    let bodies = vec![
        coinbase_body(&bids_a, &asks_a, 500),
        coinbase_body(&bids_b, &asks_b, 501),
    ];
    synth_rest_capture(dir.path(), "coinbase", 1, CB_BTC, &bodies);

    let out = replay(dir.path());
    assert_eq!(out.crossval_scored, 1, "{out:?}");
    assert_eq!(out.crossval_top10_overlap_p50, 50, "{out:?}");
    assert_eq!(out.crossval_worst_overlap, 50, "{out:?}");
    assert_eq!(out.crossval_price_overlap_p50, 100, "{out:?}");
    assert_eq!(out.crossval_price_overlap_p90, 100, "{out:?}");

    // The crossval stats are part of the determinism contract: a second
    // replay of the same capture must reproduce them exactly.
    assert_eq!(out, replay(dir.path()));
}

#[test]
fn unsynced_book_is_counted_but_not_scored() {
    let dir = tempfile::tempdir().unwrap();
    let (bids, asks) = ten_levels(&|i| format!("1.{i}"));
    // Only the syncing snapshot: eligible, but the book is unsynced when it
    // arrives, so nothing is scored and the percentiles stay 0.
    synth_rest_capture(
        dir.path(),
        "coinbase",
        1,
        CB_BTC,
        &[coinbase_body(&bids, &asks, 500)],
    );

    let out = replay(dir.path());
    assert_eq!(out.crossval_snapshots, 1, "{out:?}");
    assert_eq!(out.crossval_scored, 0, "{out:?}");
    assert_eq!(out.crossval_top10_overlap_p50, 0, "{out:?}");
    assert_eq!(out.crossval_top10_overlap_p90, 0, "{out:?}");
    assert_eq!(out.crossval_worst_overlap, 0, "{out:?}");
}

#[test]
fn binance_rest_snapshots_are_scored_too() {
    let dir = tempfile::tempdir().unwrap();
    let (bids, asks) = ten_levels(&|i| format!("0.{i}1"));
    let bodies = vec![
        binance_body(&bids, &asks, 700),
        binance_body(&bids, &asks, 701),
    ];
    synth_rest_capture(dir.path(), "binance", 2, BN_BTC, &bodies);

    let out = replay(dir.path());
    assert_eq!(out.parse_errors, 0, "{out:?}");
    assert_eq!(out.crossval_snapshots, 2, "{out:?}");
    assert_eq!(out.crossval_scored, 1, "{out:?}");
    assert_eq!(out.crossval_top10_overlap_p50, 100, "{out:?}");
    assert_eq!(out.crossval_worst_overlap, 100, "{out:?}");
}

#[test]
fn kraken_rest_records_are_never_cross_validated() {
    let dir = tempfile::tempdir().unwrap();
    let (bids, asks) = ten_levels(&|i| format!("1.{i}"));
    // A (hypothetical) Kraken REST record: kraken books are checked by the
    // in-band CRC32 oracle, so the crossval counters must not move even
    // though the record itself is consumed.
    let bodies = vec![
        binance_body(&bids, &asks, 700),
        binance_body(&bids, &asks, 701),
    ];
    synth_rest_capture(dir.path(), "kraken", 3, KR_BTC, &bodies);

    let out = replay(dir.path());
    assert_eq!(out.rest_snapshots, 2, "{out:?}");
    assert_eq!(out.crossval_snapshots, 0, "{out:?}");
    assert_eq!(out.crossval_scored, 0, "{out:?}");
}

#[test]
fn smoke_capture_crossval_is_plausible() {
    // Real captured data (committed): the mechanism must run end-to-end.
    // The smoke capture is short (~3 min), so its REST snapshots are mostly
    // the on-connect syncing fetches — counted, but only scoreable when a
    // book was already synced. Loose assertions only (statistical check);
    // the printed numbers feed the run report.
    let root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../data/smoke"));
    let out = replay(root);
    println!(
        "smoke crossval: snapshots={} scored={} exact p50={} p90={} worst={} price p50={} p90={}",
        out.crossval_snapshots,
        out.crossval_scored,
        out.crossval_top10_overlap_p50,
        out.crossval_top10_overlap_p90,
        out.crossval_worst_overlap,
        out.crossval_price_overlap_p50,
        out.crossval_price_overlap_p90,
    );
    assert!(out.crossval_snapshots > 0, "{out:?}");
    assert!(out.crossval_scored <= out.crossval_snapshots, "{out:?}");
    if out.crossval_scored > 0 {
        // The smoke capture yields a single scored sample (Coinbase BTC-USD
        // at the first 3-min staggered refresh): a "percentile" of n=1.
        // Coinbase's REST book lags the WS stream by seconds and BTC's
        // penny-dense top-10 churns constantly, so the exact (price, qty)
        // overlap sits well below 100% (observed 30%) while the price-only
        // overlap stays majority (observed 60%). Assert the loose floor
        // that separates "timing skew on the same book" from "different
        // book entirely" (~0%); the full-corpus numbers come from
        // replay-verify over longer captures with many refresh cycles.
        assert!(out.crossval_price_overlap_p50 >= 50, "{out:?}");
        assert!(out.crossval_top10_overlap_p50 >= 10, "{out:?}");
        assert!(out.crossval_top10_overlap_p50 <= 100, "{out:?}");
    }
}
