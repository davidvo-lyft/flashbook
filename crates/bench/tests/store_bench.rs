//! Integration tests for the DuckDB/SQLite/Parquet comparison harness
//! (feature `compare`): the three backends must return IDENTICAL results on
//! a synthetic store that exercises snapshots, deltas, trades, `Clear`, an
//! instrument with no snapshots, and a dangling (incomplete) `SnapBegin`
//! that makes the naive SQL anchor diverge from the validated one.
#![cfg(feature = "compare")]

use std::path::Path;

use flashbook_bench::compare::{
    full_scan_duckdb, full_scan_ours, full_scan_sqlite, load_duckdb, load_sqlite, pit_duckdb,
    pit_ours, pit_sqlite, write_parquet_via_duckdb,
};
use flashbook_proto::event::{Event, EventKind, Venue};
use flashbook_store::pit::SnapshotIndex;
use flashbook_store::segment::{StoreReader, StoreWriter};

fn ev(mono: u64, instrument: u32, kind: EventKind, price: i64, qty: i64) -> Event {
    Event {
        recv_mono_ns: mono,
        recv_wall_ns: mono + 1_000,
        venue_ts_ns: mono + 2_000,
        venue_seq: mono,
        price,
        qty,
        aux: 0,
        instrument,
        kind: kind as u8,
        venue: Venue::Coinbase as u8,
        flags: 0,
        rsvd: 0,
    }
}

/// The synthetic corpus, in strict mono order:
/// - instrument 1: two complete snapshots, deltas, a trade, then a DANGLING
///   `SnapBegin` at mono 500 (never ended) with one level after it;
/// - instrument 2: one snapshot, a delta, a `Clear`, then a second snapshot;
/// - instrument 3: a lone trade, no snapshots ever (anchor misses).
fn corpus() -> Vec<Event> {
    use EventKind::*;
    vec![
        ev(100, 1, SnapBegin, 0, 0),
        ev(101, 1, SnapBid, 10_000, 5),
        ev(102, 1, SnapAsk, 10_100, 4),
        ev(103, 1, SnapEnd, 0, 0),
        ev(105, 2, SnapBegin, 0, 0),
        ev(106, 2, SnapBid, 5_000, 5),
        ev(107, 2, SnapAsk, 5_100, 5),
        ev(108, 2, SnapEnd, 0, 0),
        ev(110, 1, BidSet, 10_000, 7),
        ev(115, 3, Trade, 777, 1),
        ev(120, 1, Trade, 10_050, 2),
        ev(150, 2, AskSet, 5_100, 3),
        ev(200, 1, SnapBegin, 0, 0),
        ev(201, 1, SnapBid, 10_200, 1),
        ev(202, 1, SnapAsk, 10_300, 2),
        ev(203, 1, SnapEnd, 0, 0),
        ev(250, 2, Clear, 0, 0),
        ev(260, 2, SnapBegin, 0, 0),
        ev(261, 2, SnapBid, 5_200, 2),
        ev(262, 2, SnapEnd, 0, 0),
        ev(300, 1, BidSet, 10_200, 9),
        ev(500, 1, SnapBegin, 0, 0), // dangling: no SnapEnd ever
        ev(510, 1, SnapBid, 9_000, 1),
    ]
}

/// Write `events` as a sealed multi-block segment and reopen it.
fn build_store(dir: &Path, events: &[Event]) -> StoreReader {
    let path = dir.join("synthetic.fbstore");
    let mut w = StoreWriter::create(&path, b"{}", 4, Some(1)).unwrap();
    for e in events {
        w.append(e).unwrap();
    }
    w.seal().unwrap();
    StoreReader::open(&path).unwrap()
}

#[test]
fn full_scan_identical_across_all_three_backends() {
    let dir = tempfile::tempdir().unwrap();
    let events = corpus();
    let reader = build_store(dir.path(), &events);

    let (_, duck) = load_duckdb(&reader, &dir.path().join("e.duckdb")).unwrap();
    let (_, lite) = load_sqlite(&reader, &dir.path().join("e.sqlite")).unwrap();

    let (_, ours) = full_scan_ours(&reader).unwrap();
    let (_, d) = full_scan_duckdb(&duck).unwrap();
    let (_, s) = full_scan_sqlite(&lite).unwrap();
    assert_eq!(ours, d, "ours vs duckdb");
    assert_eq!(ours, s, "ours vs sqlite");

    // Spot-check the aggregate itself, not just cross-backend agreement.
    assert_eq!(ours.len(), 3);
    let i1 = &ours[0];
    assert_eq!(
        (i1.instrument, i1.count, i1.max_mono),
        (1, 13, 510),
        "{i1:?}"
    );
    assert_eq!((i1.min_price, i1.max_price), (0, 10_300));
    let i3 = &ours[2];
    assert_eq!((i3.instrument, i3.count, i3.sum_qty), (3, 1, 1));
}

#[test]
fn pit_identical_across_backends_including_incomplete_anchor_divergence() {
    let dir = tempfile::tempdir().unwrap();
    let events = corpus();
    let reader = build_store(dir.path(), &events);
    let index = SnapshotIndex::build(&reader).unwrap();

    let (_, duck) = load_duckdb(&reader, &dir.path().join("e.duckdb")).unwrap();
    let (_, lite) = load_sqlite(&reader, &dir.path().join("e.sqlite")).unwrap();

    // (instrument, t, expect_divergence)
    let cases = [
        (1, 104, false), // right after first snapshot
        (1, 110, false), // + best-bid delta
        (1, 400, false), // second snapshot + delta
        (1, 600, true),  // past the dangling SnapBegin: naive SQL anchor=500
        (2, 160, false), // snapshot + ask delta
        (2, 255, false), // after Clear, before resnap: empty synced-off book
        (2, 600, false), // second snapshot
        (3, 600, false), // no snapshot ever: anchor miss on every backend
        (1, 50, false),  // before any snapshot: anchor miss
    ];
    for (inst, t, expect_div) in cases {
        let (_, ours) = pit_ours(&reader, &index, inst, t).unwrap();
        let d = pit_duckdb(&duck, inst, t, ours.anchor_mono).unwrap();
        let s = pit_sqlite(&lite, inst, t, ours.anchor_mono).unwrap();
        assert_eq!(d.top, ours, "duckdb pit({inst}, {t})");
        assert_eq!(s.top, ours, "sqlite pit({inst}, {t})");
        assert_eq!(
            d.anchor_diverged, expect_div,
            "duckdb divergence ({inst}, {t})"
        );
        assert_eq!(
            s.anchor_diverged, expect_div,
            "sqlite divergence ({inst}, {t})"
        );
    }

    // Pin the interesting states, not just agreement.
    let (_, top) = pit_ours(&reader, &index, 1, 104).unwrap();
    assert_eq!(top.anchor_mono, Some(100));
    assert_eq!(top.best_bid, Some((10_000, 5)));
    assert_eq!(top.best_ask, Some((10_100, 4)));

    let (_, top) = pit_ours(&reader, &index, 1, 110).unwrap();
    assert_eq!(top.best_bid, Some((10_000, 7)), "delta applied");

    // Divergence case: ours anchors at the complete snapshot (200), the
    // naive SQL anchor points at the dangling SnapBegin (500); folding from
    // ours' anchor replays THROUGH the dangling bracket, so the book shows
    // its partial refill identically on every backend.
    let (_, top) = pit_ours(&reader, &index, 1, 600).unwrap();
    assert_eq!(top.anchor_mono, Some(200));
    assert_eq!(top.best_bid, Some((9_000, 1)));
    assert_eq!(top.best_ask, None);
    let d = pit_duckdb(&duck, 1, 600, top.anchor_mono).unwrap();
    assert_eq!(d.sql_anchor, Some(500), "naive anchor = dangling SnapBegin");

    // Anchor miss: everything empty everywhere.
    let (_, top) = pit_ours(&reader, &index, 3, 600).unwrap();
    assert_eq!(
        (top.anchor_mono, top.best_bid, top.best_ask),
        (None, None, None)
    );

    // Clear wipes instrument 2 until its re-snapshot.
    let (_, top) = pit_ours(&reader, &index, 2, 255).unwrap();
    assert_eq!((top.best_bid, top.best_ask), (None, None));
    let (_, top) = pit_ours(&reader, &index, 2, 600).unwrap();
    assert_eq!(top.anchor_mono, Some(260));
    assert_eq!(top.best_bid, Some((5_200, 2)));
    assert_eq!(top.best_ask, None);
}

#[test]
fn parquet_export_roundtrips_row_count() {
    let dir = tempfile::tempdir().unwrap();
    let events = corpus();
    let reader = build_store(dir.path(), &events);
    let (_, duck) = load_duckdb(&reader, &dir.path().join("e.duckdb")).unwrap();

    let pq = dir.path().join("events.parquet");
    let (secs, bytes) = write_parquet_via_duckdb(&duck, &pq).unwrap();
    assert!(secs > 0.0);
    assert!(bytes > 0);
    assert_eq!(bytes, std::fs::metadata(&pq).unwrap().len());

    let n: i64 = duck
        .query_row(
            &format!(
                "SELECT count(*) FROM read_parquet('{}')",
                pq.to_str().unwrap()
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n as usize, events.len(), "parquet holds every event");
}

#[test]
fn loads_reject_u64_values_that_overflow_bigint() {
    let dir = tempfile::tempdir().unwrap();
    let mut events = corpus();
    events[5].aux = u64::MAX; // unrepresentable in BIGINT: must fail, not wrap
    let reader = build_store(dir.path(), &events);

    let err = load_duckdb(&reader, &dir.path().join("bad.duckdb")).unwrap_err();
    assert!(err.to_string().contains("aux"), "{err:#}");
    let err = load_sqlite(&reader, &dir.path().join("bad.sqlite")).unwrap_err();
    assert!(err.to_string().contains("aux"), "{err:#}");
}
