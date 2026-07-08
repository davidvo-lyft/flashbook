//! Binance codec integration tests: fast/slow differential over the captured
//! fixtures, golden events, the diff-depth sync protocol, malformed inputs,
//! and proptest fuzz + a model-based state-machine check.

use std::sync::LazyLock;

use flashbook_feed::binance::BinanceCodec;
use flashbook_feed::{CodecError, Signal, SymbolTable, VenueCodec};
use flashbook_proto::event::flags;
use flashbook_proto::{Event, EventKind, Venue};
use proptest::prelude::*;

const WS_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/binance/live-btc-eth.ndjson"
);
const REST_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/binance/rest-depth-btc.json"
);

const BTC: u32 = 6;
const ETH: u32 = 7;

static LINES: LazyLock<Vec<String>> = LazyLock::new(|| {
    std::fs::read_to_string(WS_FIXTURE)
        .expect("ws fixture")
        .lines()
        .map(str::to_owned)
        .collect()
});

static REST_BODY: LazyLock<Vec<u8>> =
    LazyLock::new(|| std::fs::read(REST_FIXTURE).expect("rest fixture"));

fn table() -> SymbolTable {
    SymbolTable::new([("BTCUSDT".to_string(), BTC), ("ETHUSDT".to_string(), ETH)])
}

fn codecs() -> (BinanceCodec, BinanceCodec) {
    (BinanceCodec::new(table()), BinanceCodec::new(table()))
}

fn depth_line(sym: &str, first_u: u64, final_u: u64, bids: &str, asks: &str) -> String {
    let lower = sym.to_lowercase();
    format!(
        r#"{{"stream":"{lower}@depth@100ms","data":{{"e":"depthUpdate","E":1700000000123,"s":"{sym}","U":{first_u},"u":{final_u},"b":[{bids}],"a":[{asks}]}}}}"#
    )
}

fn trade_line(sym: &str, t: u64, p: &str, q: &str, t_ms: u64, m: bool) -> String {
    let lower = sym.to_lowercase();
    format!(
        r#"{{"stream":"{lower}@trade","data":{{"e":"trade","E":{t_ms},"s":"{sym}","t":{t},"p":"{p}","q":"{q}","T":{t_ms},"m":{m},"M":true}}}}"#
    )
}

fn snap_body(last_update_id: u64) -> String {
    format!(
        r#"{{"lastUpdateId":{last_update_id},"bids":[["100.00","1.00"]],"asks":[["101.00","1.00"]]}}"#
    )
}

/// Parse one payload through fast codec A and slow codec B with the same
/// fixed recv timestamps; assert identical events and signal; return them.
fn step_both(
    fast: &mut BinanceCodec,
    slow: &mut BinanceCodec,
    payload: &[u8],
    mono: u64,
    wall: u64,
) -> (Signal, Vec<Event>) {
    let mut out_f = Vec::new();
    let mut out_s = Vec::new();
    let sig_f = fast
        .parse(payload, mono, wall, &mut out_f)
        .unwrap_or_else(|e| {
            panic!(
                "fast parse failed: {e} on {}",
                String::from_utf8_lossy(payload)
            )
        });
    let sig_s = slow
        .parse_slow(payload, mono, wall, &mut out_s)
        .unwrap_or_else(|e| {
            panic!(
                "slow parse failed: {e} on {}",
                String::from_utf8_lossy(payload)
            )
        });
    assert_eq!(
        sig_f,
        sig_s,
        "signal mismatch: {}",
        String::from_utf8_lossy(payload)
    );
    assert_eq!(
        out_f,
        out_s,
        "event mismatch: {}",
        String::from_utf8_lossy(payload)
    );
    (sig_f, out_f)
}

/// Run every fixture line through both paths, optionally after feeding the
/// REST snapshot to both; returns the total event count.
fn differential(with_snapshot: bool) -> usize {
    let (mut fast, mut slow) = codecs();
    let mut total = 0usize;
    if with_snapshot {
        let mut out_f = Vec::new();
        let mut out_s = Vec::new();
        let sig_f = fast
            .parse_rest_snapshot(BTC, &REST_BODY, 5, 7, &mut out_f)
            .unwrap();
        let sig_s = slow
            .parse_rest_snapshot(BTC, &REST_BODY, 5, 7, &mut out_s)
            .unwrap();
        assert_eq!(sig_f, sig_s);
        assert_eq!(out_f, out_s);
        total += out_f.len();
    }
    for (i, line) in LINES.iter().enumerate() {
        let mono = 1_000 + i as u64;
        let wall = 2_000_000 + i as u64;
        let (_, events) = step_both(&mut fast, &mut slow, line.as_bytes(), mono, wall);
        total += events.len();
    }
    assert_eq!(fast.stats().fast_msgs, LINES.len() as u64);
    assert_eq!(fast.stats().events, slow.stats().events);
    total
}

#[test]
fn venue_url_and_subscribe() {
    let c = BinanceCodec::new(table());
    assert_eq!(c.venue(), Venue::Binance);
    assert_eq!(
        c.ws_url(),
        "wss://stream.binance.com:9443/stream?streams=\
         btcusdt@depth@100ms/btcusdt@trade/ethusdt@depth@100ms/ethusdt@trade"
    );
    assert!(c.subscribe_messages().is_empty());
}

#[test]
fn differential_with_rest_snapshot() {
    // The captured REST snapshot postdates every captured diff (all diffs are
    // stale against lastUpdateId), so this pass covers snapshot + stale-drop +
    // trades; fast and slow must still agree on every line.
    let total = differential(true);
    println!("differential with snapshot: {total} events");
    assert!(total > 1_300, "total events {total} below sanity floor");
}

#[test]
fn differential_without_snapshot() {
    // No snapshot anchor: every depth diff is dropped as unsynced, but all
    // 1163 trades still flow identically through both paths.
    let total = differential(false);
    println!("differential without snapshot: {total} events");
    assert!(total > 1_100, "total events {total} below sanity floor");
}

#[test]
fn differential_synced_from_stream_start() {
    // Synthetic snapshots anchored just before each instrument's first diff
    // make every captured depth level apply, exercising the full BidSet /
    // AskSet emission path differentially.
    let (mut fast, mut slow) = codecs();
    for (inst, anchor) in [(BTC, 97_109_082_199u64), (ETH, 78_545_066_028u64)] {
        let body = snap_body(anchor);
        let mut out_f = Vec::new();
        let mut out_s = Vec::new();
        let sig_f = fast
            .parse_rest_snapshot(inst, body.as_bytes(), 1, 2, &mut out_f)
            .unwrap();
        let sig_s = slow
            .parse_rest_snapshot(inst, body.as_bytes(), 1, 2, &mut out_s)
            .unwrap();
        assert_eq!(sig_f, sig_s);
        assert_eq!(out_f, out_s);
    }
    let mut total = 0usize;
    let mut book_events = 0usize;
    for (i, line) in LINES.iter().enumerate() {
        let (_, events) = step_both(&mut fast, &mut slow, line.as_bytes(), i as u64, i as u64);
        book_events += events
            .iter()
            .filter(|e| matches!(e.kind(), Ok(EventKind::BidSet | EventKind::AskSet)))
            .count();
        total += events.len();
    }
    println!("differential synced: {total} events ({book_events} book)");
    assert!(total > 10_000, "total events {total} below sanity floor");
    assert!(book_events > 9_000, "book events {book_events} below floor");
    assert_eq!(fast.stats().gaps, 0);
}

#[test]
fn golden_trade_event() {
    // First fixture line, hand-verified:
    // {"e":"trade","E":1783465157897,"s":"BTCUSDT","t":6486989273,
    //  "p":"63551.02000000","q":"0.00317000","T":1783465157897,"m":true,...}
    let (mut fast, mut slow) = codecs();
    let (sig, events) = step_both(&mut fast, &mut slow, LINES[0].as_bytes(), 11, 22);
    assert_eq!(sig, Signal::None);
    assert_eq!(
        events,
        vec![Event {
            recv_mono_ns: 11,
            recv_wall_ns: 22,
            venue_ts_ns: 1_783_465_157_897_000_000,
            venue_seq: 6_486_989_273,
            price: 6_355_102_000_000,
            qty: 317_000,
            aux: 6_486_989_273,
            instrument: BTC,
            kind: EventKind::Trade as u8,
            venue: Venue::Binance as u8,
            flags: flags::TAKER_SELL, // m == true: buyer was maker, aggressor sold
            rsvd: 0,
        }]
    );
}

#[test]
fn golden_depth_update() {
    // Fixture line 3 (index 2), hand-verified:
    // {"e":"depthUpdate","E":1783465157917,"s":"BTCUSDT","U":97109082200,
    //  "u":97109082205,"b":[["63551.02000000","1.87857000"],
    //  ["57195.92000000","0.00480000"]],"a":[]}
    let (mut fast, mut slow) = codecs();
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        c.parse_rest_snapshot(BTC, snap_body(97_109_082_199).as_bytes(), 1, 2, &mut out)
            .unwrap();
    }
    let (sig, events) = step_both(&mut fast, &mut slow, LINES[2].as_bytes(), 33, 44);
    assert_eq!(sig, Signal::None);
    let bid = |price: i64, qty: i64| Event {
        recv_mono_ns: 33,
        recv_wall_ns: 44,
        venue_ts_ns: 1_783_465_157_917_000_000,
        venue_seq: 97_109_082_205,
        price,
        qty,
        aux: 0,
        instrument: BTC,
        kind: EventKind::BidSet as u8,
        venue: Venue::Binance as u8,
        flags: 0,
        rsvd: 0,
    };
    assert_eq!(
        events,
        vec![
            bid(6_355_102_000_000, 187_857_000),
            bid(5_719_592_000_000, 480_000),
        ]
    );
}

#[test]
fn golden_rest_snapshot() {
    let mut c = BinanceCodec::new(table());
    let mut out = Vec::new();
    let sig = c
        .parse_rest_snapshot(BTC, &REST_BODY, 9, 10, &mut out)
        .unwrap();
    assert_eq!(sig, Signal::None);
    // 100 bids + 100 asks, bracketed by Clear + SnapBegin ... SnapEnd.
    assert_eq!(out.len(), 203);
    let snap = flags::FROM_SNAPSHOT | flags::SYNTHETIC;
    let mk = |kind: EventKind, price: i64, qty: i64, aux: u64| Event {
        recv_mono_ns: 9,
        recv_wall_ns: 10,
        venue_ts_ns: 0,
        venue_seq: 97_109_906_740,
        price,
        qty,
        aux,
        instrument: BTC,
        kind: kind as u8,
        venue: Venue::Binance as u8,
        flags: snap,
        rsvd: 0,
    };
    assert_eq!(out[0], mk(EventKind::Clear, 0, 0, 0));
    assert_eq!(out[1], mk(EventKind::SnapBegin, 0, 0, 200));
    // First bid ["63566.00000000","2.33079000"].
    assert_eq!(
        out[2],
        mk(EventKind::SnapBid, 6_356_600_000_000, 233_079_000, 0)
    );
    // First ask ["63566.01000000","0.04961000"] comes after all 100 bids.
    assert_eq!(
        out[102],
        mk(EventKind::SnapAsk, 6_356_601_000_000, 4_961_000, 0)
    );
    assert_eq!(out[202], mk(EventKind::SnapEnd, 0, 0, 0));
    assert!(
        out[..102]
            .iter()
            .skip(2)
            .all(|e| e.kind == EventKind::SnapBid as u8)
    );
    assert!(
        out[102..202]
            .iter()
            .all(|e| e.kind == EventKind::SnapAsk as u8)
    );
}

#[test]
fn sync_protocol_full_walk() {
    // unsynced drop -> snapshot -> straddling first diff applies ->
    // contiguous diffs apply -> gap -> NeedResync + unsynced ->
    // re-snapshot recovers.
    let (mut fast, mut slow) = codecs();
    let lvl = r#"["100.00","1.00"]"#;

    // Unsynced: fully parsed, nothing emitted.
    let d = depth_line("BTCUSDT", 5, 6, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 1, 1);
    assert_eq!((sig, ev.len()), (Signal::None, 0));

    // Snapshot anchors last_u = 10.
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        assert_eq!(
            c.parse_rest_snapshot(BTC, snap_body(10).as_bytes(), 1, 1, &mut out)
                .unwrap(),
            Signal::None
        );
        assert_eq!(out.len(), 5); // Clear + SnapBegin + 1 bid + 1 ask + SnapEnd
    }

    // Straddling first diff (U <= lastUpdateId+1 <= u) applies.
    let d = depth_line("BTCUSDT", 9, 12, lvl, lvl);
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 2, 2);
    assert_eq!((sig, ev.len()), (Signal::None, 2));
    assert_eq!(ev[0].kind, EventKind::BidSet as u8);
    assert_eq!(ev[1].kind, EventKind::AskSet as u8);
    assert_eq!(ev[0].venue_seq, 12);

    // Contiguous diff applies.
    let d = depth_line("BTCUSDT", 13, 14, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 3, 3);
    assert_eq!((sig, ev.len()), (Signal::None, 1));

    // Gap: U=17 > last_u+1=15 -> Gap event + NeedResync, book events dropped.
    let d = depth_line("BTCUSDT", 17, 18, lvl, lvl);
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 4, 4);
    assert_eq!(sig, Signal::NeedResync { instrument: BTC });
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].kind, EventKind::Gap as u8);
    assert_eq!(ev[0].aux, 2); // missed 15, 16
    assert_eq!(ev[0].venue_seq, 18);
    assert_eq!(fast.stats().gaps, 1);
    assert_eq!(slow.stats().gaps, 1);

    // Unsynced again: contiguous-looking diff is dropped.
    let d = depth_line("BTCUSDT", 19, 20, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 5, 5);
    assert_eq!((sig, ev.len()), (Signal::None, 0));

    // Re-snapshot recovers; the next diff applies.
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        c.parse_rest_snapshot(BTC, snap_body(25).as_bytes(), 6, 6, &mut out)
            .unwrap();
    }
    let d = depth_line("BTCUSDT", 26, 27, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 7, 7);
    assert_eq!((sig, ev.len()), (Signal::None, 1));
    assert_eq!(ev[0].kind, EventKind::BidSet as u8);
}

#[test]
fn stale_diffs_dropped_after_snapshot() {
    let (mut fast, mut slow) = codecs();
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        c.parse_rest_snapshot(BTC, snap_body(100).as_bytes(), 1, 1, &mut out)
            .unwrap();
    }
    let lvl = r#"["100.00","1.00"]"#;
    // u == lastUpdateId and u < lastUpdateId: both stale, no events, no resync.
    for (first, last) in [(90u64, 100u64), (95, 99)] {
        let d = depth_line("BTCUSDT", first, last, lvl, lvl);
        let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 2, 2);
        assert_eq!((sig, ev.len()), (Signal::None, 0), "U={first} u={last}");
    }
    // State was untouched: the straddling diff still applies.
    let d = depth_line("BTCUSDT", 99, 103, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 3, 3);
    assert_eq!((sig, ev.len()), (Signal::None, 1));
    assert_eq!(fast.stats().gaps, 0);
}

#[test]
fn depth_state_is_per_instrument() {
    // Anchoring BTC must not sync ETH.
    let (mut fast, mut slow) = codecs();
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        c.parse_rest_snapshot(BTC, snap_body(10).as_bytes(), 1, 1, &mut out)
            .unwrap();
    }
    let lvl = r#"["100.00","1.00"]"#;
    let d = depth_line("ETHUSDT", 11, 12, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 2, 2);
    assert_eq!((sig, ev.len()), (Signal::None, 0)); // ETH still unsynced
    let d = depth_line("BTCUSDT", 11, 12, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 3, 3);
    assert_eq!((sig, ev.len()), (Signal::None, 1)); // BTC applies
}

#[test]
fn trade_id_gap_emits_gap_then_trade() {
    let (mut fast, mut slow) = codecs();
    for (t, want_events) in [(100u64, 1usize), (101, 1), (105, 2)] {
        let line = trade_line(
            "BTCUSDT",
            t,
            "63000.00",
            "0.50000000",
            1_700_000_000_123,
            false,
        );
        let (sig, ev) = step_both(&mut fast, &mut slow, line.as_bytes(), t, t);
        assert_eq!(sig, Signal::None, "trade gaps never demand resync");
        assert_eq!(ev.len(), want_events, "t={t}");
        if want_events == 2 {
            assert_eq!(ev[0].kind, EventKind::Gap as u8);
            assert_eq!(ev[0].aux, 3); // missed 102, 103, 104
            assert_eq!(ev[0].venue_seq, 105);
            assert_eq!(ev[1].kind, EventKind::Trade as u8);
            assert_eq!(ev[1].aux, 105);
        }
        let last = ev.last().unwrap();
        assert_eq!(last.kind, EventKind::Trade as u8);
        assert_eq!(last.flags, 0); // m == false: aggressor bought
        assert_eq!(last.venue_ts_ns, 1_700_000_000_123_000_000);
        assert_eq!(last.price, 6_300_000_000_000);
        assert_eq!(last.qty, 50_000_000);
    }
    assert_eq!(fast.stats().gaps, 1);
    assert_eq!(slow.stats().gaps, 1);
}

#[test]
fn control_ack_and_ignored_types() {
    let (mut fast, mut slow) = codecs();
    let mut out = Vec::new();
    // Subscription ack shape -> Control.
    let ack = br#"{"result":null,"id":3}"#;
    assert_eq!(fast.parse(ack, 1, 1, &mut out).unwrap(), Signal::Control);
    assert_eq!(
        slow.parse_slow(ack, 1, 1, &mut out).unwrap(),
        Signal::Control
    );
    // Unknown event type inside a valid envelope -> Ignored.
    let kline = br#"{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"s":"BTCUSDT","k":{}}}"#;
    assert_eq!(fast.parse(kline, 1, 1, &mut out).unwrap(), Signal::Ignored);
    assert_eq!(
        slow.parse_slow(kline, 1, 1, &mut out).unwrap(),
        Signal::Ignored
    );
    assert!(out.is_empty());
}

#[test]
fn server_shutdown_signals_reconnect() {
    // Binance docs: on serverShutdown the client should reconnect as soon
    // as possible. Both paths must agree: Signal::Reconnect, no events.
    let (mut fast, mut slow) = codecs();
    let frame = br#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1700000000123}}"#;
    let (sig, ev) = step_both(&mut fast, &mut slow, frame, 1, 1);
    assert_eq!((sig, ev.len()), (Signal::Reconnect, 0));
}

#[test]
fn stale_rest_snapshot_rejected() {
    // Documented sync step 4: while unsynced, the codec tracks the highest
    // diff u seen; a REST snapshot with lastUpdateId below that mark must be
    // rejected (Err, no anchor) so the connection layer retries.
    let (mut fast, mut slow) = codecs();
    let lvl = r#"["100.00","1.00"]"#;

    // Unsynced diff up to u=100 sets the high-water mark on both paths.
    let d = depth_line("BTCUSDT", 90, 100, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 1, 1);
    assert_eq!((sig, ev.len()), (Signal::None, 0));

    // Snapshot older than the mark: rejected, buffer restored, still unsynced.
    for c in [&mut fast, &mut slow] {
        let mut out = vec![Event::ZERO];
        let res = c.parse_rest_snapshot(BTC, snap_body(50).as_bytes(), 2, 2, &mut out);
        assert!(
            matches!(res, Err(CodecError::Structure("stale rest snapshot"))),
            "stale snapshot not rejected"
        );
        assert_eq!(out, vec![Event::ZERO], "buffer not restored");
    }

    // State stayed Unsynced: a contiguous-looking diff is still dropped
    // (and advances the mark to 102).
    let d = depth_line("BTCUSDT", 101, 102, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 3, 3);
    assert_eq!((sig, ev.len()), (Signal::None, 0));

    // A fresh snapshot at exactly the mark (lastUpdateId >= high-water)
    // anchors normally and clears the mark; the next diff applies.
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        assert_eq!(
            c.parse_rest_snapshot(BTC, snap_body(102).as_bytes(), 4, 4, &mut out)
                .unwrap(),
            Signal::None
        );
        assert_eq!(out.len(), 5); // Clear + SnapBegin + 1 bid + 1 ask + SnapEnd
    }
    let d = depth_line("BTCUSDT", 103, 104, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 5, 5);
    assert_eq!((sig, ev.len()), (Signal::None, 1));
    assert_eq!(ev[0].kind, EventKind::BidSet as u8);
}

#[test]
fn gap_diff_seeds_stale_snapshot_mark() {
    // The diff that triggers a sequence gap was itself seen on the stream,
    // so its u seeds the unsynced high-water mark: a recovery snapshot
    // predating it is rejected.
    let (mut fast, mut slow) = codecs();
    let lvl = r#"["100.00","1.00"]"#;
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        c.parse_rest_snapshot(BTC, snap_body(10).as_bytes(), 1, 1, &mut out)
            .unwrap();
    }
    // Gap: U=17 > last_u+1=11 -> NeedResync; mark seeded with u=18.
    let d = depth_line("BTCUSDT", 17, 18, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 2, 2);
    assert_eq!(sig, Signal::NeedResync { instrument: BTC });
    assert_eq!(ev.len(), 1);
    // Recovery snapshot at 17 predates the gap diff's u=18: rejected.
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        assert!(matches!(
            c.parse_rest_snapshot(BTC, snap_body(17).as_bytes(), 3, 3, &mut out),
            Err(CodecError::Structure("stale rest snapshot"))
        ));
        assert!(out.is_empty());
    }
    // Snapshot at 18 anchors; the straddling diff applies.
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        c.parse_rest_snapshot(BTC, snap_body(18).as_bytes(), 4, 4, &mut out)
            .unwrap();
    }
    let d = depth_line("BTCUSDT", 18, 20, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 5, 5);
    assert_eq!((sig, ev.len()), (Signal::None, 1));
}

#[test]
fn unknown_symbol_is_rejected() {
    let (mut fast, mut slow) = codecs();
    let lvl = r#"["100.00","1.00"]"#;
    let depth = depth_line("FOOUSDT", 1, 2, lvl, "");
    let trade = trade_line("FOOUSDT", 1, "1.00", "1.00", 1, true);
    let mut out = Vec::new();
    for payload in [depth.as_bytes(), trade.as_bytes()] {
        assert!(matches!(
            fast.parse(payload, 1, 1, &mut out),
            Err(CodecError::UnknownInstrument)
        ));
        assert!(matches!(
            slow.parse_slow(payload, 1, 1, &mut out),
            Err(CodecError::UnknownInstrument)
        ));
    }
    assert!(out.is_empty());
}

#[test]
fn malformed_inputs_error_and_restore_buffer() {
    let (mut fast, mut slow) = codecs();
    // Sync BTC so depth payloads reach the emission path.
    for c in [&mut fast, &mut slow] {
        let mut out = Vec::new();
        c.parse_rest_snapshot(BTC, snap_body(10).as_bytes(), 1, 1, &mut out)
            .unwrap();
    }
    let cases: Vec<String> = vec![
        // Truncated JSON.
        LINES[0][..20].to_string(),
        LINES[2][..LINES[2].len() / 2].to_string(),
        String::new(),
        "{".to_string(),
        // Wrong-type fields.
        r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":"abc","u":12,"b":[],"a":[]}}"#.to_string(),
        r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":11,"u":12,"b":42,"a":[]}}"#.to_string(),
        r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1.0","q":"1.0","T":1,"m":"yes"}}"#.to_string(),
        // Bad numbers: malformed decimal and sub-1e-8 precision.
        depth_line("BTCUSDT", 11, 12, r#"["1.2.3","1.00"]"#, ""),
        depth_line("BTCUSDT", 11, 12, r#"["100.00","0.000000001"]"#, ""),
        trade_line("BTCUSDT", 7, "1.2.3", "1.0", 1, true),
        // Bad control shape.
        r#"{"result":42,"id":1}"#.to_string(),
    ];
    let mut out = vec![Event::ZERO]; // sentinel: buffer must be restored
    for case in &cases {
        let rf = fast.parse(case.as_bytes(), 1, 1, &mut out);
        assert!(rf.is_err(), "fast accepted: {case}");
        let rs = slow.parse_slow(case.as_bytes(), 1, 1, &mut out);
        assert!(rs.is_err(), "slow accepted: {case}");
        assert_eq!(out, vec![Event::ZERO], "buffer not restored: {case}");
    }
    // The bad-number cases surface as exact fixed-point errors on both paths.
    let bad_price = depth_line("BTCUSDT", 11, 12, r#"["1.2.3","1.00"]"#, "");
    assert!(matches!(
        fast.parse(bad_price.as_bytes(), 1, 1, &mut out),
        Err(CodecError::Fixed(_))
    ));
    assert!(matches!(
        slow.parse_slow(bad_price.as_bytes(), 1, 1, &mut out),
        Err(CodecError::Fixed(_))
    ));
    // A failed message never advances the state machine: 13/14 still applies.
    let lvl = r#"["100.00","1.00"]"#;
    let d = depth_line("BTCUSDT", 9, 14, lvl, "");
    let (sig, ev) = step_both(&mut fast, &mut slow, d.as_bytes(), 2, 2);
    assert_eq!((sig, ev.len()), (Signal::None, 1));
}

#[test]
fn malformed_rest_snapshot_errors_and_restores() {
    let mut c = BinanceCodec::new(table());
    let mut out = vec![Event::ZERO];
    for body in [
        &b"{"[..],
        br#"{"bids":[],"asks":[]}"#,
        br#"{"lastUpdateId":"x","bids":[],"asks":[]}"#,
        br#"{"lastUpdateId":5,"bids":[["1.2.3","1.0"]],"asks":[]}"#,
        br#"{"lastUpdateId":5,"bids":[["1.0"]],"asks":[]}"#,
    ] {
        assert!(
            c.parse_rest_snapshot(BTC, body, 1, 1, &mut out).is_err(),
            "accepted: {}",
            String::from_utf8_lossy(body)
        );
        assert_eq!(out, vec![Event::ZERO]);
    }
    // Unknown instrument id.
    assert!(matches!(
        c.parse_rest_snapshot(999, snap_body(5).as_bytes(), 1, 1, &mut out),
        Err(CodecError::UnknownInstrument)
    ));
    // A failed snapshot never anchors: diffs stay unsynced.
    let d = depth_line("BTCUSDT", 6, 7, r#"["100.00","1.00"]"#, "");
    let mut ev = Vec::new();
    assert_eq!(c.parse(d.as_bytes(), 1, 1, &mut ev).unwrap(), Signal::None);
    assert!(ev.is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Random byte mutations / truncations of real fixture lines: both paths
    /// may error but must never panic (and never leave partial events).
    #[test]
    fn fuzz_mutated_fixture_lines_never_panic(
        idx in 0usize..10_000,
        pos in 0usize..10_000,
        byte in any::<u8>(),
        mode in 0u8..3,
    ) {
        let line = &LINES[idx % LINES.len()];
        let mut payload = line.clone().into_bytes();
        let at = pos % payload.len();
        match mode {
            0 => payload.truncate(at),
            1 => payload[at] = byte,
            _ => payload.insert(at, byte),
        }
        let (mut fast, mut slow) = codecs();
        // Anchor BTC so mutated diffs can reach the emission path too.
        let mut out = Vec::new();
        fast.parse_rest_snapshot(BTC, &REST_BODY, 1, 1, &mut out).unwrap();
        slow.parse_rest_snapshot(BTC, &REST_BODY, 1, 1, &mut out).unwrap();
        out.clear();
        let before = out.len();
        if fast.parse(&payload, 1, 2, &mut out).is_err() {
            prop_assert_eq!(out.len(), before);
        }
        let before = out.len();
        if slow.parse_slow(&payload, 1, 2, &mut out).is_err() {
            prop_assert_eq!(out.len(), before);
        }
    }

    /// Model-based check of the diff-depth state machine: random U/u chains
    /// with injected gaps and stales. The codec must never emit book events
    /// across a gap and must demand a resync exactly when U > last_u + 1.
    #[test]
    fn depth_state_machine_matches_model(
        steps in prop::collection::vec((-3i64..=6, 0u64..=4, any::<bool>()), 1..40),
    ) {
        let (mut fast, mut slow) = codecs();
        // Model: None = Unsynced, Some(last_u) = Synced.
        let mut model: Option<u64> = None;
        let mut cursor: u64 = 1_000_000;
        for (delta, span, resync) in steps {
            if resync {
                let body = snap_body(cursor);
                for c in [&mut fast, &mut slow] {
                    let mut out = Vec::new();
                    c.parse_rest_snapshot(BTC, body.as_bytes(), 1, 1, &mut out).unwrap();
                }
                model = Some(cursor);
            }
            let first = cursor.saturating_add_signed(delta).max(1);
            let last = first + span;
            let line = depth_line("BTCUSDT", first, last, r#"["100.00","1.00"]"#, r#"["101.00","2.00"]"#);
            let mut out_f = Vec::new();
            let mut out_s = Vec::new();
            let sig_f = fast.parse(line.as_bytes(), 1, 2, &mut out_f).unwrap();
            let sig_s = slow.parse_slow(line.as_bytes(), 1, 2, &mut out_s).unwrap();
            prop_assert_eq!(sig_f, sig_s);
            prop_assert_eq!(&out_f, &out_s);
            match model {
                None => {
                    prop_assert_eq!(sig_f, Signal::None);
                    prop_assert!(out_f.is_empty());
                }
                Some(last_u) if last <= last_u => {
                    // Stale duplicate: dropped, state unchanged.
                    prop_assert_eq!(sig_f, Signal::None);
                    prop_assert!(out_f.is_empty());
                }
                Some(last_u) if first > last_u + 1 => {
                    // Gap: exactly one Gap marker, no book events, resync.
                    prop_assert_eq!(sig_f, Signal::NeedResync { instrument: BTC });
                    prop_assert_eq!(out_f.len(), 1);
                    prop_assert_eq!(out_f[0].kind, EventKind::Gap as u8);
                    prop_assert_eq!(out_f[0].aux, first - last_u - 1);
                    prop_assert_eq!(out_f[0].venue_seq, last);
                    model = None;
                }
                Some(_) => {
                    // Applies (contiguous or straddling last_u + 1).
                    prop_assert_eq!(sig_f, Signal::None);
                    prop_assert_eq!(out_f.len(), 2);
                    prop_assert_eq!(out_f[0].kind, EventKind::BidSet as u8);
                    prop_assert_eq!(out_f[1].kind, EventKind::AskSet as u8);
                    model = Some(last);
                }
            }
            cursor = cursor.max(last) + 1;
        }
    }
}
