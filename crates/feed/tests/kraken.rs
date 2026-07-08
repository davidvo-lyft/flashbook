//! Kraken codec integration tests: fast/slow differential over live
//! captures, hand-verified goldens, the CRC32 book-checksum oracle,
//! malformed-input hardening, and proptest fuzz.

use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;

use flashbook_feed::codec::{CodecError, Signal, SymbolTable, VenueCodec};
use flashbook_feed::kraken::{KrakenCodec, pair_decimals};
use flashbook_proto::event::flags;
use flashbook_proto::kraken_crc::kraken_book_crc32;
use flashbook_proto::{Event, EventKind, Registry, Venue};
use proptest::prelude::*;

const MONO: u64 = 111_000;
const WALL: u64 = 1_783_465_000_000_000_000;

fn kraken_table() -> SymbolTable {
    let reg = Registry::builtin();
    SymbolTable::new(
        reg.for_venue(Venue::Kraken)
            .map(|m| (m.venue_symbol.clone(), m.id)),
    )
}

fn codec() -> KrakenCodec {
    KrakenCodec::new(kraken_table())
}

fn fixture_lines(name: &str) -> Vec<String> {
    let path = format!("{}/fixtures/kraken/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {path}: {e}"))
        .lines()
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

static LIVE_BTC_ETH: LazyLock<Vec<String>> = LazyLock::new(|| fixture_lines("live-btc-eth.ndjson"));
static LIVE_5SYM: LazyLock<Vec<String>> = LazyLock::new(|| fixture_lines("live-5sym-d100.ndjson"));

/// Parse one payload with a fresh codec's fast path, unwrapping the result.
fn parse_fast(payload: &str) -> (Signal, Vec<Event>) {
    let mut c = codec();
    let mut out = Vec::new();
    let sig = c
        .parse(payload.as_bytes(), MONO, WALL, &mut out)
        .unwrap_or_else(|e| panic!("fast parse failed: {e}: {payload}"));
    (sig, out)
}

/// Parse one payload with a fresh codec's slow path, unwrapping the result.
fn parse_slow(payload: &str) -> (Signal, Vec<Event>) {
    let mut c = codec();
    let mut out = Vec::new();
    let sig = c
        .parse_slow(payload.as_bytes(), MONO, WALL, &mut out)
        .unwrap_or_else(|e| panic!("slow parse failed: {e}: {payload}"));
    (sig, out)
}

/// Assert fast and slow agree on `payload` and return the shared result.
fn parse_both(payload: &str) -> (Signal, Vec<Event>) {
    let (sf, ef) = parse_fast(payload);
    let (ss, es) = parse_slow(payload);
    assert_eq!(sf, ss, "signal mismatch: {payload}");
    assert_eq!(ef, es, "event mismatch: {payload}");
    (sf, ef)
}

// ---------------------------------------------------------------------------
// 1. Differential: fast == slow over every captured line.
// ---------------------------------------------------------------------------

fn run_differential(lines: &[String], floor: usize) {
    let mut fast = codec();
    let mut slow = codec();
    let mut total_events = 0usize;
    for (i, line) in lines.iter().enumerate() {
        let mono = MONO + i as u64;
        let wall = WALL + i as u64;
        let mut out_f = Vec::new();
        let mut out_s = Vec::new();
        let sig_f = fast
            .parse(line.as_bytes(), mono, wall, &mut out_f)
            .unwrap_or_else(|e| panic!("fast error line {i}: {e}"));
        let sig_s = slow
            .parse_slow(line.as_bytes(), mono, wall, &mut out_s)
            .unwrap_or_else(|e| panic!("slow error line {i}: {e}"));
        assert_eq!(sig_f, sig_s, "signal mismatch line {i}: {line}");
        assert_eq!(out_f, out_s, "event mismatch line {i}: {line}");
        total_events += out_f.len();
    }
    println!("differential: {} lines, {total_events} events", lines.len());
    assert!(
        total_events > floor,
        "suspiciously few events: {total_events} <= {floor}"
    );
    assert_eq!(fast.stats().fast_msgs, lines.len() as u64);
    assert_eq!(fast.stats().events, total_events as u64);
    assert_eq!(slow.stats().events, total_events as u64);
}

#[test]
fn differential_live_btc_eth() {
    run_differential(&LIVE_BTC_ETH, 5_000);
}

#[test]
fn differential_live_5sym_d100() {
    run_differential(&LIVE_5SYM, 20_000);
}

// ---------------------------------------------------------------------------
// 2. Goldens: real fixture lines with hand-verified fields.
// ---------------------------------------------------------------------------

#[test]
fn golden_btc_snapshot() {
    // Line 7 of live-btc-eth.ndjson (first BTC/USD book snapshot).
    let line = LIVE_BTC_ETH
        .iter()
        .find(|l| l.contains("\"type\":\"snapshot\"") && l.contains("BTC/USD"))
        .unwrap();
    let (sig, out) = parse_both(line);
    assert_eq!(sig, Signal::None);
    // Clear + SnapBegin + 10 SnapBid + 10 SnapAsk + SnapEnd
    assert_eq!(out.len(), 23);
    for (i, e) in out.iter().enumerate() {
        assert_eq!(e.recv_mono_ns, MONO, "event {i}");
        assert_eq!(e.recv_wall_ns, WALL, "event {i}");
        assert_eq!(e.venue_ts_ns, 0, "snapshots carry no venue_ts (event {i})");
        assert_eq!(e.venue_seq, 0, "event {i}");
        assert_eq!(e.instrument, 11, "event {i}"); // BTC/USD on Kraken
        assert_eq!(e.venue, Venue::Kraken as u8, "event {i}");
        assert_eq!(e.flags, flags::FROM_SNAPSHOT, "event {i}");
        assert_eq!(e.rsvd, 0, "event {i}");
    }
    assert_eq!(out[0].kind, EventKind::Clear as u8);
    assert_eq!(out[1].kind, EventKind::SnapBegin as u8);
    assert_eq!(out[1].aux, 20);
    // best bid: {"price":63501.6,"qty":0.63889204}
    assert_eq!(out[2].kind, EventKind::SnapBid as u8);
    assert_eq!(out[2].price, 6_350_160_000_000);
    assert_eq!(out[2].qty, 63_889_204);
    // last bid: {"price":63495.2,"qty":0.05990000}
    assert_eq!(out[11].kind, EventKind::SnapBid as u8);
    assert_eq!(out[11].price, 6_349_520_000_000);
    assert_eq!(out[11].qty, 5_990_000);
    // best ask: {"price":63501.9,"qty":0.00100000}
    assert_eq!(out[12].kind, EventKind::SnapAsk as u8);
    assert_eq!(out[12].price, 6_350_190_000_000);
    assert_eq!(out[12].qty, 100_000);
    assert_eq!(out[22].kind, EventKind::SnapEnd as u8);
    assert_eq!(out[22].aux, 423_379_305); // venue checksum
}

#[test]
fn golden_eth_book_update() {
    // Verbatim line 9 of live-btc-eth.ndjson.
    let line = r#"{"channel":"book","type":"update","data":[{"symbol":"ETH/USD","bids":[],"asks":[{"price":1774.14,"qty":2.00130641}],"checksum":3356266482,"timestamp":"2026-07-07T23:00:03.977459Z"}]}"#;
    let (sig, out) = parse_both(line);
    assert_eq!(sig, Signal::None);
    assert_eq!(out.len(), 2);
    let ask = &out[0];
    assert_eq!(ask.kind, EventKind::AskSet as u8);
    assert_eq!(ask.instrument, 12); // ETH/USD on Kraken
    assert_eq!(ask.price, 177_414_000_000); // 1774.14 at 1e-8
    assert_eq!(ask.qty, 200_130_641);
    assert_eq!(ask.venue_ts_ns, 1_783_465_203_977_459_000); // python datetime
    assert_eq!(ask.venue_seq, 0);
    assert_eq!(ask.flags, 0);
    let ck = &out[1];
    assert_eq!(ck.kind, EventKind::Checksum as u8);
    assert_eq!(ck.aux, 3_356_266_482);
    assert_eq!(ck.venue_ts_ns, 1_783_465_203_977_459_000);
    assert_eq!(ck.instrument, 12);
    assert_eq!(ck.price, 0);
    assert_eq!(ck.qty, 0);
}

#[test]
fn golden_eth_trade() {
    // Verbatim trade line from live-btc-eth.ndjson.
    let line = r#"{"channel":"trade","type":"update","data":[{"symbol":"ETH/USD","side":"buy","price":1774.14,"qty":0.00147959,"ord_type":"limit","trade_id":64175815,"timestamp":"2026-07-07T23:00:04.585581Z"}]}"#;
    let (sig, out) = parse_both(line);
    assert_eq!(sig, Signal::None);
    assert_eq!(out.len(), 1);
    let t = &out[0];
    assert_eq!(t.kind, EventKind::Trade as u8);
    assert_eq!(t.instrument, 12);
    assert_eq!(t.price, 177_414_000_000);
    assert_eq!(t.qty, 147_959);
    assert_eq!(t.aux, 64_175_815);
    assert_eq!(t.venue_seq, 64_175_815);
    assert_eq!(t.venue_ts_ns, 1_783_465_204_585_581_000); // python datetime
    assert_eq!(t.flags, 0, "buy => taker bought, no TAKER_SELL");
    assert_eq!(t.venue, Venue::Kraken as u8);
}

// ---------------------------------------------------------------------------
// 3. The CRC32 oracle: replayed book state must reproduce every checksum
//    the venue sent (snapshots and updates).
// ---------------------------------------------------------------------------

/// `(price, qty)` mantissa levels, best-first, as [`kraken_book_crc32`] wants.
type Levels = Vec<(i64, i64)>;

#[derive(Default)]
struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl Book {
    fn set(side: &mut BTreeMap<i64, i64>, price: i64, qty: i64) {
        if qty == 0 {
            side.remove(&price);
        } else {
            side.insert(price, qty);
        }
    }

    /// Kraken truncates the client book to the subscription depth: when an
    /// insert pushes a side past `depth`, the worst levels fall off (the
    /// venue re-sends them if they come back into range).
    fn truncate(&mut self, depth: usize) {
        while self.bids.len() > depth {
            self.bids.pop_first(); // lowest bid is worst
        }
        while self.asks.len() > depth {
            self.asks.pop_last(); // highest ask is worst
        }
    }

    fn top10(&self) -> (Levels, Levels) {
        let asks = self.asks.iter().take(10).map(|(&p, &q)| (p, q)).collect();
        let bids = self
            .bids
            .iter()
            .rev()
            .take(10)
            .map(|(&p, &q)| (p, q))
            .collect();
        (asks, bids)
    }
}

/// Replay `lines` through the fast path, mirror book state from the emitted
/// events, and assert every venue checksum (SnapEnd + Checksum events)
/// against [`kraken_book_crc32`]. Returns the number of checks performed.
fn run_crc_oracle(lines: &[String], depth: usize) -> usize {
    let reg = Registry::builtin();
    let mut c = codec();
    let mut books: HashMap<u32, Book> = HashMap::new();
    let mut out = Vec::new();
    let mut checks = 0usize;
    for (i, line) in lines.iter().enumerate() {
        out.clear();
        c.parse(line.as_bytes(), MONO, WALL, &mut out)
            .unwrap_or_else(|e| panic!("parse error line {i}: {e}"));
        for e in &out {
            let book = books.entry(e.instrument).or_default();
            match e.kind().unwrap() {
                EventKind::Clear => {
                    book.bids.clear();
                    book.asks.clear();
                }
                EventKind::SnapBid | EventKind::BidSet => {
                    Book::set(&mut book.bids, e.price, e.qty);
                }
                EventKind::SnapAsk | EventKind::AskSet => {
                    Book::set(&mut book.asks, e.price, e.qty);
                }
                EventKind::SnapEnd | EventKind::Checksum => {
                    book.truncate(depth);
                    let sym = &reg.get(e.instrument).unwrap().venue_symbol;
                    let (pd, qd) = pair_decimals(sym).unwrap();
                    let (asks, bids) = book.top10();
                    let crc = kraken_book_crc32(&asks, &bids, pd, qd);
                    assert_eq!(
                        u64::from(crc),
                        e.aux,
                        "checksum mismatch line {i} sym {sym}: {line}"
                    );
                    checks += 1;
                }
                _ => {}
            }
        }
    }
    checks
}

#[test]
fn crc_oracle_live_btc_eth() {
    let checks = run_crc_oracle(&LIVE_BTC_ETH, 10);
    println!("crc oracle (btc-eth, depth 10): {checks} checksums verified");
    assert!(checks > 2_900, "expected thousands of checks, got {checks}");
}

#[test]
fn crc_oracle_live_5sym_d100() {
    let checks = run_crc_oracle(&LIVE_5SYM, 100);
    println!("crc oracle (5sym, depth 100): {checks} checksums verified");
    assert!(checks > 5_000, "expected thousands of checks, got {checks}");
}

// ---------------------------------------------------------------------------
// 4. Protocol / control-plane behaviour.
// ---------------------------------------------------------------------------

#[test]
fn control_plane_signals() {
    let status = r#"{"channel":"status","type":"update","data":[{"version":"2.0.10","system":"online","api_version":"v2","connection_id":1}]}"#;
    let ack = r#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2026-07-07T23:00:03.501510Z","time_out":"2026-07-07T23:00:03.501553Z"}"#;
    let nack = r#"{"method":"subscribe","result":{"channel":"book","symbol":"NOPE/USD"},"success":false,"error":"Currency pair not supported"}"#;
    for line in [status, ack, nack] {
        let (sig, out) = parse_both(line);
        assert_eq!(sig, Signal::Control, "{line}");
        assert!(out.is_empty(), "{line}");
    }
    // channels we deliberately don't handle
    let ticker =
        r#"{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","last":63500.0}]}"#;
    let (sig, out) = parse_both(ticker);
    assert_eq!(sig, Signal::Ignored);
    assert!(out.is_empty());
}

#[test]
fn heartbeat_emits_connection_level_event() {
    let (sig, out) = parse_both(r#"{"channel":"heartbeat"}"#);
    assert_eq!(sig, Signal::None);
    assert_eq!(out.len(), 1);
    let e = &out[0];
    assert_eq!(e.kind, EventKind::Heartbeat as u8);
    assert_eq!(e.instrument, 0, "connection-level, not per-instrument");
    assert_eq!(e.venue_ts_ns, 0);
    assert_eq!(e.venue_seq, 0);
    assert_eq!((e.price, e.qty, e.aux), (0, 0, 0));
    assert_eq!(e.flags, 0);
    assert_eq!(e.venue, Venue::Kraken as u8);
    assert_eq!(e.recv_mono_ns, MONO);
    assert_eq!(e.recv_wall_ns, WALL);
}

#[test]
fn trade_snapshot_flag_and_taker_sell() {
    // Historical trades sent on subscribe are type=snapshot; side is the
    // taker side, so "sell" sets TAKER_SELL.
    let line = r#"{"channel":"trade","type":"snapshot","data":[{"symbol":"BTC/USD","side":"sell","price":63500.1,"qty":0.5,"ord_type":"market","trade_id":42,"timestamp":"2026-07-07T23:00:04.585581Z"},{"symbol":"BTC/USD","side":"buy","price":63500.2,"qty":0.25,"ord_type":"limit","trade_id":43,"timestamp":"2026-07-07T23:00:04.585581Z"}]}"#;
    let (sig, out) = parse_both(line);
    assert_eq!(sig, Signal::None);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].flags, flags::FROM_SNAPSHOT | flags::TAKER_SELL);
    assert_eq!(out[0].price, 6_350_010_000_000);
    assert_eq!(out[0].qty, 50_000_000);
    assert_eq!(out[0].aux, 42);
    assert_eq!(out[1].flags, flags::FROM_SNAPSHOT);
    assert_eq!(out[1].aux, 43);
}

#[test]
fn multi_entry_book_update_preserves_order() {
    // One frame, two symbols; per entry: bids, then asks, then checksum.
    // Also exercises qty 0 (level delete).
    let line = r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":63501.6,"qty":0.00000000}],"asks":[{"price":63501.9,"qty":1.5}],"checksum":111,"timestamp":"2026-07-07T23:00:03.977459Z"},{"symbol":"ETH/USD","bids":[{"price":1774.13,"qty":2.0}],"asks":[],"checksum":222,"timestamp":"2026-07-07T23:00:04.585581Z"}]}"#;
    let (sig, out) = parse_both(line);
    assert_eq!(sig, Signal::None);
    let kinds: Vec<u8> = out.iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        vec![
            EventKind::BidSet as u8,
            EventKind::AskSet as u8,
            EventKind::Checksum as u8,
            EventKind::BidSet as u8,
            EventKind::Checksum as u8,
        ]
    );
    assert_eq!(out[0].instrument, 11);
    assert_eq!(out[0].qty, 0, "qty 0 deletes the level");
    assert_eq!(out[1].qty, 150_000_000);
    assert_eq!(out[2].aux, 111);
    assert_eq!(out[2].venue_ts_ns, 1_783_465_203_977_459_000);
    assert_eq!(out[3].instrument, 12);
    assert_eq!(out[3].venue_ts_ns, 1_783_465_204_585_581_000);
    assert_eq!(out[4].aux, 222);
}

/// Legal-JSON whitespace after `"price":` / `"qty":` must not defeat the
/// slow path's number-quoting pre-pass: a reformatted frame parses via
/// parse_slow identically to its compact form. (The fast path's exact
/// byte-literal matching rejects the spaced form by design; it falls back
/// to the slow path in production.)
#[test]
fn slow_path_tolerates_whitespace_after_price_qty_keys() {
    let compact = r#"{"channel":"book","type":"update","data":[{"symbol":"ETH/USD","bids":[{"price":1774.13,"qty":2.0}],"asks":[{"price":1774.14,"qty":2.00130641}],"checksum":3356266482,"timestamp":"2026-07-07T23:00:03.977459Z"}]}"#;
    let spaced = r#"{"channel":"book","type":"update","data":[{"symbol":"ETH/USD","bids":[{"price": 1774.13,"qty":	2.0}],"asks":[{"price":  1774.14,"qty":
2.00130641}],"checksum":3356266482,"timestamp":"2026-07-07T23:00:03.977459Z"}]}"#;
    let (sig_c, out_c) = parse_both(compact);
    let (sig_s, out_s) = parse_slow(spaced);
    assert_eq!(sig_c, sig_s);
    assert_eq!(out_c, out_s, "spaced book update must parse identically");

    let compact = r#"{"channel":"trade","type":"update","data":[{"symbol":"ETH/USD","side":"buy","price":1774.14,"qty":0.00147959,"ord_type":"limit","trade_id":64175815,"timestamp":"2026-07-07T23:00:04.585581Z"}]}"#;
    let spaced = r#"{"channel":"trade","type":"update","data":[{"symbol":"ETH/USD","side":"buy","price": 1774.14,"qty": 0.00147959,"ord_type":"limit","trade_id":64175815,"timestamp":"2026-07-07T23:00:04.585581Z"}]}"#;
    let (sig_c, out_c) = parse_both(compact);
    let (sig_s, out_s) = parse_slow(spaced);
    assert_eq!(sig_c, sig_s);
    assert_eq!(out_c, out_s, "spaced trade must parse identically");
}

// ---------------------------------------------------------------------------
// 5. Malformed input: correct errors, no panics, no partial events.
// ---------------------------------------------------------------------------

#[test]
fn unknown_symbol_is_an_error_in_both_paths() {
    let book = r#"{"channel":"book","type":"update","data":[{"symbol":"ADA/USD","bids":[],"asks":[{"price":1.0,"qty":1.0}],"checksum":1,"timestamp":"2026-07-07T23:00:03.977459Z"}]}"#;
    let trade = r#"{"channel":"trade","type":"update","data":[{"symbol":"ADA/USD","side":"buy","price":1.0,"qty":1.0,"ord_type":"limit","trade_id":1,"timestamp":"2026-07-07T23:00:03.977459Z"}]}"#;
    for line in [book, trade] {
        let mut c = codec();
        let mut out = Vec::new();
        assert!(matches!(
            c.parse(line.as_bytes(), MONO, WALL, &mut out),
            Err(CodecError::UnknownInstrument)
        ));
        assert!(out.is_empty(), "no partial events: {line}");
        assert!(matches!(
            c.parse_slow(line.as_bytes(), MONO, WALL, &mut out),
            Err(CodecError::UnknownInstrument)
        ));
        assert!(out.is_empty(), "no partial events (slow): {line}");
    }
}

#[test]
fn malformed_inputs_error_cleanly() {
    let cases: &[&str] = &[
        // truncated mid-message
        r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":63501.6,"#,
        // bad number token
        r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":1.2.3,"qty":1.0}],"asks":[],"checksum":1,"timestamp":"2026-07-07T23:00:03.977459Z"}]}"#,
        // wrong-type checksum
        r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],"asks":[],"checksum":"abc","timestamp":"2026-07-07T23:00:03.977459Z"}]}"#,
        // update missing its timestamp
        r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],"asks":[{"price":1.0,"qty":1.0}],"checksum":1}]}"#,
        // bogus timestamp
        r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],"asks":[],"checksum":1,"timestamp":"2026-13-99T99:99:99Z"}]}"#,
        // unknown message type
        r#"{"channel":"book","type":"delta","data":[]}"#,
        // bad trade side
        r#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"hold","price":1.0,"qty":1.0,"ord_type":"limit","trade_id":1,"timestamp":"2026-07-07T23:00:03.977459Z"}]}"#,
        // not JSON at all
        "hello world",
        "",
        // precision finer than 1e-8
        r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":1.000000001,"qty":1.0}],"asks":[],"checksum":1,"timestamp":"2026-07-07T23:00:03.977459Z"}]}"#,
    ];
    let sentinel = Event {
        recv_mono_ns: 7,
        kind: EventKind::Heartbeat as u8,
        venue: Venue::Kraken as u8,
        ..Event::ZERO
    };
    for line in cases {
        let mut c = codec();
        let mut out = vec![sentinel];
        assert!(
            c.parse(line.as_bytes(), MONO, WALL, &mut out).is_err(),
            "fast should error: {line}"
        );
        assert_eq!(out, vec![sentinel], "buffer restored (fast): {line}");
        assert!(
            c.parse_slow(line.as_bytes(), MONO, WALL, &mut out).is_err(),
            "slow should error: {line}"
        );
        assert_eq!(out, vec![sentinel], "buffer restored (slow): {line}");
    }
}

#[test]
fn buffer_restored_after_mid_message_failure() {
    // First entry parses fine; the second entry's ask qty is malformed, so
    // the already-emitted BTC events must be rolled back.
    let line = r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":63501.6,"qty":1.0}],"asks":[],"checksum":111,"timestamp":"2026-07-07T23:00:03.977459Z"},{"symbol":"ETH/USD","bids":[],"asks":[{"price":1774.14,"qty":1..2}],"checksum":222,"timestamp":"2026-07-07T23:00:03.977459Z"}]}"#;
    let mut c = codec();
    let mut out = Vec::new();
    assert!(c.parse(line.as_bytes(), MONO, WALL, &mut out).is_err());
    assert!(out.is_empty(), "partial BTC events must be rolled back");
    assert!(c.parse_slow(line.as_bytes(), MONO, WALL, &mut out).is_err());
    assert!(
        out.is_empty(),
        "partial BTC events must be rolled back (slow)"
    );
}

// ---------------------------------------------------------------------------
// 6. Codec metadata.
// ---------------------------------------------------------------------------

#[test]
fn pair_decimals_match_asset_pairs() {
    assert_eq!(pair_decimals("BTC/USD"), Some((1, 8)));
    assert_eq!(pair_decimals("ETH/USD"), Some((2, 8)));
    assert_eq!(pair_decimals("SOL/USD"), Some((2, 8)));
    assert_eq!(pair_decimals("XRP/USD"), Some((5, 8)));
    assert_eq!(pair_decimals("DOGE/USD"), Some((7, 8)));
    assert_eq!(pair_decimals("ADA/USD"), None);
    assert_eq!(pair_decimals("XBT/USD"), None, "v1 names are not used");
}

#[test]
fn ws_url_and_subscribe_messages() {
    let c = codec();
    assert_eq!(c.venue(), Venue::Kraken);
    assert_eq!(c.ws_url(), "wss://ws.kraken.com/v2");
    let msgs = c.subscribe_messages();
    assert_eq!(msgs.len(), 2);
    let book: serde_json::Value = serde_json::from_str(&msgs[0]).unwrap();
    assert_eq!(book["method"], "subscribe");
    assert_eq!(book["params"]["channel"], "book");
    assert_eq!(book["params"]["depth"], 100);
    let syms: Vec<&str> = book["params"]["symbol"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(
        syms,
        ["BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "DOGE/USD"]
    );
    let trade: serde_json::Value = serde_json::from_str(&msgs[1]).unwrap();
    assert_eq!(trade["params"]["channel"], "trade");
    assert_eq!(trade["params"]["symbol"], book["params"]["symbol"]);
    assert!(trade["params"].get("depth").is_none());
    // Matches the capture configuration: no historical-trade snapshot.
    assert_eq!(trade["params"]["snapshot"], false);
    assert!(
        book["params"].get("snapshot").is_none(),
        "book subscription unchanged"
    );
}

#[test]
fn stats_count_fast_msgs_and_events() {
    let mut c = codec();
    let mut out = Vec::new();
    c.parse(br#"{"channel":"heartbeat"}"#, MONO, WALL, &mut out)
        .unwrap();
    let bad = br#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","#;
    assert!(c.parse(bad, MONO, WALL, &mut out).is_err());
    c.parse_slow(br#"{"channel":"heartbeat"}"#, MONO, WALL, &mut out)
        .unwrap();
    assert_eq!(c.stats().fast_msgs, 1, "errors don't count as fast msgs");
    assert_eq!(c.stats().events, 2);
    assert_eq!(c.stats().gaps, 0, "kraken v2 has no seq numbers, no gaps");
}

// ---------------------------------------------------------------------------
// 7. Proptest fuzz: mutations/truncations of real lines never panic.
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn fuzz_byte_mutations_never_panic(
        idx in 0usize..3000,
        pos in 0usize..4096,
        byte in any::<u8>(),
    ) {
        let line = &LIVE_BTC_ETH[idx % LIVE_BTC_ETH.len()];
        let mut bytes = line.clone().into_bytes();
        let p = pos % bytes.len();
        bytes[p] = byte;
        let mut c = codec();
        let mut out = Vec::new();
        let _ = c.parse(&bytes, MONO, WALL, &mut out);
        let mut out = Vec::new();
        let _ = c.parse_slow(&bytes, MONO, WALL, &mut out);
    }

    #[test]
    fn fuzz_truncations_never_panic(
        idx in 0usize..3000,
        cut in 0usize..4096,
    ) {
        let line = &LIVE_BTC_ETH[idx % LIVE_BTC_ETH.len()];
        let bytes = &line.as_bytes()[..cut.min(line.len())];
        let mut c = codec();
        let mut out = Vec::new();
        let _ = c.parse(bytes, MONO, WALL, &mut out);
        let mut out = Vec::new();
        let _ = c.parse_slow(bytes, MONO, WALL, &mut out);
    }
}
