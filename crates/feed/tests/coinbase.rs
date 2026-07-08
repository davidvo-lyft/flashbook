//! Integration tests for the Coinbase codec: fast/slow differential over the
//! captured fixture, hand-verified goldens, gap state machines, malformed
//! inputs, and proptest fuzz.

use std::sync::OnceLock;

use flashbook_feed::coinbase::CoinbaseCodec;
use flashbook_feed::{CodecError, Signal, SymbolTable, VenueCodec};
use flashbook_proto::event::flags;
use flashbook_proto::{Event, EventKind, Registry, Venue};
use proptest::prelude::*;

const FIXTURE_WS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/coinbase/live-btc-eth.ndjson"
);
const FIXTURE_REST: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/coinbase/rest-book-btc.json"
);

/// Symbol table with the fixture's products, ids from the builtin registry
/// (BTC-USD = 1, ETH-USD = 2).
fn table() -> SymbolTable {
    let reg = Registry::builtin();
    SymbolTable::new(
        reg.for_venue(Venue::Coinbase)
            .filter(|m| m.venue_symbol == "BTC-USD" || m.venue_symbol == "ETH-USD")
            .map(|m| (m.venue_symbol.clone(), m.id)),
    )
}

fn codec() -> CoinbaseCodec {
    CoinbaseCodec::new(table())
}

fn fixture_lines() -> &'static Vec<Vec<u8>> {
    static LINES: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    LINES.get_or_init(|| {
        std::fs::read(FIXTURE_WS)
            .expect("fixture readable")
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .map(<[u8]>::to_vec)
            .collect()
    })
}

/// First fixture line whose `"type"` equals `ty`.
fn first_line_of_type(ty: &str) -> &'static [u8] {
    let needle = format!("\"type\":\"{ty}\"");
    fixture_lines()
        .iter()
        .find(|l| l.windows(needle.len()).any(|w| w == needle.as_bytes()))
        .map(Vec::as_slice)
        .unwrap_or_else(|| panic!("no {ty} line in fixture"))
}

/// Run one payload through fresh fast and slow codecs; assert identical
/// output and return the fast result.
fn parse_both(payload: &[u8], mono: u64, wall: u64) -> (Signal, Vec<Event>) {
    let mut fast = codec();
    let mut slow = codec();
    let mut out_f = Vec::new();
    let mut out_s = Vec::new();
    let sig_f = fast
        .parse(payload, mono, wall, &mut out_f)
        .expect("fast ok");
    let sig_s = slow
        .parse_slow(payload, mono, wall, &mut out_s)
        .expect("slow ok");
    assert_eq!(sig_f, sig_s);
    assert_eq!(out_f, out_s);
    (sig_f, out_f)
}

/// Feed a sequence of payloads through one fast and one slow codec (shared
/// state across the sequence) and assert per-message identical output.
/// Returns the fast codec's per-message event vectors, per-message signals,
/// and final gap count.
fn run_sequence(payloads: &[&[u8]]) -> (Vec<Vec<Event>>, Vec<Signal>, u64) {
    let mut fast = codec();
    let mut slow = codec();
    let mut all = Vec::new();
    let mut sigs = Vec::new();
    for (i, p) in payloads.iter().enumerate() {
        let mono = 100 + i as u64;
        let wall = 200 + i as u64;
        let mut out_f = Vec::new();
        let mut out_s = Vec::new();
        let sig_f = fast.parse(p, mono, wall, &mut out_f).expect("fast ok");
        let sig_s = slow.parse_slow(p, mono, wall, &mut out_s).expect("slow ok");
        assert_eq!(sig_f, sig_s, "signal mismatch at msg {i}");
        assert_eq!(out_f, out_s, "events mismatch at msg {i}");
        all.push(out_f);
        sigs.push(sig_f);
    }
    assert_eq!(fast.stats().gaps, slow.stats().gaps);
    let gaps = fast.stats().gaps;
    (all, sigs, gaps)
}

fn synth_match(ty: &str, trade_id: u64, side: &str, product: &str, seq: u64) -> String {
    format!(
        "{{\"type\":\"{ty}\",\"trade_id\":{trade_id},\
         \"maker_order_id\":\"6b380eba-fd1d-43a8-91a0-fdb06e6ce87f\",\
         \"taker_order_id\":\"b3639a38-ae66-493c-96ba-d2319e503590\",\
         \"side\":\"{side}\",\"size\":\"0.5\",\"price\":\"100.25\",\
         \"product_id\":\"{product}\",\"sequence\":{seq},\
         \"time\":\"2026-07-07T22:58:17Z\"}}"
    )
}

fn synth_heartbeat(last_trade_id: u64, product: &str, seq: u64) -> String {
    format!(
        "{{\"type\":\"heartbeat\",\"last_trade_id\":{last_trade_id},\
         \"product_id\":\"{product}\",\"sequence\":{seq},\
         \"time\":\"2026-07-07T22:58:18Z\"}}"
    )
}

/// Count occurrences of a byte-pair needle (each snapshot level opens with
/// `["`, so this recounts levels without any JSON parser).
fn count_sub(hay: &[u8], needle: &[u8]) -> usize {
    hay.windows(needle.len()).filter(|w| w == &needle).count()
}

fn find_sub(hay: &[u8], needle: &[u8]) -> usize {
    hay.windows(needle.len())
        .position(|w| w == needle)
        .expect("needle present")
}

#[test]
fn venue_url_and_subscribe_message() {
    let c = codec();
    assert_eq!(c.venue(), Venue::Coinbase);
    assert_eq!(c.ws_url(), "wss://ws-feed.exchange.coinbase.com");
    let subs = c.subscribe_messages();
    assert_eq!(subs.len(), 1);
    let v: serde_json::Value = serde_json::from_str(&subs[0]).expect("valid json");
    assert_eq!(v["type"], "subscribe");
    let ids: Vec<&str> = v["product_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect();
    assert!(ids.contains(&"BTC-USD") && ids.contains(&"ETH-USD"));
    assert_eq!(
        v["channels"],
        serde_json::json!(["level2_batch", "matches", "heartbeat"])
    );
}

/// DIFFERENTIAL: every fixture line through parse() (codec A) and
/// parse_slow() (codec B) must yield identical events and signals, with
/// zero errors overall.
#[test]
fn differential_fast_vs_slow_over_full_fixture() {
    let lines = fixture_lines();
    assert_eq!(lines.len(), 2456);
    let mut fast = codec();
    let mut slow = codec();
    let mut total_events = 0usize;
    for (i, line) in lines.iter().enumerate() {
        let mono = 7 * i as u64 + 1;
        let wall = 7 * i as u64 + 2;
        let mut out_f = Vec::new();
        let mut out_s = Vec::new();
        let sig_f = fast
            .parse(line, mono, wall, &mut out_f)
            .unwrap_or_else(|e| panic!("fast error on line {}: {e}", i + 1));
        let sig_s = slow
            .parse_slow(line, mono, wall, &mut out_s)
            .unwrap_or_else(|e| panic!("slow error on line {}: {e}", i + 1));
        assert_eq!(sig_f, sig_s, "signal mismatch on line {}", i + 1);
        assert_eq!(out_f, out_s, "event mismatch on line {}", i + 1);
        total_events += out_f.len();
    }
    assert_eq!(fast.stats().fast_msgs, lines.len() as u64);
    assert_eq!(fast.stats().events, slow.stats().events);
    assert_eq!(fast.stats().gaps, slow.stats().gaps);
    println!(
        "differential: {} lines, {} events, {} gaps",
        lines.len(),
        total_events,
        fast.stats().gaps
    );
    assert!(total_events > 50_000, "sanity floor: got {total_events}");
}

/// GOLDEN: first real `match` line, every field hand-verified.
/// `{"type":"match","trade_id":1052119289,...,"side":"sell","size":"0.2",
///   "price":"63514.6","product_id":"BTC-USD","sequence":132235026010,
///   "time":"2026-07-07T22:58:17.164892Z"}`
#[test]
fn golden_match_line() {
    let line = first_line_of_type("match");
    let (sig, out) = parse_both(line, 11, 22);
    assert_eq!(sig, Signal::None);
    assert_eq!(
        out,
        vec![Event {
            recv_mono_ns: 11,
            recv_wall_ns: 22,
            venue_ts_ns: 1_783_465_097_164_892_000,
            venue_seq: 132_235_026_010,
            price: 6_351_460_000_000, // "63514.6"
            qty: 20_000_000,          // "0.2"
            aux: 1_052_119_289,       // trade_id
            instrument: 1,            // BTC-USD
            kind: EventKind::Trade as u8,
            venue: Venue::Coinbase as u8,
            flags: 0, // maker side "sell" => taker BOUGHT => no TAKER_SELL
            rsvd: 0,
        }]
    );
}

/// GOLDEN: first real `l2update` line (44 interleaved changes, BTC-USD,
/// time 2026-07-07T22:58:17.068248Z). Mantissas hand-verified.
#[test]
fn golden_l2update_line() {
    let line = first_line_of_type("l2update");
    let (sig, out) = parse_both(line, 33, 44);
    assert_eq!(sig, Signal::None);
    assert_eq!(out.len(), 44);
    // changes[0] = ["buy","63507.67","0.00000000"] -> BidSet, qty 0 deletes
    assert_eq!(
        out[0],
        Event {
            recv_mono_ns: 33,
            recv_wall_ns: 44,
            venue_ts_ns: 1_783_465_097_068_248_000,
            venue_seq: 0,
            price: 6_350_767_000_000, // "63507.67"
            qty: 0,
            aux: 0,
            instrument: 1,
            kind: EventKind::BidSet as u8,
            venue: Venue::Coinbase as u8,
            flags: 0,
            rsvd: 0,
        }
    );
    // changes[17] = ["sell","63543.00","2.00000000"] -> AskSet; payload
    // order (buys then sells here) must be preserved as-is.
    assert_eq!(out[17].kind, EventKind::AskSet as u8);
    assert_eq!(out[17].price, 6_354_300_000_000);
    assert_eq!(out[17].qty, 200_000_000);
    for (i, e) in out.iter().enumerate() {
        let want = if i < 17 {
            EventKind::BidSet
        } else {
            EventKind::AskSet
        };
        assert_eq!(e.kind, want as u8, "change {i}");
        assert_eq!(e.venue_seq, 0);
        assert_eq!(e.flags, 0);
    }
}

/// GOLDEN: first real `heartbeat` line; fresh codec has no trade baseline,
/// so exactly one Heartbeat event and no Gap.
#[test]
fn golden_heartbeat_line() {
    let line = first_line_of_type("heartbeat");
    let (sig, out) = parse_both(line, 55, 66);
    assert_eq!(sig, Signal::None);
    assert_eq!(
        out,
        vec![Event {
            recv_mono_ns: 55,
            recv_wall_ns: 66,
            venue_ts_ns: 1_783_465_098_000_000_000, // 2026-07-07T22:58:18Z
            venue_seq: 132_235_026_653,
            price: 0,
            qty: 0,
            aux: 1_052_119_293, // last_trade_id
            instrument: 1,
            kind: EventKind::Heartbeat as u8,
            venue: Venue::Coinbase as u8,
            flags: 0,
            rsvd: 0,
        }]
    );
}

/// Snapshot bracket: Clear + SnapBegin + bids + asks + SnapEnd, canonical
/// bids-then-asks (the JSON carries asks first), FROM_SNAPSHOT everywhere,
/// and SnapBegin.aux equal to an independent textual recount of the levels.
#[test]
fn snapshot_ordering_and_level_counts() {
    let line = first_line_of_type("snapshot"); // ETH-USD snapshot
    // Independent recount: each level opens with `["`; asks precede bids in
    // the raw JSON, time trails the bids array.
    let asks_at = find_sub(line, b"\"asks\":");
    let bids_at = find_sub(line, b"\"bids\":");
    assert!(asks_at < bids_at, "fixture carries asks before bids");
    let n_asks = count_sub(&line[asks_at..bids_at], b"[\"");
    let n_bids = count_sub(&line[bids_at..], b"[\"");
    let total = n_bids + n_asks;
    assert_eq!(total, 22_768); // hand-counted fixture total

    let (sig, out) = parse_both(line, 1, 2);
    assert_eq!(sig, Signal::None);
    assert_eq!(out.len(), total + 3);
    assert_eq!(out[0].kind, EventKind::Clear as u8);
    assert_eq!(out[1].kind, EventKind::SnapBegin as u8);
    assert_eq!(out[1].aux, total as u64);
    for e in &out[2..2 + n_bids] {
        assert_eq!(e.kind, EventKind::SnapBid as u8);
    }
    for e in &out[2 + n_bids..2 + total] {
        assert_eq!(e.kind, EventKind::SnapAsk as u8);
    }
    let end = out[out.len() - 1];
    assert_eq!(end.kind, EventKind::SnapEnd as u8);
    assert_eq!(end.aux, 0); // Coinbase snapshots carry no checksum
    for e in &out {
        assert_eq!(e.flags, flags::FROM_SNAPSHOT);
        assert_eq!(e.instrument, 2); // ETH-USD
        assert_eq!(e.venue_seq, 0);
        assert_eq!(e.venue_ts_ns, 1_783_465_097_050_195_000);
    }
    // First bid ["1774.92","3.32560450"], first ask ["1774.93","0.56100580"].
    assert_eq!(out[2].price, 177_492_000_000);
    assert_eq!(out[2].qty, 332_560_450);
    assert_eq!(out[2 + n_bids].price, 177_493_000_000);
    assert_eq!(out[2 + n_bids].qty, 56_100_580);
}

/// Maker-side mapping in both directions: Coinbase's match `side` is the
/// MAKER's side, so side "buy" (bid hit) means the taker SOLD.
#[test]
fn maker_side_mapping_both_directions() {
    let maker_buy = synth_match("match", 10, "buy", "BTC-USD", 500);
    let maker_sell = synth_match("match", 11, "sell", "BTC-USD", 501);
    let (all, _sigs, gaps) = run_sequence(&[maker_buy.as_bytes(), maker_sell.as_bytes()]);
    assert_eq!(gaps, 0);
    assert_eq!(all[0].len(), 1);
    assert_eq!(all[0][0].kind, EventKind::Trade as u8);
    assert_eq!(all[0][0].flags, flags::TAKER_SELL); // maker bid hit => taker sold
    assert_eq!(all[1][0].flags, 0); // maker ask lifted => taker bought
    assert_eq!(all[0][0].price, 10_025_000_000);
    assert_eq!(all[0][0].qty, 50_000_000);
}

/// Trade-gap injection: consecutive trade ids increment by 1; a jump emits
/// Gap(aux = missed) BEFORE the Trade and signals NeedResync for the
/// instrument (the l2 book may have silently dropped messages too). First
/// match never gaps (no baseline).
#[test]
fn trade_gap_injection() {
    let m1 = synth_match("match", 100, "sell", "BTC-USD", 900);
    let m2 = synth_match("match", 105, "sell", "BTC-USD", 901);
    let (all, sigs, gaps) = run_sequence(&[m1.as_bytes(), m2.as_bytes()]);
    assert_eq!(gaps, 1);
    assert_eq!(all[0].len(), 1, "first match: baseline only, no gap");
    assert_eq!(sigs[0], Signal::None, "gap-free match: no resync");
    assert_eq!(all[1].len(), 2);
    assert_eq!(all[1][0].kind, EventKind::Gap as u8);
    assert_eq!(all[1][0].aux, 4); // 101..=104 missed
    assert_eq!(all[1][0].venue_seq, 901);
    assert_eq!(all[1][0].flags, 0);
    assert_eq!(all[1][1].kind, EventKind::Trade as u8);
    assert_eq!(all[1][1].aux, 105);
    assert_eq!(sigs[1], Signal::NeedResync { instrument: 1 });

    // Gap tracking is per instrument: an ETH trade doesn't disturb BTC.
    let m3 = synth_match("match", 7, "sell", "ETH-USD", 1);
    let m4 = synth_match("match", 106, "sell", "BTC-USD", 902);
    let (all2, sigs2, gaps2) = run_sequence(&[m1.as_bytes(), m3.as_bytes(), m4.as_bytes()]);
    assert_eq!(gaps2, 1); // 101..=105 missed on BTC
    assert_eq!(all2[2][0].kind, EventKind::Gap as u8);
    assert_eq!(all2[2][0].aux, 5);
    assert_eq!(sigs2[2], Signal::NeedResync { instrument: 1 });
}

/// Out-of-order matches (documented by Coinbase: lower trade ids "can be
/// ignored or represent a message that has arrived out of order") must
/// never roll the baseline backwards: the sequence 100, 102, 101, 103
/// produces exactly ONE gap (at 102, one resync), the straggler 101 is
/// still emitted as a Trade, and 103 is contiguous with the advanced
/// baseline.
#[test]
fn out_of_order_matches_report_gap_once() {
    let m100 = synth_match("match", 100, "sell", "BTC-USD", 1);
    let m102 = synth_match("match", 102, "sell", "BTC-USD", 2);
    let m101 = synth_match("match", 101, "sell", "BTC-USD", 3);
    let m103 = synth_match("match", 103, "sell", "BTC-USD", 4);
    let (all, sigs, gaps) = run_sequence(&[
        m100.as_bytes(),
        m102.as_bytes(),
        m101.as_bytes(),
        m103.as_bytes(),
    ]);
    assert_eq!(gaps, 1, "exactly one gap for 100,102,101,103");
    assert_eq!(all[0].len(), 1, "baseline seed");
    assert_eq!(sigs[0], Signal::None);
    assert_eq!(all[1].len(), 2, "102 skips 101: gap + trade");
    assert_eq!(all[1][0].kind, EventKind::Gap as u8);
    assert_eq!(all[1][0].aux, 1);
    assert_eq!(sigs[1], Signal::NeedResync { instrument: 1 });
    assert_eq!(all[2].len(), 1, "straggler 101: trade only, no new gap");
    assert_eq!(all[2][0].kind, EventKind::Trade as u8);
    assert_eq!(all[2][0].aux, 101);
    assert_eq!(sigs[2], Signal::None);
    assert_eq!(all[3].len(), 1, "103 contiguous with baseline 102");
    assert_eq!(all[3][0].kind, EventKind::Trade as u8);
    assert_eq!(sigs[3], Signal::None);
}

/// A regressed trade id followed by a heartbeat must not double-report the
/// already-reported gap: before the advance-only fix, the straggler lowered
/// the baseline to 101 and hb(102) re-reported the same missing trade.
#[test]
fn out_of_order_match_then_heartbeat_reports_once() {
    let m100 = synth_match("match", 100, "sell", "BTC-USD", 1);
    let m102 = synth_match("match", 102, "sell", "BTC-USD", 2);
    let m101 = synth_match("match", 101, "sell", "BTC-USD", 3);
    let hb102 = synth_heartbeat(102, "BTC-USD", 4);
    let (all, sigs, gaps) = run_sequence(&[
        m100.as_bytes(),
        m102.as_bytes(),
        m101.as_bytes(),
        hb102.as_bytes(),
    ]);
    assert_eq!(gaps, 1, "the 101 gap is reported once, at m102");
    assert_eq!(sigs[1], Signal::NeedResync { instrument: 1 });
    assert_eq!(all[3].len(), 1, "heartbeat alone: no re-reported gap");
    assert_eq!(all[3][0].kind, EventKind::Heartbeat as u8);
    assert_eq!(sigs[3], Signal::None);
}

/// Heartbeat cross-check: last_trade_id ahead of the last seen trade emits
/// Gap AFTER the Heartbeat and signals NeedResync; once reported, the
/// baseline advances so the same gap is not re-reported. No baseline (no
/// matches yet) => no gap.
#[test]
fn heartbeat_gap_detection() {
    let hb_cold = synth_heartbeat(999, "BTC-USD", 10);
    let m = synth_match("match", 100, "sell", "BTC-USD", 11);
    let hb_ok = synth_heartbeat(100, "BTC-USD", 12);
    let hb_ahead = synth_heartbeat(103, "BTC-USD", 13);
    let hb_again = synth_heartbeat(103, "BTC-USD", 14);
    let (all, sigs, gaps) = run_sequence(&[
        hb_cold.as_bytes(),
        m.as_bytes(),
        hb_ok.as_bytes(),
        hb_ahead.as_bytes(),
        hb_again.as_bytes(),
    ]);
    assert_eq!(gaps, 1);
    assert_eq!(all[0].len(), 1, "no baseline: heartbeat alone");
    assert_eq!(sigs[0], Signal::None);
    assert_eq!(all[2].len(), 1, "matching last_trade_id: no gap");
    assert_eq!(sigs[2], Signal::None, "gap-free heartbeat: no resync");
    assert_eq!(all[3].len(), 2);
    assert_eq!(all[3][0].kind, EventKind::Heartbeat as u8);
    assert_eq!(all[3][0].aux, 103);
    assert_eq!(all[3][1].kind, EventKind::Gap as u8);
    assert_eq!(all[3][1].aux, 3); // trades 101..=103 never arrived
    assert_eq!(sigs[3], Signal::NeedResync { instrument: 1 });
    assert_eq!(all[4].len(), 1, "gap reported once, baseline advanced");
    assert_eq!(sigs[4], Signal::None);
}

/// NeedResync carries the instrument that actually lost sync (ETH here,
/// not the codec's first instrument).
#[test]
fn need_resync_carries_gapped_instrument() {
    let m1 = synth_match("match", 100, "sell", "ETH-USD", 1);
    let m2 = synth_match("match", 105, "sell", "ETH-USD", 2);
    let (_all, sigs, gaps) = run_sequence(&[m1.as_bytes(), m2.as_bytes()]);
    assert_eq!(gaps, 1);
    assert_eq!(sigs[1], Signal::NeedResync { instrument: 2 }); // ETH-USD
}

/// `last_match` seeds the per-instrument baseline (never gap-checked), so
/// the next contiguous match doesn't gap and a later jump does.
#[test]
fn last_match_seeds_baseline() {
    let lm = synth_match("last_match", 50, "buy", "ETH-USD", 1);
    let m1 = synth_match("match", 51, "sell", "ETH-USD", 2);
    let m2 = synth_match("match", 53, "sell", "ETH-USD", 3);
    let (all, _sigs, gaps) = run_sequence(&[lm.as_bytes(), m1.as_bytes(), m2.as_bytes()]);
    assert_eq!(gaps, 1);
    assert_eq!(all[0].len(), 1);
    assert_eq!(all[0][0].kind, EventKind::Trade as u8);
    assert_eq!(all[0][0].flags, flags::TAKER_SELL); // maker "buy"
    assert_eq!(all[1].len(), 1, "51 follows 50: contiguous");
    assert_eq!(all[2].len(), 2, "53 skips 52: gap");
    assert_eq!(all[2][0].kind, EventKind::Gap as u8);
    assert_eq!(all[2][0].aux, 1);
}

/// REST /products/BTC-USD/book?level=2 (3-element levels): full synthetic
/// snapshot bracket with FROM_SNAPSHOT|SYNTHETIC and venue_seq = sequence.
#[test]
fn rest_snapshot_golden() {
    let body = std::fs::read(FIXTURE_REST).expect("rest fixture readable");
    // Independent recount (REST carries bids first).
    let bids_at = find_sub(&body, b"\"bids\":");
    let asks_at = find_sub(&body, b"\"asks\":");
    assert!(bids_at < asks_at);
    let n_bids = count_sub(&body[bids_at..asks_at], b"[\"");
    let n_asks = count_sub(&body[asks_at..], b"[\"");
    assert_eq!((n_bids, n_asks), (15_221, 27_106));

    let mut c = codec();
    let mut out = Vec::new();
    let sig = c
        .parse_rest_snapshot(1, &body, 9, 10, &mut out)
        .expect("rest parses");
    assert_eq!(sig, Signal::None);
    assert_eq!(out.len(), n_bids + n_asks + 3);
    assert_eq!(out[0].kind, EventKind::Clear as u8);
    assert_eq!(out[1].kind, EventKind::SnapBegin as u8);
    assert_eq!(out[1].aux, (n_bids + n_asks) as u64);
    for e in &out[2..2 + n_bids] {
        assert_eq!(e.kind, EventKind::SnapBid as u8);
    }
    for e in &out[2 + n_bids..2 + n_bids + n_asks] {
        assert_eq!(e.kind, EventKind::SnapAsk as u8);
    }
    assert_eq!(out[out.len() - 1].kind, EventKind::SnapEnd as u8);
    for e in &out {
        assert_eq!(e.flags, flags::FROM_SNAPSHOT | flags::SYNTHETIC);
        assert_eq!(e.instrument, 1);
        assert_eq!(e.venue_seq, 132_236_333_487);
        assert_eq!(e.venue_ts_ns, 1_783_467_384_683_209_774);
        assert_eq!(e.recv_mono_ns, 9);
        assert_eq!(e.recv_wall_ns, 10);
    }
    // First bid ["63527.87","0.48883646",8], first ask ["63527.88","0.000018",1]
    // — the third element (order count) must be skipped, not mis-parsed.
    assert_eq!(out[2].price, 6_352_787_000_000);
    assert_eq!(out[2].qty, 48_883_646);
    assert_eq!(out[2 + n_bids].price, 6_352_788_000_000);
    assert_eq!(out[2 + n_bids].qty, 1_800);
    assert_eq!(c.stats().events, out.len() as u64);
}

/// REST body in auction mode: the `auction` object carries its own `time`
/// property, serialized BEFORE the top-level `time` (body order is bids,
/// asks, sequence, auction_mode, auction, time). venue_ts must come from
/// the top-level (last) `time`, not the auction's.
#[test]
fn rest_snapshot_auction_mode_uses_top_level_time() {
    let body = br#"{"bids":[["100.00","1.5",3]],"asks":[["100.10","2.0",1]],"sequence":777,"auction_mode":true,"auction":{"open_price":"100.05","open_size":"0.1","best_bid_price":"100.00","best_bid_size":"1.5","best_ask_price":"100.10","best_ask_size":"2.0","auction_state":"collection","can_open":"no","time":"2026-07-07T09:00:00Z"},"time":"2026-07-07T10:00:00.123456Z"}"#;
    let mut c = codec();
    let mut out = Vec::new();
    let sig = c
        .parse_rest_snapshot(1, body, 3, 4, &mut out)
        .expect("auction-mode body parses");
    assert_eq!(sig, Signal::None);
    // Clear + SnapBegin + 1 bid + 1 ask + SnapEnd
    assert_eq!(out.len(), 5);
    assert_eq!(out[2].kind, EventKind::SnapBid as u8);
    assert_eq!(out[2].price, 10_000_000_000);
    assert_eq!(out[3].kind, EventKind::SnapAsk as u8);
    for e in &out {
        // 2026-07-07T10:00:00.123456Z — the top-level time, NOT the
        // auction's 09:00:00 (1_783_414_800_000_000_000).
        assert_eq!(e.venue_ts_ns, 1_783_418_400_123_456_000);
        assert_eq!(e.venue_seq, 777);
        assert_eq!(e.flags, flags::FROM_SNAPSHOT | flags::SYNTHETIC);
    }
}

/// One malformed-input case: payload plus a predicate over the error kind.
type MalformedCase<'a> = (&'a [u8], fn(&CodecError) -> bool);

/// Malformed inputs: both paths must error (never panic) and leave `out`
/// exactly as it was (buffer-length restore; no partial garbage events).
#[test]
fn malformed_inputs_error_cleanly_and_restore_buffer() {
    let good_match = synth_match("match", 1, "sell", "BTC-USD", 1);
    let mut truncated = good_match.as_bytes().to_vec();
    truncated.truncate(truncated.len() / 2);
    let l2_bad_price = br#"{"type":"l2update","product_id":"BTC-USD","changes":[["buy","63507.67","0.5"],["sell","1.2.3","1.0"]],"time":"2026-07-07T22:58:17Z"}"#;
    let cases: Vec<MalformedCase> = vec![
        (b"", |_| true),
        (b"hello", |_| true),
        (b"{}", |_| true),
        (&truncated, |_| true),
        // wrong-type field: trade_id as string
        (
            br#"{"type":"match","trade_id":"abc","side":"sell","size":"1","price":"2","product_id":"BTC-USD","sequence":3,"time":"2026-07-07T22:58:17Z"}"#,
            |e| matches!(e, CodecError::Structure(_)),
        ),
        // bad number "1.2.3" -> exact fixed-point error, after one valid change
        (l2_bad_price, |e| matches!(e, CodecError::Fixed(_))),
        // unknown symbol
        (
            br#"{"type":"match","trade_id":9,"side":"sell","size":"1","price":"2","product_id":"FOO-USD","sequence":3,"time":"2026-07-07T22:58:17Z"}"#,
            |e| matches!(e, CodecError::UnknownInstrument),
        ),
        // changes is not an array
        (
            br#"{"type":"l2update","product_id":"BTC-USD","changes":{},"time":"2026-07-07T22:58:17Z"}"#,
            |e| matches!(e, CodecError::Structure(_)),
        ),
        // bad side token
        (
            br#"{"type":"l2update","product_id":"BTC-USD","changes":[["hold","1","2"]],"time":"2026-07-07T22:58:17Z"}"#,
            |e| matches!(e, CodecError::Structure(_)),
        ),
        // unparseable time
        (
            br#"{"type":"heartbeat","last_trade_id":1,"product_id":"BTC-USD","sequence":2,"time":"not-a-time"}"#,
            |e| matches!(e, CodecError::Structure(_)),
        ),
    ];
    for (payload, check) in cases {
        let mut fast = codec();
        let mut slow = codec();
        // Pre-seed the buffer to prove errors restore, not clear.
        let mut out = vec![Event::ZERO];
        let e = fast
            .parse(payload, 1, 2, &mut out)
            .expect_err("fast must error");
        assert!(check(&e), "fast error kind: {e} for {payload:?}");
        assert_eq!(out.len(), 1, "fast left partial events for {payload:?}");
        let e = slow
            .parse_slow(payload, 1, 2, &mut out)
            .expect_err("slow must error");
        assert!(check(&e), "slow error kind: {e} for {payload:?}");
        assert_eq!(out.len(), 1, "slow left partial events for {payload:?}");
        assert_eq!(out[0], Event::ZERO);
    }
}

/// Control-plane and deliberately ignored message types.
#[test]
fn control_and_ignored_signals() {
    let subs = first_line_of_type("subscriptions");
    let (sig, out) = parse_both(subs, 1, 2);
    assert_eq!(sig, Signal::Control);
    assert!(out.is_empty());
    let (sig, out) = parse_both(br#"{"type":"error","message":"nope"}"#, 1, 2);
    assert_eq!(sig, Signal::Control);
    assert!(out.is_empty());
    for ignored in [
        &br#"{"type":"status","products":[]}"#[..],
        br#"{"type":"ticker","product_id":"BTC-USD","price":"1"}"#,
    ] {
        let (sig, out) = parse_both(ignored, 1, 2);
        assert_eq!(sig, Signal::Ignored);
        assert!(out.is_empty());
    }
}

/// An empty `changes` list is valid and emits nothing.
#[test]
fn empty_changes_l2update() {
    let payload =
        br#"{"type":"l2update","product_id":"ETH-USD","changes":[],"time":"2026-07-07T22:58:17Z"}"#;
    let (sig, out) = parse_both(payload, 1, 2);
    assert_eq!(sig, Signal::None);
    assert!(out.is_empty());
}

/// CodecStats bookkeeping on the fast path.
#[test]
fn stats_counters() {
    let mut c = codec();
    let mut out = Vec::new();
    let hb = synth_heartbeat(5, "BTC-USD", 1);
    let m1 = synth_match("match", 10, "sell", "BTC-USD", 2);
    let m2 = synth_match("match", 15, "sell", "BTC-USD", 3);
    let subs = first_line_of_type("subscriptions");
    for p in [hb.as_bytes(), m1.as_bytes(), m2.as_bytes(), subs] {
        c.parse(p, 1, 2, &mut out).expect("parses");
    }
    assert_eq!(c.stats().fast_msgs, 4);
    assert_eq!(c.stats().events, 4); // hb + trade + (gap + trade) + nothing
    assert_eq!(c.stats().gaps, 1);
    assert_eq!(out.len(), 4);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// PROPTEST: random byte mutations and truncations of real fixture lines
    /// (and the REST body) never panic either parse path — they may error.
    #[test]
    fn fuzz_mutations_never_panic(
        line_sel in 0usize..100_000,
        cut_pct in 0usize..=100,
        muts in proptest::collection::vec((0usize..1_000_000, any::<u8>()), 0..8),
    ) {
        let lines = fixture_lines();
        let mut payload = lines[line_sel % lines.len()].clone();
        for &(pos, byte) in &muts {
            if !payload.is_empty() {
                let p = pos % payload.len();
                payload[p] = byte;
            }
        }
        payload.truncate(payload.len() * cut_pct / 100);
        let mut fast = codec();
        let mut slow = codec();
        let mut out = Vec::new();
        let _ = fast.parse(&payload, 1, 2, &mut out);
        out.clear();
        let _ = slow.parse_slow(&payload, 1, 2, &mut out);
        out.clear();
        let _ = fast.parse_rest_snapshot(1, &payload, 1, 2, &mut out);
    }
}
