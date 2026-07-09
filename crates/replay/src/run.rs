//! The replay driver: merged raw records -> venue codecs -> normalized
//! events -> per-instrument books, with the Kraken CRC32 oracle verified
//! inline, Coinbase/Binance books cross-validated against captured REST
//! snapshot bodies (statistical, timing-skewed — see the `crossval_*`
//! fields), and determinism digests over both the event stream and the
//! final book states.
//!
//! Determinism contract: replaying the same segments twice yields identical
//! `event_stream_digest` and `books_digest` (asserted by tests and by the
//! `replay` binary's `--verify-determinism` mode). To reproduce capture
//! behavior exactly, codec state resets where capture's did: at every
//! `{"event":"connect"...}` NOTE record (capture creates a fresh codec per
//! WS session).

use std::path::Path;

use flashbook_feed::binance::BinanceCodec;
use flashbook_feed::coinbase::CoinbaseCodec;
use flashbook_feed::conn::parse_rest_envelope;
use flashbook_feed::kraken::{KrakenCodec, pair_decimals};
use flashbook_feed::{CodecError, SymbolTable, VenueCodec};
use flashbook_lob::book::Apply;
use flashbook_lob::{BookSet, L2Book};
use flashbook_proto::event::EVENT_SIZE;
use flashbook_proto::rawlog::rkind;
use flashbook_proto::{Event, Registry, Venue};

use crate::source::{MergedStream, SourceError};

/// Counters and digests from one replay pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayOutcome {
    /// Raw records consumed.
    pub records: u64,
    /// Sum of raw record payload bytes (the "raw JSON" baseline for
    /// compression ratios).
    pub raw_payload_bytes: u64,
    /// WS text records parsed.
    pub ws_frames: u64,
    /// REST snapshot records applied.
    pub rest_snapshots: u64,
    /// NOTE records seen (codec resets happen on connect notes).
    pub notes: u64,
    /// Codec state resets driven by connect notes.
    pub codec_resets: u64,
    /// Normalized events emitted.
    pub events: u64,
    /// Frames that needed the slow-path fallback.
    pub fallbacks: u64,
    /// Frames neither path parsed.
    pub parse_errors: u64,
    /// Gap events observed in the stream.
    pub gaps: u64,
    /// Torn segment tails skipped by the source layer.
    pub torn_tails: u64,
    /// Kraken checksums verified OK against the local book.
    pub checksums_ok: u64,
    /// Kraken checksum mismatches (correctness failures!).
    pub checksum_mismatches: u64,
    /// Checksum events skipped because the book wasn't synced/complete.
    pub checksums_skipped: u64,
    /// Coinbase/Binance REST snapshot records eligible for cross-validation
    /// (Kraken has no REST path; its oracle is the per-message CRC32).
    pub crossval_snapshots: u64,
    /// Cross-validated snapshots actually scored: the live reconstructed
    /// book was synced when the REST body arrived and the body parsed.
    /// On-connect snapshots are *expected* to be skipped (they are what
    /// syncs the book in the first place); only periodic refreshes land on
    /// an already-synced book.
    pub crossval_scored: u64,
    /// Median exact top-10 overlap, integer percent 0-100. Per snapshot:
    /// |intersection of (price, qty) pairs between live book top-10 and
    /// REST body top-10| / 10, averaged over both sides. This is a
    /// STATISTICAL cross-check, not an oracle: the REST body is fetched
    /// over HTTP while the WS stream keeps mutating the book, so timing
    /// skew makes high-but-not-100% overlap the expected healthy reading.
    pub crossval_top10_overlap_p50: u64,
    /// 90th percentile (ascending nearest-rank) of the exact overlap.
    pub crossval_top10_overlap_p90: u64,
    /// Minimum exact overlap over all scored snapshots (0 if none scored).
    pub crossval_worst_overlap: u64,
    /// Median price-only top-10 overlap (ignores qty), integer percent.
    /// Less sensitive to in-flight qty churn than the exact metric.
    pub crossval_price_overlap_p50: u64,
    /// 90th percentile of the price-only overlap.
    pub crossval_price_overlap_p90: u64,
    /// FNV-1a over every emitted event's 64 bytes, in order.
    pub event_stream_digest: u64,
    /// Combined digest over all final book states.
    pub books_digest: u64,
    /// First/last monotonic record timestamps (replay span).
    pub first_mono_ns: u64,
    /// Last monotonic record timestamp.
    pub last_mono_ns: u64,
}

/// Replay errors.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// Segment reading failed.
    #[error("source: {0}")]
    Source(#[from] SourceError),
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[inline]
fn fnv1a(mut h: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Nearest-rank percentile (floor index) over an ascending-sorted slice;
/// 0 when empty. Pure integer arithmetic — deterministic by construction.
fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() - 1) * pct / 100]
}

/// Fixed-point (price, qty) levels of one book side, best-first.
type SideLevels = Vec<(i64, i64)>;

/// Parse the top-10 (bids, asks) of a Coinbase or Binance REST book body
/// into fixed-point (price, qty) pairs, best-first (both venues serialize
/// levels best-first). Coinbase `/book?level=2` levels are
/// `["price","size",num_orders]`, Binance `/depth` levels are
/// `["price","qty"]`; both are consumed by reading the first two string
/// elements, exactly the fields the codecs' `parse_rest_snapshot` uses.
/// Cold path (runs a few times per capture-hour), so serde_json is fine.
/// `None` = body not interpretable; callers skip scoring, never fail.
fn parse_rest_top10(body: &[u8]) -> Option<(SideLevels, SideLevels)> {
    fn side(v: &serde_json::Value, key: &str) -> Option<SideLevels> {
        v.get(key)?
            .as_array()?
            .iter()
            .take(10)
            .map(|lvl| {
                let lvl = lvl.as_array()?;
                let p = flashbook_proto::parse_fixed(lvl.first()?.as_str()?.as_bytes()).ok()?;
                let q = flashbook_proto::parse_fixed(lvl.get(1)?.as_str()?.as_bytes()).ok()?;
                Some((p, q))
            })
            .collect()
    }
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    Some((side(&v, "bids")?, side(&v, "asks")?))
}

/// How many of the REST side's top-10 levels appear in the live top-10:
/// as exact (price, qty) pairs, and as prices regardless of qty. Returned
/// as `(exact, price_only)` raw counts (each 0..=10).
fn side_overlap(live: &[(i64, i64)], rest: &[(i64, i64)]) -> (u64, u64) {
    let mut exact = 0u64;
    let mut price = 0u64;
    for r in rest {
        exact += u64::from(live.contains(r));
        price += u64::from(live.iter().any(|l| l.0 == r.0));
    }
    (exact, price)
}

/// Score one Coinbase/Binance REST snapshot body against the live
/// reconstructed book, **before** the snapshot is applied. Skipped (not
/// scored) when the book is absent/unsynced or the body doesn't parse.
/// Deterministic: a pure function of stream content and book state.
fn score_crossval<B: L2Book>(
    books: &BookSet<B>,
    instrument: u32,
    body: &[u8],
    out: &mut ReplayOutcome,
    exact_samples: &mut Vec<u64>,
    price_samples: &mut Vec<u64>,
) {
    let Some(book) = books.get(instrument) else {
        return;
    };
    if !book.is_synced() {
        return;
    }
    let Some((rest_bids, rest_asks)) = parse_rest_top10(body) else {
        return;
    };
    let (mut live_bids, mut live_asks) = (Vec::with_capacity(10), Vec::with_capacity(10));
    book.top_n_into(flashbook_proto::event::Side::Bid, 10, &mut live_bids);
    book.top_n_into(flashbook_proto::event::Side::Ask, 10, &mut live_asks);
    let (be, bp) = side_overlap(&live_bids, &rest_bids);
    let (ae, ap) = side_overlap(&live_asks, &rest_asks);
    // Both sides fixed at /10 each -> combined percent = (count/20)*100.
    out.crossval_scored += 1;
    exact_samples.push((be + ae) * 5);
    price_samples.push((bp + ap) * 5);
}

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

/// Replay every segment under `root` through fresh codecs into books of
/// type `B`. `on_event` sees every emitted event in deterministic order
/// (feed it to the tick store, the bus, or nothing).
pub fn replay_books<B: L2Book>(
    root: &Path,
    registry: &Registry,
    make_book: fn(Option<usize>) -> B,
    kraken_depth: Option<usize>,
    mut on_event: impl FnMut(&Event),
) -> Result<ReplayOutcome, ReplayError> {
    let mut out = ReplayOutcome::default();
    let mut stream = MergedStream::new(root)?;

    let mut codecs: [Option<Box<dyn VenueCodec>>; 3] = [None, None, None];
    // Kraken books carry the venue depth cap; other venues unlimited.
    let mut books: BookSet<B> = BookSet::new(None, make_book);
    let mut kraken_books: BookSet<B> = BookSet::new(kraken_depth, make_book);
    let mut buf: Vec<Event> = Vec::with_capacity(4096);
    let mut digest = FNV_OFFSET;
    // REST cross-validation samples (integer percent per scored snapshot).
    let mut xv_exact: Vec<u64> = Vec::new();
    let mut xv_price: Vec<u64> = Vec::new();

    while let Some(rec) = stream.next()? {
        out.records += 1;
        out.raw_payload_bytes += rec.payload.len() as u64;
        if out.first_mono_ns == 0 {
            out.first_mono_ns = rec.recv_mono_ns;
        }
        out.last_mono_ns = rec.recv_mono_ns;

        let Ok(venue) = Venue::try_from(rec.venue) else {
            continue;
        };
        let idx = rec.venue as usize - 1;
        if codecs[idx].is_none() {
            codecs[idx] = Some(make_codec(venue, registry));
        }

        buf.clear();
        let signal = match rec.rkind {
            rkind::WS_TEXT | rkind::WS_BINARY => {
                out.ws_frames += 1;
                let codec = codecs[idx].as_mut().expect("codec present");
                match codec.parse(&rec.payload, rec.recv_mono_ns, rec.recv_wall_ns, &mut buf) {
                    Ok(s) => Some(s),
                    Err(CodecError::Structure(_)) => {
                        buf.clear();
                        out.fallbacks += 1;
                        match codec.parse_slow(
                            &rec.payload,
                            rec.recv_mono_ns,
                            rec.recv_wall_ns,
                            &mut buf,
                        ) {
                            Ok(s) => Some(s),
                            Err(_) => {
                                buf.clear();
                                out.parse_errors += 1;
                                None
                            }
                        }
                    }
                    Err(_) => {
                        buf.clear();
                        out.parse_errors += 1;
                        None
                    }
                }
            }
            rkind::REST_SNAPSHOT => {
                out.rest_snapshots += 1;
                let codec = codecs[idx].as_mut().expect("codec present");
                match parse_rest_envelope(&rec.payload) {
                    Ok((instrument, body)) => {
                        // Cross-validate the live book against the REST body
                        // BEFORE applying it (only Coinbase/Binance take the
                        // REST path; Kraken's oracle is the in-band CRC32).
                        if matches!(venue, Venue::Coinbase | Venue::Binance) {
                            out.crossval_snapshots += 1;
                            score_crossval(
                                &books,
                                instrument,
                                body,
                                &mut out,
                                &mut xv_exact,
                                &mut xv_price,
                            );
                        }
                        match codec.parse_rest_snapshot(
                            instrument,
                            body,
                            rec.recv_mono_ns,
                            rec.recv_wall_ns,
                            &mut buf,
                        ) {
                            Ok(s) => Some(s),
                            Err(_) => {
                                buf.clear();
                                out.parse_errors += 1;
                                None
                            }
                        }
                    }
                    Err(_) => {
                        out.parse_errors += 1;
                        None
                    }
                }
            }
            rkind::NOTE => {
                out.notes += 1;
                // capture built a fresh codec per WS session; mirror that
                if rec.payload.starts_with(br#"{"event":"connect""#) {
                    codecs[idx] = Some(make_codec(venue, registry));
                    out.codec_resets += 1;
                }
                None
            }
            _ => None,
        };
        let _ = signal; // Signal drives live resync; replay just replays what happened.

        for ev in &buf {
            out.events += 1;
            digest = fnv1a(digest, Event::slice_as_bytes(std::slice::from_ref(ev)));
            debug_assert_eq!(
                Event::slice_as_bytes(std::slice::from_ref(ev)).len(),
                EVENT_SIZE
            );
            if ev.kind == flashbook_proto::EventKind::Gap as u8 {
                out.gaps += 1;
            }
            on_event(ev);

            let apply = if venue == Venue::Kraken {
                kraken_books.apply(ev)
            } else {
                books.apply(ev)
            };
            if venue == Venue::Kraken {
                let crc = match apply {
                    Apply::ChecksumToVerify { crc } => Some(crc),
                    Apply::SnapshotComplete { checksum } if checksum != 0 => Some(checksum),
                    _ => None,
                };
                if let Some(want) = crc {
                    verify_kraken_crc(&kraken_books, registry, ev.instrument, want, &mut out);
                }
            }
        }
    }

    xv_exact.sort_unstable();
    xv_price.sort_unstable();
    out.crossval_top10_overlap_p50 = percentile(&xv_exact, 50);
    out.crossval_top10_overlap_p90 = percentile(&xv_exact, 90);
    out.crossval_worst_overlap = xv_exact.first().copied().unwrap_or(0);
    out.crossval_price_overlap_p50 = percentile(&xv_price, 50);
    out.crossval_price_overlap_p90 = percentile(&xv_price, 90);
    out.torn_tails = stream.stats().torn_tails;
    out.event_stream_digest = digest;
    out.books_digest = books.combined_digest() ^ kraken_books.combined_digest().rotate_left(1);
    Ok(out)
}

fn verify_kraken_crc<B: L2Book>(
    books: &BookSet<B>,
    registry: &Registry,
    instrument: u32,
    want: u32,
    out: &mut ReplayOutcome,
) {
    let Some(book) = books.get(instrument) else {
        out.checksums_skipped += 1;
        return;
    };
    if !book.is_synced() {
        out.checksums_skipped += 1;
        return;
    }
    let Some((price_dec, qty_dec)) = registry
        .get(instrument)
        .and_then(|m| pair_decimals(&m.venue_symbol))
    else {
        out.checksums_skipped += 1;
        return;
    };
    let (mut asks, mut bids) = (Vec::with_capacity(10), Vec::with_capacity(10));
    book.top_n_into(flashbook_proto::event::Side::Ask, 10, &mut asks);
    book.top_n_into(flashbook_proto::event::Side::Bid, 10, &mut bids);
    let got = flashbook_proto::kraken_crc::kraken_book_crc32(&asks, &bids, price_dec, qty_dec);
    if got == want {
        out.checksums_ok += 1;
    } else {
        out.checksum_mismatches += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flashbook_lob::LadderBook;
    use flashbook_proto::rawlog::{RawLogWriter, rkind};
    use std::path::PathBuf;

    /// Build a synthetic capture from real fixture lines: kraken frames get
    /// real venue payloads (so the CRC oracle runs), with fabricated but
    /// strictly ordered receive timestamps.
    fn synth_capture(dir: &Path, lines: &[&str], venue: &str, venue_id: u8) -> PathBuf {
        let vdir = dir.join(venue);
        std::fs::create_dir_all(&vdir).unwrap();
        let path = vdir.join(format!("{venue}-1000.fbraw"));
        let mut w = RawLogWriter::create(&path, venue_id, 1000, 1000, b"{}").unwrap();
        w.append(rkind::NOTE, 1, 1, br#"{"event":"connect","attempt":0}"#)
            .unwrap();
        for (i, l) in lines.iter().enumerate() {
            let t = 10 + i as u64 * 10;
            w.append(rkind::WS_TEXT, t, t + 1, l.as_bytes()).unwrap();
        }
        w.finish().unwrap();
        path
    }

    fn kraken_fixture_lines(n: usize) -> Vec<String> {
        let raw = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../feed/fixtures/kraken/live-btc-eth.ndjson"
        ))
        .expect("kraken fixture present");
        raw.lines().take(n).map(ToString::to_string).collect()
    }

    #[test]
    fn replay_is_deterministic_and_verifies_kraken_crcs() {
        let dir = tempfile::tempdir().unwrap();
        let lines = kraken_fixture_lines(800);
        let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
        synth_capture(dir.path(), &refs, "kraken", 3);

        let registry = Registry::builtin();
        let run = || {
            replay_books::<LadderBook>(
                dir.path(),
                &registry,
                |d| d.map_or_else(LadderBook::new, LadderBook::with_max_depth),
                Some(10), // fixture was captured at depth 10
                |_| {},
            )
            .unwrap()
        };
        let a = run();
        let b = run();
        assert_eq!(a, b, "replay must be fully deterministic");
        assert!(a.events > 500, "events flowed: {a:?}");
        assert_eq!(a.parse_errors, 0, "{a:?}");
        assert!(a.checksums_ok > 100, "oracle ran: {a:?}");
        assert_eq!(a.checksum_mismatches, 0, "oracle must be clean: {a:?}");
        assert_eq!(a.codec_resets, 1);
    }

    #[test]
    fn codec_reset_on_connect_note_changes_outcome_only_via_state() {
        // Two identical kraken snapshots separated by a connect note replay
        // cleanly (second session re-snapshots the book).
        let dir = tempfile::tempdir().unwrap();
        let lines = kraken_fixture_lines(40);
        let vdir = dir.path().join("kraken");
        std::fs::create_dir_all(&vdir).unwrap();
        let path = vdir.join("kraken-1000.fbraw");
        let mut w = RawLogWriter::create(&path, 3, 1000, 1000, b"{}").unwrap();
        w.append(rkind::NOTE, 1, 1, br#"{"event":"connect","attempt":0}"#)
            .unwrap();
        for (i, l) in lines.iter().enumerate() {
            w.append(rkind::WS_TEXT, 10 + i as u64, 11 + i as u64, l.as_bytes())
                .unwrap();
        }
        w.append(
            rkind::NOTE,
            5000,
            5001,
            br#"{"event":"connect","attempt":1}"#,
        )
        .unwrap();
        for (i, l) in lines.iter().enumerate() {
            w.append(
                rkind::WS_TEXT,
                6000 + i as u64,
                6001 + i as u64,
                l.as_bytes(),
            )
            .unwrap();
        }
        w.finish().unwrap();

        let registry = Registry::builtin();
        let out = replay_books::<LadderBook>(
            dir.path(),
            &registry,
            |d| d.map_or_else(LadderBook::new, LadderBook::with_max_depth),
            Some(10),
            |_| {},
        )
        .unwrap();
        assert_eq!(out.codec_resets, 2);
        assert_eq!(out.parse_errors, 0);
        assert_eq!(out.checksum_mismatches, 0);
    }
}
