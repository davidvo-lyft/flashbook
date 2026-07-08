//! The replay driver: merged raw records -> venue codecs -> normalized
//! events -> per-instrument books, with the Kraken CRC32 oracle verified
//! inline and determinism digests over both the event stream and the final
//! book states.
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
                    Ok((instrument, body)) => match codec.parse_rest_snapshot(
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
                    },
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
