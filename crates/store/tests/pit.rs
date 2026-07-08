//! Integration tests for the point-in-time snapshot index: build state
//! machine (complete vs incomplete/invalidated snapshots), `latest_at`
//! binary search, sidecar save/load/corruption, block-boundary entries,
//! and PIT scan vs naive full-scan filter (proptest).

use std::collections::HashMap;
use std::path::Path;

use flashbook_proto::event::{Event, EventKind};
use flashbook_store::pit::{PIT_HEADER_LEN, PitError, SnapEntry, SnapshotIndex, pit_scan};
use flashbook_store::segment::{StoreReader, StoreWriter};
use proptest::prelude::*;

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// One synthetic event; mono is the caller's clock, other fields filler.
fn ev(kind: EventKind, instrument: u32, mono: u64) -> Event {
    Event {
        recv_mono_ns: mono,
        recv_wall_ns: mono + 1_700_000_000_000_000_000,
        venue_ts_ns: 0,
        venue_seq: mono,
        price: 6_358_964_000_000 + i64::from(instrument),
        qty: 200_000_000,
        aux: 0,
        instrument,
        kind: kind as u8,
        venue: 1,
        flags: 0,
        rsvd: 0,
    }
}

fn write_store(path: &Path, block_events: usize, evs: &[Event], seal: bool) -> StoreReader {
    let mut w = StoreWriter::create(path, b"pit-test", block_events, None).unwrap();
    for e in evs {
        w.append(e).unwrap();
    }
    if seal {
        w.seal().unwrap();
    } else {
        w.checkpoint().unwrap();
        drop(w);
    }
    StoreReader::open(path).unwrap()
}

/// Push a complete snapshot (Begin, `levels` x Bid/Ask, End), one mono
/// tick per event. Returns the SnapBegin mono.
fn push_snap(evs: &mut Vec<Event>, inst: u32, t: &mut u64, levels: usize) -> u64 {
    let begin = *t;
    evs.push(ev(EventKind::SnapBegin, inst, *t));
    *t += 1;
    for i in 0..levels {
        let kind = if i % 2 == 0 {
            EventKind::SnapBid
        } else {
            EventKind::SnapAsk
        };
        evs.push(ev(kind, inst, *t));
        *t += 1;
    }
    evs.push(ev(EventKind::SnapEnd, inst, *t));
    *t += 1;
    begin
}

/// Reference implementation of the index over the flat event list.
/// `block_events` mirrors the writer's fixed block cut so flat positions
/// map to (block_idx, event_idx).
fn naive_entries(evs: &[Event], block_events: usize) -> Vec<SnapEntry> {
    let mut pending: HashMap<u32, (u64, usize)> = HashMap::new();
    let mut out = Vec::new();
    for (i, e) in evs.iter().enumerate() {
        match e.kind().unwrap() {
            EventKind::SnapBegin => {
                pending.insert(e.instrument, (e.recv_mono_ns, i));
            }
            EventKind::Clear => {
                pending.remove(&e.instrument);
            }
            EventKind::SnapEnd => {
                if let Some((mono, pos)) = pending.remove(&e.instrument) {
                    out.push(SnapEntry {
                        instrument: e.instrument,
                        mono,
                        block_idx: u32::try_from(pos / block_events).unwrap(),
                        event_idx: u32::try_from(pos % block_events).unwrap(),
                    });
                }
            }
            _ => {}
        }
    }
    out.sort_by_key(|e| (e.instrument, e.mono));
    out
}

/// Reference PIT window: flat tail from the entry's SnapBegin, filtered to
/// the instrument, cut at `t`.
fn naive_pit(evs: &[Event], entry: &SnapEntry, block_events: usize, t: u64) -> Vec<Event> {
    let flat = entry.block_idx as usize * block_events + entry.event_idx as usize;
    evs[flat..]
        .iter()
        .filter(|e| e.instrument == entry.instrument && e.recv_mono_ns <= t)
        .copied()
        .collect()
}

fn scan_events(r: &StoreReader, entry: &SnapEntry, t: u64) -> Vec<Event> {
    let mut got = Vec::new();
    let n = pit_scan(r, entry, t, |e| got.push(*e)).unwrap();
    assert_eq!(n as usize, got.len());
    got
}

#[test]
fn no_snapshots_empty_index() {
    let d = tmp();
    let mut evs = Vec::new();
    for i in 0..50u64 {
        evs.push(ev(EventKind::BidSet, 1, 100 + i));
    }
    let r = write_store(&d.path().join("s.fbstore"), 16, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    assert!(idx.is_empty());
    assert_eq!(idx.len(), 0);
    assert_eq!(idx.latest_at(1, u64::MAX), None);
}

#[test]
fn complete_snapshot_indexed_with_exact_position() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 1000u64;
    evs.push(ev(EventKind::BidSet, 1, t)); // one delta before the snapshot
    t += 1;
    let begin = push_snap(&mut evs, 1, &mut t, 4);
    evs.push(ev(EventKind::Trade, 1, t));
    let r = write_store(&d.path().join("s.fbstore"), 64, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    assert_eq!(
        idx.entries(),
        &[SnapEntry {
            instrument: 1,
            mono: begin,
            block_idx: 0,
            event_idx: 1,
        }]
    );
}

#[test]
fn interleaved_instruments_both_indexed() {
    let d = tmp();
    // Two snapshots interleaved event-by-event: legal, both complete.
    let evs = vec![
        ev(EventKind::SnapBegin, 1, 10),
        ev(EventKind::SnapBegin, 2, 11),
        ev(EventKind::SnapBid, 1, 12),
        ev(EventKind::SnapBid, 2, 13),
        ev(EventKind::SnapAsk, 2, 14),
        ev(EventKind::SnapAsk, 1, 15),
        ev(EventKind::SnapEnd, 2, 16),
        ev(EventKind::SnapEnd, 1, 17),
    ];
    let r = write_store(&d.path().join("s.fbstore"), 3, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    // sorted by (instrument, mono); positions reflect block_events = 3
    assert_eq!(
        idx.entries(),
        &[
            SnapEntry {
                instrument: 1,
                mono: 10,
                block_idx: 0,
                event_idx: 0,
            },
            SnapEntry {
                instrument: 2,
                mono: 11,
                block_idx: 0,
                event_idx: 1,
            },
        ]
    );
}

#[test]
fn missing_snap_end_not_indexed() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 10u64;
    let begin2 = push_snap(&mut evs, 2, &mut t, 2); // instrument 2 completes
    // instrument 1: Begin + levels, file ends before SnapEnd
    evs.push(ev(EventKind::SnapBegin, 1, t));
    t += 1;
    evs.push(ev(EventKind::SnapBid, 1, t));
    let r = write_store(&d.path().join("s.fbstore"), 4, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    assert_eq!(idx.len(), 1);
    assert_eq!(idx.entries()[0].instrument, 2);
    assert_eq!(idx.entries()[0].mono, begin2);
    assert_eq!(idx.latest_at(1, u64::MAX), None);
}

#[test]
fn clear_mid_fill_invalidates_snapshot() {
    let d = tmp();
    let mut evs = vec![
        ev(EventKind::SnapBegin, 1, 10),
        ev(EventKind::SnapBid, 1, 11),
        ev(EventKind::Clear, 1, 12), // resync: bracket is dead
        ev(EventKind::SnapAsk, 1, 13),
        ev(EventKind::SnapEnd, 1, 14), // dangling end: ignored
    ];
    let mut t = 20u64;
    let begin = push_snap(&mut evs, 1, &mut t, 2); // later snapshot is fine
    let r = write_store(&d.path().join("s.fbstore"), 8, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    assert_eq!(idx.len(), 1);
    assert_eq!(idx.entries()[0].mono, begin);
    // t = 14 is after the aborted bracket's SnapEnd but before the good one
    assert_eq!(idx.latest_at(1, 14), None);
}

#[test]
fn clear_for_other_instrument_does_not_invalidate() {
    let d = tmp();
    let evs = vec![
        ev(EventKind::SnapBegin, 1, 10),
        ev(EventKind::SnapBid, 1, 11),
        ev(EventKind::Clear, 2, 12), // instrument 2's problem
        ev(EventKind::SnapAsk, 1, 13),
        ev(EventKind::SnapEnd, 1, 14),
    ];
    let r = write_store(&d.path().join("s.fbstore"), 8, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    assert_eq!(
        idx.entries(),
        &[SnapEntry {
            instrument: 1,
            mono: 10,
            block_idx: 0,
            event_idx: 0,
        }]
    );
}

#[test]
fn restarted_snap_begin_keeps_only_the_new_bracket() {
    let d = tmp();
    let evs = vec![
        ev(EventKind::SnapBegin, 1, 10),
        ev(EventKind::SnapBid, 1, 11),
        ev(EventKind::SnapBegin, 1, 12), // restart discards the first
        ev(EventKind::SnapBid, 1, 13),
        ev(EventKind::SnapEnd, 1, 14),
    ];
    let r = write_store(&d.path().join("s.fbstore"), 8, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    assert_eq!(
        idx.entries(),
        &[SnapEntry {
            instrument: 1,
            mono: 12,
            block_idx: 0,
            event_idx: 2,
        }]
    );
    // t = 11: the only SnapBegin <= t belongs to the discarded bracket
    assert_eq!(idx.latest_at(1, 11), None);
}

#[test]
fn latest_at_picks_latest_and_none_before_any() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 100u64;
    let b1 = push_snap(&mut evs, 7, &mut t, 2);
    t = 200;
    let b2 = push_snap(&mut evs, 7, &mut t, 2);
    t = 300;
    let b3 = push_snap(&mut evs, 7, &mut t, 2);
    let r = write_store(&d.path().join("s.fbstore"), 4, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    assert_eq!(idx.len(), 3);
    assert_eq!(idx.latest_at(7, 99), None); // before any snapshot
    assert_eq!(idx.latest_at(7, b1).unwrap().mono, b1); // exact hit
    assert_eq!(idx.latest_at(7, 150).unwrap().mono, b1);
    assert_eq!(idx.latest_at(7, 250).unwrap().mono, b2);
    assert_eq!(idx.latest_at(7, u64::MAX).unwrap().mono, b3);
    assert_eq!(idx.latest_at(8, u64::MAX), None); // unknown instrument
}

#[test]
fn sidecar_roundtrip_and_rebuild_match() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 10u64;
    for inst in 1..=3u32 {
        push_snap(&mut evs, inst, &mut t, 5);
        push_snap(&mut evs, inst, &mut t, 3);
    }
    let p = d.path().join("s.fbstore");
    let r = write_store(&p, 7, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    assert_eq!(idx.len(), 6);

    let sp = d.path().join("s.fbsnpix");
    idx.save(&sp).unwrap();
    let loaded = SnapshotIndex::load(&sp).unwrap();
    assert_eq!(loaded, idx);

    // rebuild from a fresh reader equals the original index
    let r2 = StoreReader::open(&p).unwrap();
    assert_eq!(SnapshotIndex::build(&r2).unwrap(), idx);

    // empty index roundtrips too
    let se = d.path().join("empty.fbsnpix");
    SnapshotIndex::default().save(&se).unwrap();
    assert!(SnapshotIndex::load(&se).unwrap().is_empty());
}

#[test]
fn sidecar_corruption_and_torn_are_distinct_errors() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 10u64;
    push_snap(&mut evs, 1, &mut t, 3);
    push_snap(&mut evs, 2, &mut t, 3);
    let r = write_store(&d.path().join("s.fbstore"), 4, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    let sp = d.path().join("s.fbsnpix");
    idx.save(&sp).unwrap();
    let good = std::fs::read(&sp).unwrap();

    let check = |bytes: &[u8]| {
        let p = d.path().join("mut.fbsnpix");
        std::fs::write(&p, bytes).unwrap();
        SnapshotIndex::load(&p).unwrap_err()
    };

    // flipped byte inside the packed entries -> CRC mismatch (corrupt)
    let mut bad = good.clone();
    bad[PIT_HEADER_LEN + 5] ^= 0xff;
    assert!(matches!(check(&bad), PitError::CorruptSidecar(_)));

    // bad magic -> corrupt
    let mut bad = good.clone();
    bad[0] = b'X';
    assert!(matches!(check(&bad), PitError::CorruptSidecar(_)));

    // unsupported version -> corrupt
    let mut bad = good.clone();
    bad[8] = 9;
    assert!(matches!(check(&bad), PitError::CorruptSidecar(_)));

    // trailing bytes -> corrupt
    let mut bad = good.clone();
    bad.push(0);
    assert!(matches!(check(&bad), PitError::CorruptSidecar(_)));

    // truncations at every prefix length -> torn (never corrupt, never Ok)
    for cut in 0..good.len() {
        assert!(
            matches!(check(&good[..cut]), PitError::TornSidecar(_)),
            "cut at {cut} should be torn"
        );
    }
}

#[test]
fn snapshot_at_block_boundary_spanning_blocks() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 10u64;
    // 4 filler events = exactly one block (block_events = 4), so the
    // snapshot's SnapBegin is event 0 of block 1 and its 10 levels span
    // blocks 1..=3.
    for _ in 0..4 {
        evs.push(ev(EventKind::BidSet, 2, t));
        t += 1;
    }
    let begin = push_snap(&mut evs, 1, &mut t, 10); // 12 events total
    evs.push(ev(EventKind::Trade, 1, t));
    let r = write_store(&d.path().join("s.fbstore"), 4, &evs, true);
    assert!(r.n_blocks() >= 4);
    let idx = SnapshotIndex::build(&r).unwrap();
    assert_eq!(
        idx.entries(),
        &[SnapEntry {
            instrument: 1,
            mono: begin,
            block_idx: 1,
            event_idx: 0,
        }]
    );
    // scan through the trade: full snapshot (12 events) + 1 trade
    let entry = idx.latest_at(1, u64::MAX).unwrap();
    let got = scan_events(&r, entry, u64::MAX);
    assert_eq!(got.len(), 13);
    assert_eq!(got, naive_pit(&evs, entry, 4, u64::MAX));
    assert_eq!(got[0].kind().unwrap(), EventKind::SnapBegin);
    assert_eq!(got[11].kind().unwrap(), EventKind::SnapEnd);
    assert_eq!(got[12].kind().unwrap(), EventKind::Trade);
}

#[test]
fn pit_scan_cuts_at_t_and_skips_other_instruments() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 100u64;
    let begin = push_snap(&mut evs, 1, &mut t, 2); // ends at t=103, t now 104
    // interleaved post-snapshot flow on two instruments
    for i in 0..10u64 {
        let inst = if i % 2 == 0 { 1 } else { 2 };
        let kind = if i % 3 == 0 {
            EventKind::Trade
        } else {
            EventKind::BidSet
        };
        evs.push(ev(kind, inst, t));
        t += 1;
    }
    let r = write_store(&d.path().join("s.fbstore"), 4, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    let entry = *idx.latest_at(1, 108).unwrap();
    assert_eq!(entry.mono, begin);
    let got = scan_events(&r, &entry, 108);
    assert_eq!(got, naive_pit(&evs, &entry, 4, 108));
    assert!(got.iter().all(|e| e.instrument == 1));
    assert!(got.iter().all(|e| e.recv_mono_ns <= 108));
    // trades for the instrument are included, not just book events
    assert!(got.iter().any(|e| e.kind().unwrap() == EventKind::Trade));
    // t before the SnapBegin: not an error, zero events
    assert_eq!(pit_scan(&r, &entry, begin - 1, |_| {}).unwrap(), 0);
}

#[test]
fn pit_scan_rejects_stale_or_foreign_entries() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 10u64;
    push_snap(&mut evs, 1, &mut t, 2);
    let r = write_store(&d.path().join("s.fbstore"), 8, &evs, true);
    let idx = SnapshotIndex::build(&r).unwrap();
    let good = *idx.latest_at(1, u64::MAX).unwrap();

    let cases = [
        SnapEntry {
            block_idx: 99,
            ..good
        }, // block out of range
        SnapEntry {
            event_idx: 99,
            ..good
        }, // event out of range
        SnapEntry {
            event_idx: good.event_idx + 1,
            ..good
        }, // points at a SnapBid
        SnapEntry {
            instrument: 42,
            ..good
        }, // wrong instrument
        SnapEntry {
            mono: good.mono + 1,
            ..good
        }, // wrong mono
    ];
    for bad in cases {
        assert!(
            matches!(
                pit_scan(&r, &bad, u64::MAX, |_| {}),
                Err(PitError::Mismatch(_))
            ),
            "{bad:?} should be rejected"
        );
    }
}

#[test]
fn unsealed_store_builds_identical_index() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 10u64;
    push_snap(&mut evs, 1, &mut t, 6);
    push_snap(&mut evs, 2, &mut t, 6);
    let rs = write_store(&d.path().join("sealed.fbstore"), 5, &evs, true);
    let ru = write_store(&d.path().join("unsealed.fbstore"), 5, &evs, false);
    assert!(rs.sealed());
    assert!(!ru.sealed());
    let a = SnapshotIndex::build(&rs).unwrap();
    let b = SnapshotIndex::build(&ru).unwrap();
    assert_eq!(a, b);
    assert_eq!(a.len(), 2);
}

/// Arbitrary event soup: any mix of kinds/instruments with non-decreasing
/// mono — complete, incomplete, restarted and Clear-aborted snapshots all
/// arise naturally.
fn arb_events() -> impl Strategy<Value = Vec<Event>> {
    let kinds = [
        EventKind::BidSet,
        EventKind::AskSet,
        EventKind::Trade,
        EventKind::SnapBegin,
        EventKind::SnapBid,
        EventKind::SnapAsk,
        EventKind::SnapEnd,
        EventKind::Clear,
        EventKind::Heartbeat,
    ];
    prop::collection::vec((0usize..kinds.len(), 1u32..4, 0u64..4), 1..300).prop_map(move |items| {
        let mut t = 1_000u64;
        items
            .into_iter()
            .map(|(k, inst, dt)| {
                t += dt; // 0 allowed: duplicate timestamps happen
                ev(kinds[k], inst, t)
            })
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn prop_index_matches_naive(evs in arb_events(), block_events in 1usize..9) {
        let d = tmp();
        let r = write_store(&d.path().join("s.fbstore"), block_events, &evs, true);
        let idx = SnapshotIndex::build(&r).unwrap();
        prop_assert_eq!(idx.entries(), &naive_entries(&evs, block_events)[..]);

        // sidecar roundtrip preserves arbitrary indexes
        let sp = d.path().join("s.fbsnpix");
        idx.save(&sp).unwrap();
        prop_assert_eq!(&SnapshotIndex::load(&sp).unwrap(), &idx);
    }

    #[test]
    fn prop_pit_scan_matches_naive_filter(
        evs in arb_events(),
        block_events in 1usize..9,
        t_off in 0u64..1200,
    ) {
        let d = tmp();
        let r = write_store(&d.path().join("s.fbstore"), block_events, &evs, true);
        let idx = SnapshotIndex::build(&r).unwrap();
        let t = 1_000 + t_off; // spans before/inside/after the mono range
        let naive = naive_entries(&evs, block_events);
        for inst in 1u32..4 {
            // naive latest_at: last complete snapshot with mono <= t
            let want = naive.iter().rfind(|e| e.instrument == inst && e.mono <= t);
            let got = idx.latest_at(inst, t);
            prop_assert_eq!(got, want);
            if let Some(entry) = got {
                let events = scan_events(&r, entry, t);
                prop_assert_eq!(events, naive_pit(&evs, entry, block_events, t));
            }
        }
    }
}

/// Informal local smoke: index build + PIT query timings on a larger
/// synthetic store. Not a benchmark — run explicitly with --ignored.
#[test]
#[ignore = "informal smoke timing, run explicitly"]
fn smoke_pit_timings() {
    let d = tmp();
    let mut evs = Vec::new();
    let mut t = 1_000u64;
    // ~206k events: 3 instruments x 100 rounds x (500-level snapshot + 185 deltas)
    for _round in 0..100 {
        for inst in 1..=3u32 {
            push_snap(&mut evs, inst, &mut t, 500);
            for _ in 0..185 {
                evs.push(ev(EventKind::BidSet, inst, t));
                t += 1;
            }
        }
    }
    let p = d.path().join("big.fbstore");
    let r = write_store(&p, 8192, &evs, true);
    println!(
        "store: {} events, {} blocks, {} bytes",
        r.n_events(),
        r.n_blocks(),
        std::fs::metadata(&p).unwrap().len()
    );

    let t0 = std::time::Instant::now();
    let idx = SnapshotIndex::build(&r).unwrap();
    println!("build: {} entries in {:?}", idx.len(), t0.elapsed());

    let mid = 1_000 + (t - 1_000) / 2;
    let t0 = std::time::Instant::now();
    let entry = *idx.latest_at(2, mid).unwrap();
    println!("latest_at: {:?} -> {entry:?}", t0.elapsed());

    let t0 = std::time::Instant::now();
    let mut kinds = 0u64;
    let delivered = pit_scan(&r, &entry, mid, |e| kinds += u64::from(e.kind)).unwrap();
    println!(
        "pit_scan: {delivered} events (kind sum {kinds}) in {:?}",
        t0.elapsed()
    );
    assert!(delivered > 500);
}
