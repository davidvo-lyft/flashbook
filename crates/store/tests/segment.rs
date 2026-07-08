//! Integration tests for the segment file layer: seal/recovery round trips,
//! arbitrary-truncation recovery, footer corruption fallback, and the
//! directory-driven mono range scan vs. a naive filter.

use std::fs::File;
use std::path::Path;

use flashbook_proto::event::Event;
use flashbook_store::block::MAX_BLOCK_EVENTS;
use flashbook_store::segment::{
    DIR_ENTRY_LEN, FILE_HEADER_LEN, FOOTER_TRAILER_LEN, SegmentError, StoreReader, StoreWriter,
    recover_truncate,
};
use proptest::prelude::*;

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// Deterministic mono-nondecreasing events (realistic-ish shapes).
fn mk_events(n: usize, seed: u64) -> Vec<Event> {
    let mut evs = Vec::with_capacity(n);
    let mut t = 5_000_000_000u64;
    let mut price = 6_358_964_000_000i64;
    for i in 0..n {
        let x = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add((i as u64).wrapping_mul(2_862_933_555_777_941_757));
        t += x % 700_000; // may be 0: duplicates allowed
        price += ((x >> 8) % 2001) as i64 - 1000;
        evs.push(Event {
            recv_mono_ns: t,
            recv_wall_ns: t + 1_700_000_000_000_000_000,
            venue_ts_ns: t + 1_700_000_000_000_000_000 - 3_000_000,
            venue_seq: 1000 + i as u64,
            price,
            qty: ((x >> 16) % 1_000_000) as i64,
            aux: x >> 32,
            instrument: (x % 15) as u32 + 1,
            kind: (x % 11) as u8 + 1,
            venue: (x % 3) as u8 + 1,
            flags: (x % 8) as u8,
            rsvd: 0,
        });
    }
    evs
}

fn write_all(
    path: &Path,
    meta: &[u8],
    block_events: usize,
    zstd: Option<i32>,
    events: &[Event],
    seal: bool,
) -> u64 {
    let mut w = StoreWriter::create(path, meta, block_events, zstd).unwrap();
    for e in events {
        w.append(e).unwrap();
    }
    if seal {
        w.seal().unwrap()
    } else {
        w.checkpoint().unwrap();
        let bytes = w.bytes();
        drop(w); // simulated crash: no seal
        bytes
    }
}

fn read_all(r: &StoreReader) -> Vec<Event> {
    let mut out = Vec::new();
    r.scan(|e| out.push(*e)).unwrap();
    out
}

#[test]
fn multi_block_roundtrip_sealed() {
    let d = tmp();
    let p = d.path().join("a.fbstore");
    let evs = mk_events(1000, 42); // 256/block -> 3 full + 1 partial
    let sealed_len = write_all(&p, br#"{"phase":3}"#, 256, Some(3), &evs, true);
    assert_eq!(sealed_len, std::fs::metadata(&p).unwrap().len());

    let r = StoreReader::open(&p).unwrap();
    assert!(r.sealed());
    assert!(r.torn().is_none());
    assert_eq!(r.n_blocks(), 4);
    assert_eq!(r.n_events(), 1000);
    assert_eq!(r.meta(), br#"{"phase":3}"#);
    assert_eq!(read_all(&r), evs);
    assert_eq!(r.verify().unwrap(), 1000);

    // directory ranges match the data
    let b0 = &r.blocks()[0];
    assert_eq!(b0.n_events, 256);
    assert_eq!(b0.min_recv_mono, evs[0].recv_mono_ns);
    assert_eq!(b0.max_recv_mono, evs[255].recv_mono_ns);
}

#[test]
fn sealed_and_unsealed_reads_agree() {
    let d = tmp();
    let evs = mk_events(700, 7);
    let ps = d.path().join("sealed.fbstore");
    let pu = d.path().join("unsealed.fbstore");
    write_all(&ps, b"m", 128, Some(3), &evs, true);
    write_all(&pu, b"m", 128, Some(3), &evs, false);

    let rs = StoreReader::open(&ps).unwrap();
    let ru = StoreReader::open(&pu).unwrap();
    assert!(rs.sealed());
    assert!(!ru.sealed());
    assert!(ru.torn().is_none());
    // identical directories (footer-loaded vs scan-built) ...
    assert_eq!(rs.blocks(), ru.blocks());
    // ... identical events ...
    assert_eq!(read_all(&rs), evs);
    assert_eq!(read_all(&ru), evs);
    // ... and the block region is byte-for-byte the same file prefix.
    let bs = std::fs::read(&ps).unwrap();
    let bu = std::fs::read(&pu).unwrap();
    assert_eq!(&bs[..bu.len()], &bu[..]);
    assert!(bs.len() > bu.len()); // footer
    assert_eq!(
        bs.len() - bu.len(),
        rs.n_blocks() * DIR_ENTRY_LEN + FOOTER_TRAILER_LEN
    );
}

#[test]
fn meta_roundtrip_including_empty() {
    let d = tmp();
    for (name, meta) in [
        ("m1", &br#"{"venues":["kraken"],"depth":100}"#[..]),
        ("m2", &b""[..]),
    ] {
        let p = d.path().join(name);
        write_all(&p, meta, 8, None, &mk_events(3, 1), true);
        let r = StoreReader::open(&p).unwrap();
        assert_eq!(r.meta(), meta);
    }
}

#[test]
fn empty_store_header_only_opens() {
    let d = tmp();
    let p = d.path().join("empty-unsealed.fbstore");
    let w = StoreWriter::create(&p, b"{}", 64, None).unwrap();
    drop(w); // header only, no seal
    let r = StoreReader::open(&p).unwrap();
    assert!(!r.sealed());
    assert_eq!(r.n_blocks(), 0);
    assert_eq!(r.n_events(), 0);
    assert!(r.torn().is_none());
    assert_eq!(r.verify().unwrap(), 0);
    assert_eq!(r.scan_mono_range(0, u64::MAX, |_| {}).unwrap(), 0);
}

#[test]
fn empty_store_sealed_opens() {
    let d = tmp();
    let p = d.path().join("empty-sealed.fbstore");
    let w = StoreWriter::create(&p, b"{}", 64, None).unwrap();
    w.seal().unwrap();
    let r = StoreReader::open(&p).unwrap();
    assert!(r.sealed());
    assert_eq!(r.n_blocks(), 0);
    assert_eq!(r.n_events(), 0);
    assert_eq!(r.verify().unwrap(), 0);
}

#[test]
fn out_of_order_append_rejected_equal_allowed() {
    let d = tmp();
    let p = d.path().join("ooo.fbstore");
    let mut w = StoreWriter::create(&p, b"", 64, None).unwrap();
    let mut e = mk_events(1, 3)[0];
    e.recv_mono_ns = 100;
    w.append(&e).unwrap();
    e.recv_mono_ns = 100; // equal: allowed (non-decreasing)
    w.append(&e).unwrap();
    e.recv_mono_ns = 99;
    match w.append(&e) {
        Err(SegmentError::OutOfOrder {
            last: 100,
            next: 99,
        }) => {}
        other => panic!("expected OutOfOrder, got {other:?}"),
    }
    // writer still usable after the rejection
    e.recv_mono_ns = 101;
    w.append(&e).unwrap();
    w.seal().unwrap();
    let r = StoreReader::open(&p).unwrap();
    assert_eq!(r.n_events(), 3);
}

#[test]
fn rsvd_must_be_zero() {
    let d = tmp();
    let p = d.path().join("rsvd.fbstore");
    let mut w = StoreWriter::create(&p, b"", 64, None).unwrap();
    let mut e = mk_events(1, 3)[0];
    e.rsvd = 1;
    assert!(matches!(w.append(&e), Err(SegmentError::Config(_))));
}

#[test]
fn writer_config_rejected() {
    let d = tmp();
    assert!(matches!(
        StoreWriter::create(&d.path().join("z.fbstore"), b"", 0, None),
        Err(SegmentError::Config(_))
    ));
    assert!(matches!(
        StoreWriter::create(
            &d.path().join("big.fbstore"),
            b"",
            MAX_BLOCK_EVENTS + 1,
            None
        ),
        Err(SegmentError::Config(_))
    ));
    // create refuses to overwrite
    let p = d.path().join("dup.fbstore");
    StoreWriter::create(&p, b"", 8, None).unwrap();
    assert!(StoreWriter::create(&p, b"", 8, None).is_err());
}

#[test]
fn exact_block_multiple_no_partial_block() {
    let d = tmp();
    let p = d.path().join("exact.fbstore");
    let evs = mk_events(384, 11);
    let mut w = StoreWriter::create(&p, b"", 128, None).unwrap();
    for e in &evs {
        w.append(e).unwrap();
    }
    assert_eq!(w.n_blocks(), 3);
    assert_eq!(w.pending_events(), 0);
    assert_eq!(w.events(), 384);
    w.seal().unwrap();
    let r = StoreReader::open(&p).unwrap();
    assert_eq!(r.n_blocks(), 3);
    assert!(r.blocks().iter().all(|b| b.n_events == 128));
    assert_eq!(read_all(&r), evs);
}

#[test]
fn max_block_events_boundary() {
    // big block_events at the format cap, exact multiple: 2 full blocks
    let d = tmp();
    let p = d.path().join("max.fbstore");
    let evs = mk_events(2 * MAX_BLOCK_EVENTS, 13);
    write_all(&p, b"", MAX_BLOCK_EVENTS, Some(1), &evs, true);
    let r = StoreReader::open(&p).unwrap();
    assert_eq!(r.n_blocks(), 2);
    assert_eq!(r.n_events(), 2 * MAX_BLOCK_EVENTS as u64);
    assert_eq!(r.verify().unwrap(), 2 * MAX_BLOCK_EVENTS as u64);
    assert_eq!(read_all(&r), evs);
}

#[test]
fn mixed_zstd_and_raw_blocks_in_one_file() {
    let d = tmp();
    let p = d.path().join("mixed.fbstore");
    // block 0: 4096 highly repetitive events -> zstd shrinks it -> stored
    // compressed. block 1: a single event with large distinct field values
    // -> its ~40-byte column body can't repay the zstd frame overhead ->
    // encode_block keeps the raw representation. One file, both kinds.
    let n = 4096;
    let mut evs = Vec::new();
    let mut t = 1_000_000u64;
    for _ in 0..n {
        t += 1000;
        let mut e = Event::ZERO;
        e.recv_mono_ns = t;
        e.recv_wall_ns = t;
        e.kind = 1;
        e.venue = 1;
        e.instrument = 1;
        evs.push(e);
    }
    let x = 0xBF58_476D_1CE4_E5B9u64;
    evs.push(Event {
        recv_mono_ns: t + 1000,
        recv_wall_ns: x,
        venue_ts_ns: x.wrapping_mul(31),
        venue_seq: x.wrapping_mul(17),
        price: x.wrapping_mul(13) as i64,
        qty: x.wrapping_mul(7) as i64,
        aux: x.wrapping_mul(3),
        instrument: x as u32,
        kind: 3,
        venue: 2,
        flags: 1,
        rsvd: 0,
    });
    write_all(&p, b"", n, Some(3), &evs, true);
    let r = StoreReader::open(&p).unwrap();
    assert_eq!(r.n_blocks(), 2);
    let mut buf = Vec::new();
    let h0 = r.decode_block(0, &mut buf).unwrap();
    let h1 = r.decode_block(1, &mut buf).unwrap();
    assert!(h0.zstd, "repetitive block should be stored compressed");
    assert!(!h1.zstd, "tiny block should be stored raw");
    assert_eq!(buf, evs); // both decodes appended in order
    assert_eq!(r.verify().unwrap(), n as u64 + 1);
}

#[test]
fn checkpoint_partial_block_survives_crash() {
    let d = tmp();
    let p = d.path().join("crash.fbstore");
    let evs = mk_events(150, 21);
    let mut w = StoreWriter::create(&p, b"", 100, None).unwrap();
    for e in &evs {
        w.append(e).unwrap();
    }
    assert_eq!(w.pending_events(), 50);
    w.checkpoint().unwrap(); // partial block forced out + fsync
    assert_eq!(w.pending_events(), 0);
    drop(w); // crash: never sealed

    let r = StoreReader::open(&p).unwrap();
    assert!(!r.sealed());
    assert!(r.torn().is_none());
    assert_eq!(r.n_blocks(), 2);
    assert_eq!(r.blocks()[1].n_events, 50);
    assert_eq!(read_all(&r), evs);
}

#[test]
fn unsynced_pending_events_lost_but_file_clean() {
    let d = tmp();
    let p = d.path().join("pending.fbstore");
    let evs = mk_events(130, 23);
    let mut w = StoreWriter::create(&p, b"", 100, None).unwrap();
    for e in &evs {
        w.append(e).unwrap();
    }
    drop(w); // 30 pending events never became a block
    let r = StoreReader::open(&p).unwrap();
    assert_eq!(r.n_events(), 100);
    assert!(r.torn().is_none());
    assert_eq!(read_all(&r), evs[..100]);
}

#[test]
fn truncation_recovery_deterministic() {
    let d = tmp();
    let p = d.path().join("full.fbstore");
    let evs = mk_events(500, 31);
    write_all(&p, b"meta", 128, Some(3), &evs, true);
    let full = StoreReader::open(&p).unwrap();
    let ends: Vec<u64> = {
        // block end offsets from consecutive directory offsets + data end
        let mut v: Vec<u64> = full
            .blocks()
            .iter()
            .skip(1)
            .map(|b| b.file_offset)
            .collect();
        v.push(
            full.blocks().last().unwrap().file_offset + {
                let mut buf = Vec::new();
                let h = full.decode_block(full.n_blocks() - 1, &mut buf).unwrap();
                h.total_len() as u64
            },
        );
        v
    };
    let file_len = std::fs::metadata(&p).unwrap().len();
    drop(full);

    // cut mid-block 2 (between end of block 1 and end of block 2)
    let cut = (ends[0] + ends[1]) / 2;
    let pc = d.path().join("cut.fbstore");
    std::fs::copy(&p, &pc).unwrap();
    File::options()
        .write(true)
        .open(&pc)
        .unwrap()
        .set_len(cut)
        .unwrap();

    let r = StoreReader::open(&pc).unwrap();
    assert!(!r.sealed());
    assert_eq!(r.n_blocks(), 1);
    assert_eq!(r.n_events(), 128);
    let torn = r.torn().unwrap();
    assert_eq!(torn.valid_bytes, ends[0]);
    drop(r);

    let new_len = recover_truncate(&pc).unwrap();
    assert_eq!(new_len, ends[0]);
    let r = StoreReader::open(&pc).unwrap();
    assert!(r.torn().is_none());
    assert_eq!(read_all(&r), evs[..128]);

    // recover_truncate on an intact sealed file is a no-op
    assert_eq!(recover_truncate(&p).unwrap(), file_len);
    assert!(StoreReader::open(&p).unwrap().sealed());
}

#[test]
fn footer_crc_corruption_falls_back_to_scan() {
    let d = tmp();
    let p = d.path().join("badfooter.fbstore");
    let evs = mk_events(300, 37);
    write_all(&p, b"", 128, None, &evs, true);
    let dir_start = {
        let r = StoreReader::open(&p).unwrap();
        assert!(r.sealed());
        let mut buf = Vec::new();
        let h = r.decode_block(r.n_blocks() - 1, &mut buf).unwrap();
        r.blocks().last().unwrap().file_offset + h.total_len() as u64
    };
    // flip a byte inside the directory: crc must fail
    let mut bytes = std::fs::read(&p).unwrap();
    bytes[dir_start as usize] ^= 0xFF;
    std::fs::write(&p, &bytes).unwrap();

    let r = StoreReader::open(&p).unwrap();
    assert!(!r.sealed(), "corrupt footer must not be trusted");
    assert_eq!(r.n_blocks(), 3);
    assert_eq!(read_all(&r), evs); // data blocks fully readable
    // the corrupt directory shows up as a torn tail at its start offset
    assert_eq!(r.torn().unwrap().valid_bytes, dir_start);
    drop(r);

    // recovery turns it into a clean unsealed file
    recover_truncate(&p).unwrap();
    let r = StoreReader::open(&p).unwrap();
    assert!(!r.sealed());
    assert!(r.torn().is_none());
    assert_eq!(read_all(&r), evs);
}

#[test]
fn footer_bad_dir_start_falls_back_to_scan() {
    let d = tmp();
    let p = d.path().join("badstart.fbstore");
    let evs = mk_events(200, 41);
    write_all(&p, b"", 64, None, &evs, true);
    let mut bytes = std::fs::read(&p).unwrap();
    let n = bytes.len();
    // clobber directory_start_offset (u64 right before the footer magic)
    bytes[n - 16..n - 8].copy_from_slice(&u64::MAX.to_le_bytes());
    std::fs::write(&p, &bytes).unwrap();
    let r = StoreReader::open(&p).unwrap();
    assert!(!r.sealed());
    assert_eq!(read_all(&r), evs);
    assert!(r.torn().is_some());
}

#[test]
fn corrupt_block_body_detected() {
    let d = tmp();
    let p = d.path().join("corruptblock.fbstore");
    let evs = mk_events(300, 43);
    write_all(&p, b"", 128, Some(3), &evs, true);
    let r = StoreReader::open(&p).unwrap();
    let mid = r.blocks()[1].file_offset as usize;
    let mut buf = Vec::new();
    let h1 = r.decode_block(1, &mut buf).unwrap();
    drop(r);

    let mut bytes = std::fs::read(&p).unwrap();
    bytes[mid + h1.total_len() - 1] ^= 0xFF; // last body byte of block 1
    std::fs::write(&p, &bytes).unwrap();

    // sealed path: directory still valid, but decoding block 1 fails CRC
    let r = StoreReader::open(&p).unwrap();
    assert!(r.sealed());
    assert!(r.decode_block(0, &mut Vec::new()).is_ok());
    assert!(r.decode_block(1, &mut Vec::new()).is_err());
    assert!(r.verify().is_err());
}

#[test]
fn bad_file_header_rejected() {
    let d = tmp();
    let p = d.path().join("bad.fbstore");
    std::fs::write(&p, b"NOTMAGIC\x01\x00\x00\x00\x00\x00\x00\x00").unwrap();
    assert!(matches!(
        StoreReader::open(&p),
        Err(SegmentError::BadHeader(_))
    ));
    let p2 = d.path().join("short.fbstore");
    std::fs::write(&p2, b"FBST").unwrap();
    assert!(matches!(
        StoreReader::open(&p2),
        Err(SegmentError::BadHeader(_))
    ));
    let p3 = d.path().join("metatrunc.fbstore");
    let mut h = Vec::new();
    h.extend_from_slice(b"FBSTORE1");
    h.extend_from_slice(&[1, 0, 0, 0]);
    h.extend_from_slice(&100u32.to_le_bytes()); // claims 100 meta bytes, has 0
    std::fs::write(&p3, &h).unwrap();
    assert!(matches!(
        StoreReader::open(&p3),
        Err(SegmentError::BadHeader(_))
    ));
}

#[test]
fn scan_mono_range_edges() {
    let d = tmp();
    let p = d.path().join("range.fbstore");
    let evs = mk_events(600, 47);
    write_all(&p, b"", 64, None, &evs, true);
    let r = StoreReader::open(&p).unwrap();
    let lo_all = evs[0].recv_mono_ns;
    let hi_all = evs[599].recv_mono_ns;
    assert_eq!(r.scan_mono_range(lo_all, hi_all, |_| {}).unwrap(), 600);
    assert_eq!(r.scan_mono_range(0, u64::MAX, |_| {}).unwrap(), 600);
    assert_eq!(r.scan_mono_range(hi_all + 1, u64::MAX, |_| {}).unwrap(), 0);
    assert_eq!(
        r.scan_mono_range(0, lo_all.saturating_sub(1), |_| {})
            .unwrap(),
        0
    );
    assert_eq!(r.scan_mono_range(500, 400, |_| {}).unwrap(), 0); // lo > hi
    // single-point query
    let t = evs[300].recv_mono_ns;
    let naive = evs.iter().filter(|e| e.recv_mono_ns == t).count() as u64;
    assert_eq!(r.scan_mono_range(t, t, |_| {}).unwrap(), naive);
}

#[test]
fn decode_block_index_out_of_range() {
    let d = tmp();
    let p = d.path().join("idx.fbstore");
    write_all(&p, b"", 8, None, &mk_events(8, 51), true);
    let r = StoreReader::open(&p).unwrap();
    assert!(matches!(
        r.decode_block(1, &mut Vec::new()),
        Err(SegmentError::BadBlockIndex { idx: 1, blocks: 1 })
    ));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Truncate a sealed file at an arbitrary point >= the file header:
    /// the reader recovers exactly the blocks that fully survived, flags
    /// a torn tail unless the cut lands on a block boundary, and
    /// recover_truncate makes the file clean.
    #[test]
    fn recovery_from_arbitrary_truncation(seed in any::<u64>(), frac in 0.0f64..1.0) {
        let d = tmp();
        let p = d.path().join("t.fbstore");
        let evs = mk_events(200, seed);
        write_all(&p, b"pm", 32, if seed % 2 == 0 { Some(1) } else { None }, &evs, true);
        let full_len = std::fs::metadata(&p).unwrap().len();
        let (ends, data_start) = {
            let r = StoreReader::open(&p).unwrap();
            prop_assert!(r.sealed());
            let mut ends: Vec<u64> = r.blocks().iter().skip(1).map(|b| b.file_offset).collect();
            let mut buf = Vec::new();
            let h = r.decode_block(r.n_blocks() - 1, &mut buf).unwrap();
            ends.push(r.blocks().last().unwrap().file_offset + h.total_len() as u64);
            (ends, r.blocks()[0].file_offset)
        };

        let min_cut = FILE_HEADER_LEN as u64 + 2; // keep the header + meta intact
        prop_assert_eq!(min_cut, data_start);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let cut = min_cut + ((full_len - min_cut) as f64 * frac) as u64;

        File::options().write(true).open(&p).unwrap().set_len(cut).unwrap();
        let r = StoreReader::open(&p).unwrap();
        let expect_blocks = ends.iter().filter(|&&e| e <= cut).count();
        prop_assert_eq!(r.n_blocks(), expect_blocks);
        let clean_points: Vec<u64> = std::iter::once(data_start).chain(ends.iter().copied()).collect();
        let expect_torn = cut != full_len && !clean_points.contains(&cut);
        prop_assert_eq!(r.torn().is_some(), expect_torn, "cut={} ends={:?}", cut, ends);
        prop_assert_eq!(r.sealed(), cut == full_len);
        let got = read_all(&r);
        prop_assert_eq!(&got[..], &evs[..got.len()]);
        drop(r);

        let new_len = recover_truncate(&p).unwrap();
        let r = StoreReader::open(&p).unwrap();
        prop_assert!(r.torn().is_none());
        prop_assert_eq!(r.n_blocks(), expect_blocks);
        prop_assert_eq!(std::fs::metadata(&p).unwrap().len(), new_len);
        prop_assert_eq!(r.verify().unwrap(), r.n_events());
    }

    /// scan_mono_range == naive filter, for random mono distributions
    /// (bursts of duplicates included) and random query windows.
    #[test]
    fn range_scan_equals_naive_filter(
        seed in any::<u64>(),
        n in 1usize..400,
        gaps in proptest::collection::vec(0u64..3_000, 1..400),
        qlo in 0.0f64..1.2,
        qspan in 0.0f64..0.6,
        sealed in any::<bool>(),
    ) {
        let d = tmp();
        let p = d.path().join("r.fbstore");
        let mut evs = mk_events(n, seed);
        // reshape mono values: cumulative random gaps (many zero => dups)
        let mut t = 1_000u64;
        for (i, e) in evs.iter_mut().enumerate() {
            t += gaps[i % gaps.len()];
            e.recv_mono_ns = t;
        }
        write_all(&p, b"", 16, Some(1), &evs, sealed);
        let r = StoreReader::open(&p).unwrap();

        let t0 = evs[0].recv_mono_ns;
        let t1 = evs[n - 1].recv_mono_ns;
        let span = (t1 - t0).max(1);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let lo = t0.saturating_add((span as f64 * qlo) as u64).saturating_sub(span / 2);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let hi = lo.saturating_add((span as f64 * qspan) as u64);

        let mut got = Vec::new();
        let count = r.scan_mono_range(lo, hi, |e| got.push(*e)).unwrap();
        let want: Vec<Event> = evs
            .iter()
            .filter(|e| (lo..=hi).contains(&e.recv_mono_ns))
            .copied()
            .collect();
        prop_assert_eq!(count as usize, want.len());
        prop_assert_eq!(got, want);
    }

    /// Sealed and unsealed readers agree on arbitrary event streams.
    #[test]
    fn sealed_unsealed_agree_prop(seed in any::<u64>(), n in 1usize..300, be in 1usize..64) {
        let d = tmp();
        let evs = mk_events(n, seed);
        let ps = d.path().join("s.fbstore");
        let pu = d.path().join("u.fbstore");
        write_all(&ps, b"x", be, Some(1), &evs, true);
        write_all(&pu, b"x", be, Some(1), &evs, false);
        let rs = StoreReader::open(&ps).unwrap();
        let ru = StoreReader::open(&pu).unwrap();
        prop_assert_eq!(rs.blocks(), ru.blocks());
        prop_assert_eq!(read_all(&rs), read_all(&ru));
        prop_assert_eq!(rs.verify().unwrap(), n as u64);
        prop_assert_eq!(ru.verify().unwrap(), n as u64);
    }
}

/// Informal smoke at soak-capture scale (~139k events, like data/smoke).
/// Ignored by default; run with `--ignored --nocapture` for rough numbers
/// (unoptimized build, busy machine — NOT official).
#[test]
#[ignore = "informal smoke numbers only; run with --ignored --nocapture"]
fn smoke_synthetic_scale() {
    let d = tmp();
    let p = d.path().join("smoke.fbstore");
    let n = 138_964; // matches the smoke capture's message count
    let evs = mk_events(n, 2026);

    let t0 = std::time::Instant::now();
    let mut w = StoreWriter::create(&p, br#"{"smoke":true}"#, 8192, Some(3)).unwrap();
    for e in &evs {
        w.append(e).unwrap();
    }
    let sealed_len = w.seal().unwrap();
    let write_dt = t0.elapsed();

    let t1 = std::time::Instant::now();
    let r = StoreReader::open(&p).unwrap();
    let mut count = 0u64;
    r.scan(|_| count += 1).unwrap();
    let read_dt = t1.elapsed();
    assert_eq!(count, n as u64);
    assert_eq!(r.verify().unwrap(), n as u64);

    let raw = n as u64 * 64;
    println!(
        "smoke: {n} events; file {sealed_len} B ({:.2} B/event, {:.1}x vs 64B raw); \
         write {write_dt:?} ({:.1} Mevt/s); open+scan {read_dt:?} ({:.1} Mevt/s)",
        sealed_len as f64 / n as f64,
        raw as f64 / sealed_len as f64,
        n as f64 / write_dt.as_secs_f64() / 1e6,
        n as f64 / read_dt.as_secs_f64() / 1e6,
    );
}
