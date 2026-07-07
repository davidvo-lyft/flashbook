//! Property tests for the proto crate: fixed-point parsing against an i128
//! reference model, format/parse roundtrips, event byte-cast roundtrips, and
//! rawlog torn-tail recovery at arbitrary cut points.

use flashbook_proto::event::{EVENT_SIZE, Event};
use flashbook_proto::fixed::{ParseFixedError, format_fixed, parse_fixed};
use flashbook_proto::rawlog::{RawLogReader, RawLogWriter, rkind, scan, truncate_to_valid};
use proptest::prelude::*;

/// Reference model: build the decimal string from components, compute the
/// exact expected mantissa with i128 arithmetic.
fn reference(int_part: u64, frac: &str, exp: i32, neg: bool) -> Result<i64, ParseFixedError> {
    let mut mant = i128::from(int_part);
    for c in frac.chars() {
        mant = mant * 10 + i128::from(c.to_digit(10).unwrap());
        if mant > i128::from(u64::MAX) * 1_000_000_000_000 {
            return Err(ParseFixedError::Overflow);
        }
    }
    let eff = 8 + exp - i32::try_from(frac.len()).unwrap();
    let mut v = mant;
    if eff >= 0 {
        for _ in 0..eff {
            v = v.checked_mul(10).ok_or(ParseFixedError::Overflow)?;
        }
    } else {
        for _ in 0..(-eff) {
            if v % 10 != 0 {
                return Err(ParseFixedError::PrecisionLoss);
            }
            v /= 10;
        }
    }
    if neg {
        v = -v;
    }
    if v > i128::from(i64::MAX) || v < i128::from(i64::MIN) {
        return Err(ParseFixedError::Overflow);
    }
    Ok(i64::try_from(v).unwrap())
}

proptest! {
    #[test]
    fn parse_matches_i128_reference(
        int_part in 0u64..u64::MAX / 2,
        frac in "[0-9]{0,12}",
        exp in -12i32..12,
        neg in any::<bool>(),
        with_exp in any::<bool>(),
    ) {
        let sign = if neg { "-" } else { "" };
        let mut s = format!("{sign}{int_part}");
        if !frac.is_empty() {
            s.push('.');
            s.push_str(&frac);
        }
        let effective_exp = if with_exp { exp } else { 0 };
        if with_exp {
            s.push_str(&format!("e{exp}"));
        }
        let got = parse_fixed(s.as_bytes());
        let want = reference(int_part, &frac, effective_exp, neg);
        prop_assert_eq!(got, want, "input {}", s);
    }

    #[test]
    fn format_parse_roundtrip(m in any::<i64>()) {
        let s = format_fixed(m);
        prop_assert_eq!(parse_fixed(s.as_bytes()), Ok(m), "formatted {}", s);
    }

    #[test]
    fn parse_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
        let _ = parse_fixed(&bytes);
    }

    #[test]
    fn event_bytes_roundtrip(seed in proptest::collection::vec(any::<u64>(), 1..64)) {
        let evs: Vec<Event> = seed
            .iter()
            .map(|&i| Event {
                recv_mono_ns: i,
                recv_wall_ns: i.wrapping_mul(3),
                venue_ts_ns: i.wrapping_mul(5),
                venue_seq: i.wrapping_mul(7),
                #[allow(clippy::cast_possible_wrap)]
                price: i.wrapping_mul(11) as i64,
                #[allow(clippy::cast_possible_wrap)]
                qty: i.wrapping_mul(13) as i64,
                aux: i.wrapping_mul(17),
                instrument: (i % 15) as u32 + 1,
                kind: (i % 11) as u8 + 1,
                venue: (i % 3) as u8 + 1,
                flags: (i % 8) as u8,
                rsvd: 0,
            })
            .collect();
        let bytes = Event::slice_as_bytes(&evs);
        prop_assert_eq!(bytes.len(), evs.len() * EVENT_SIZE);
        let back = Event::bytes_as_slice(bytes).unwrap();
        prop_assert_eq!(back, &evs[..]);
        let copied: Vec<Event> = Event::iter_unaligned(bytes).collect();
        prop_assert_eq!(&copied[..], &evs[..]);
    }

    /// Cut a valid segment at ANY byte offset within the record region:
    /// the reader must yield some valid prefix of records then flag a torn
    /// tail (or clean EOF if the cut lands exactly on a record boundary),
    /// and truncation must recover a cleanly-readable file.
    #[test]
    fn rawlog_survives_arbitrary_cuts(
        n_records in 1usize..20,
        cut_back in 1u64..200,
        payload_seed in any::<u64>(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("seg.fbraw");
        let mut w = RawLogWriter::create(&p, 2, 1, 2, b"{}").unwrap();
        for i in 0..n_records {
            let payload = format!("{{\"i\":{i},\"x\":{}}}", payload_seed);
            w.append(rkind::WS_TEXT, i as u64, i as u64, payload.as_bytes()).unwrap();
        }
        w.finish().unwrap();

        let full_len = std::fs::metadata(&p).unwrap().len();
        let header_len = {
            // header ends where record region begins; recover it by reading
            let rd = RawLogReader::open(&p).unwrap();
            rd.offset()
        };
        // cut somewhere strictly inside the record region
        let cut = std::cmp::max(header_len, full_len.saturating_sub(cut_back));
        let f = std::fs::File::options().write(true).open(&p).unwrap();
        f.set_len(cut).unwrap();
        drop(f);

        let rep = scan(&p).unwrap();
        prop_assert!(rep.records <= n_records as u64);
        if let Some((valid, _)) = rep.torn {
            truncate_to_valid(&p, valid).unwrap();
            let clean = scan(&p).unwrap();
            prop_assert!(clean.torn.is_none());
            prop_assert_eq!(clean.records, rep.records);
        } else {
            // cut landed exactly on a record boundary: clean file
            prop_assert_eq!(rep.valid_bytes, cut);
        }
    }
}
