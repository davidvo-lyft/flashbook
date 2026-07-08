//! Column encoding primitives: zigzag, unsigned LEB128 varints, and
//! stateful delta / delta-of-delta transforms. These are the whole
//! compression story of the tick store (plus optional per-block zstd),
//! chosen per column for the shapes real feed data has:
//!
//! - timestamps: near-constant increments -> delta-of-delta ~ 1-2 bytes
//! - sequence numbers / trade ids: +1 steps -> delta ~ 1 byte
//! - prices: cluster near the book -> delta ~ 1-3 bytes
//! - quantities / aux: repetitive small magnitudes -> plain zigzag varint

/// Map a signed value to unsigned so small magnitudes stay small.
#[inline]
pub fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// Inverse of [`zigzag`].
#[inline]
pub fn unzigzag(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

/// Append an unsigned LEB128 varint (1..=10 bytes).
#[inline]
pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Read an unsigned LEB128 varint at `*pos`, advancing it. `None` on
/// truncation or a >10-byte (overlong/overflow) encoding.
#[inline]
pub fn take_uvarint(b: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        let &byte = b.get(*pos)?;
        *pos += 1;
        if shift == 63 && byte > 1 {
            return None; // would overflow u64
        }
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

/// Encode a column of u64s as zigzag(delta) varints (first value is a delta
/// from 0, i.e. stored as-is zigzagged as i64 wrapping).
pub fn encode_delta_u64(vals: impl Iterator<Item = u64>, out: &mut Vec<u8>) {
    let mut prev: u64 = 0;
    for v in vals {
        let d = v.wrapping_sub(prev) as i64;
        put_uvarint(out, zigzag(d));
        prev = v;
    }
}

/// Decode `n` values encoded by [`encode_delta_u64`].
pub fn decode_delta_u64(b: &[u8], pos: &mut usize, n: usize, out: &mut Vec<u64>) -> Option<()> {
    let mut prev: u64 = 0;
    for _ in 0..n {
        let d = unzigzag(take_uvarint(b, pos)?);
        prev = prev.wrapping_add(d as u64);
        out.push(prev);
    }
    Some(())
}

/// Encode a column of u64s as zigzag(delta-of-delta) varints.
pub fn encode_dod_u64(vals: impl Iterator<Item = u64>, out: &mut Vec<u8>) {
    let mut prev: u64 = 0;
    let mut prev_delta: i64 = 0;
    for v in vals {
        let delta = v.wrapping_sub(prev) as i64;
        let dod = delta.wrapping_sub(prev_delta);
        put_uvarint(out, zigzag(dod));
        prev = v;
        prev_delta = delta;
    }
}

/// Decode `n` values encoded by [`encode_dod_u64`].
pub fn decode_dod_u64(b: &[u8], pos: &mut usize, n: usize, out: &mut Vec<u64>) -> Option<()> {
    let mut prev: u64 = 0;
    let mut prev_delta: i64 = 0;
    for _ in 0..n {
        let dod = unzigzag(take_uvarint(b, pos)?);
        let delta = prev_delta.wrapping_add(dod);
        prev = prev.wrapping_add(delta as u64);
        prev_delta = delta;
        out.push(prev);
    }
    Some(())
}

/// Encode a column of i64s as zigzag(delta) varints.
pub fn encode_delta_i64(vals: impl Iterator<Item = i64>, out: &mut Vec<u8>) {
    let mut prev: i64 = 0;
    for v in vals {
        put_uvarint(out, zigzag(v.wrapping_sub(prev)));
        prev = v;
    }
}

/// Decode `n` values encoded by [`encode_delta_i64`].
pub fn decode_delta_i64(b: &[u8], pos: &mut usize, n: usize, out: &mut Vec<i64>) -> Option<()> {
    let mut prev: i64 = 0;
    for _ in 0..n {
        prev = prev.wrapping_add(unzigzag(take_uvarint(b, pos)?));
        out.push(prev);
    }
    Some(())
}

/// Encode a column of i64s as plain zigzag varints (no delta).
pub fn encode_zz_i64(vals: impl Iterator<Item = i64>, out: &mut Vec<u8>) {
    for v in vals {
        put_uvarint(out, zigzag(v));
    }
}

/// Decode `n` values encoded by [`encode_zz_i64`].
pub fn decode_zz_i64(b: &[u8], pos: &mut usize, n: usize, out: &mut Vec<i64>) -> Option<()> {
    for _ in 0..n {
        out.push(unzigzag(take_uvarint(b, pos)?));
    }
    Some(())
}

/// Encode a column of u32s as plain varints.
pub fn encode_v_u32(vals: impl Iterator<Item = u32>, out: &mut Vec<u8>) {
    for v in vals {
        put_uvarint(out, u64::from(v));
    }
}

/// Decode `n` values encoded by [`encode_v_u32`].
pub fn decode_v_u32(b: &[u8], pos: &mut usize, n: usize, out: &mut Vec<u32>) -> Option<()> {
    for _ in 0..n {
        let v = take_uvarint(b, pos)?;
        out.push(u32::try_from(v).ok()?);
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn zigzag_goldens() {
        for (v, u) in [
            (0i64, 0u64),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (2, 4),
            (i64::MAX, u64::MAX - 1),
            (i64::MIN, u64::MAX),
        ] {
            assert_eq!(zigzag(v), u, "{v}");
            assert_eq!(unzigzag(u), v, "{u}");
        }
    }

    #[test]
    fn uvarint_goldens() {
        let mut b = Vec::new();
        put_uvarint(&mut b, 0);
        put_uvarint(&mut b, 127);
        put_uvarint(&mut b, 128);
        put_uvarint(&mut b, 300);
        put_uvarint(&mut b, u64::MAX);
        assert_eq!(&b[..5], &[0x00, 0x7f, 0x80, 0x01, 0xac]);
        let mut pos = 0;
        assert_eq!(take_uvarint(&b, &mut pos), Some(0));
        assert_eq!(take_uvarint(&b, &mut pos), Some(127));
        assert_eq!(take_uvarint(&b, &mut pos), Some(128));
        assert_eq!(take_uvarint(&b, &mut pos), Some(300));
        assert_eq!(take_uvarint(&b, &mut pos), Some(u64::MAX));
        assert_eq!(pos, b.len());
    }

    #[test]
    fn uvarint_rejects_truncation_and_overflow() {
        // truncated multi-byte
        let mut pos = 0;
        assert_eq!(take_uvarint(&[0x80], &mut pos), None);
        // 11-byte encoding (too long)
        let mut pos = 0;
        assert_eq!(take_uvarint(&[0x80; 11], &mut pos), None);
        // 10th byte with high bits beyond u64
        let mut b = vec![0xffu8; 9];
        b.push(0x02);
        let mut pos = 0;
        assert_eq!(take_uvarint(&b, &mut pos), None);
        // empty
        let mut pos = 0;
        assert_eq!(take_uvarint(&[], &mut pos), None);
    }

    #[test]
    fn dod_compresses_regular_timestamps() {
        // 1ms cadence: after the first two values, each DoD is 0 -> 1 byte
        let vals: Vec<u64> = (0..1000u64)
            .map(|i| 1_700_000_000_000_000_000 + i * 1_000_000)
            .collect();
        let mut out = Vec::new();
        encode_dod_u64(vals.iter().copied(), &mut out);
        assert!(out.len() < 1020, "dod encoding too large: {}", out.len());
        let mut pos = 0;
        let mut back = Vec::new();
        decode_dod_u64(&out, &mut pos, vals.len(), &mut back).unwrap();
        assert_eq!(back, vals);
        assert_eq!(pos, out.len());
    }

    proptest! {
        #[test]
        fn zigzag_roundtrip(v in any::<i64>()) {
            prop_assert_eq!(unzigzag(zigzag(v)), v);
        }

        #[test]
        fn uvarint_roundtrip(vals in proptest::collection::vec(any::<u64>(), 0..200)) {
            let mut b = Vec::new();
            for &v in &vals {
                put_uvarint(&mut b, v);
            }
            let mut pos = 0;
            let mut back = Vec::new();
            while pos < b.len() {
                back.push(take_uvarint(&b, &mut pos).unwrap());
            }
            prop_assert_eq!(back, vals);
        }

        #[test]
        fn delta_u64_roundtrip(vals in proptest::collection::vec(any::<u64>(), 0..300)) {
            let mut out = Vec::new();
            encode_delta_u64(vals.iter().copied(), &mut out);
            let mut pos = 0;
            let mut back = Vec::new();
            decode_delta_u64(&out, &mut pos, vals.len(), &mut back).unwrap();
            prop_assert_eq!(back, vals);
            prop_assert_eq!(pos, out.len());
        }

        #[test]
        fn dod_u64_roundtrip(vals in proptest::collection::vec(any::<u64>(), 0..300)) {
            let mut out = Vec::new();
            encode_dod_u64(vals.iter().copied(), &mut out);
            let mut pos = 0;
            let mut back = Vec::new();
            decode_dod_u64(&out, &mut pos, vals.len(), &mut back).unwrap();
            prop_assert_eq!(back, vals);
        }

        #[test]
        fn delta_i64_roundtrip(vals in proptest::collection::vec(any::<i64>(), 0..300)) {
            let mut out = Vec::new();
            encode_delta_i64(vals.iter().copied(), &mut out);
            let mut pos = 0;
            let mut back = Vec::new();
            decode_delta_i64(&out, &mut pos, vals.len(), &mut back).unwrap();
            prop_assert_eq!(back, vals);
        }

        #[test]
        fn zz_i64_roundtrip(vals in proptest::collection::vec(any::<i64>(), 0..300)) {
            let mut out = Vec::new();
            encode_zz_i64(vals.iter().copied(), &mut out);
            let mut pos = 0;
            let mut back = Vec::new();
            decode_zz_i64(&out, &mut pos, vals.len(), &mut back).unwrap();
            prop_assert_eq!(back, vals);
        }

        #[test]
        fn v_u32_roundtrip(vals in proptest::collection::vec(any::<u32>(), 0..300)) {
            let mut out = Vec::new();
            encode_v_u32(vals.iter().copied(), &mut out);
            let mut pos = 0;
            let mut back = Vec::new();
            decode_v_u32(&out, &mut pos, vals.len(), &mut back).unwrap();
            prop_assert_eq!(back, vals);
        }

        #[test]
        fn decoders_never_panic_on_garbage(b in proptest::collection::vec(any::<u8>(), 0..200), n in 0usize..64) {
            let mut pos = 0;
            let mut out64 = Vec::new();
            let _ = decode_dod_u64(&b, &mut pos, n, &mut out64);
            let mut pos = 0;
            let mut outi = Vec::new();
            let _ = decode_delta_i64(&b, &mut pos, n, &mut outi);
            let mut pos = 0;
            let mut out32 = Vec::new();
            let _ = decode_v_u32(&b, &mut pos, n, &mut out32);
        }
    }
}
