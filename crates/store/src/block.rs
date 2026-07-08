//! Block format: the unit of storage, compression, indexing and recovery.
//!
//! A block encodes up to [`MAX_BLOCK_EVENTS`] events column-wise (one stream
//! per Event field, each with the encoding that fits its shape — see
//! [`crate::varint`]), optionally zstd-compresses the concatenated column
//! bytes, and self-delimits with a fixed header + CRC32 so files recover
//! from torn writes by truncating at the last valid block.
//!
//! Layout (little-endian):
//!
//! ```text
//! u32 magic 0xFB5B_10C4 | u8 version=1 | u8 flags (bit0 = zstd)
//! u16 reserved | u32 n_events | u32 body_len (as stored)
//! u32 raw_len (pre-compression) | u64 min_recv_mono | u64 max_recv_mono
//! u64 min_recv_wall | u64 max_recv_wall | u32 crc32(body as stored)
//! body bytes
//! ```
//!
//! Column order in the body: recv_mono (DoD), recv_wall (DoD), venue_ts
//! (delta), venue_seq (delta), price (delta zz), qty (zz), aux (delta),
//! instrument (varint), then kind/venue/flags as raw byte runs. `rsvd` is
//! not stored (asserted 0, reconstructed as 0) — roundtrip is byte-exact
//! for every event the writer accepts.

use flashbook_proto::event::{EVENT_SIZE, Event};

use crate::varint::{
    decode_delta_i64, decode_delta_u64, decode_dod_u64, decode_v_u32, decode_zz_i64,
    encode_delta_i64, encode_delta_u64, encode_dod_u64, encode_v_u32, encode_zz_i64,
};

/// Block magic.
pub const BLOCK_MAGIC: u32 = 0xFB5B_10C4;
/// Format version.
pub const BLOCK_VERSION: u8 = 1;
/// Header size on disk.
pub const HEADER_LEN: usize = 4 + 1 + 1 + 2 + 4 + 4 + 4 + 8 + 8 + 8 + 8 + 4;
/// Hard cap on events per block (format limit; writers default lower).
pub const MAX_BLOCK_EVENTS: usize = 65_536;
/// Sanity cap on a stored body (fits any 65536-event block comfortably).
pub const MAX_BODY_LEN: usize = 64 * 1024 * 1024;

/// Flags bit: body is zstd-compressed.
pub const FLAG_ZSTD: u8 = 1;

/// Errors from block encode/decode.
#[derive(Debug, thiserror::Error)]
pub enum BlockError {
    /// Writer input violated a format invariant.
    #[error("encode: {0}")]
    Encode(&'static str),
    /// Bytes don't start with a valid header.
    #[error("bad header: {0}")]
    BadHeader(&'static str),
    /// Header valid but body missing/truncated/corrupt — torn tail.
    #[error("torn block: {0}")]
    Torn(&'static str),
    /// Body present but column streams are inconsistent.
    #[error("corrupt body: {0}")]
    Corrupt(&'static str),
}

/// Parsed block header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    /// Events in the block.
    pub n_events: u32,
    /// Stored body length.
    pub body_len: u32,
    /// Pre-compression body length.
    pub raw_len: u32,
    /// Body is zstd-compressed.
    pub zstd: bool,
    /// Min recv_mono_ns in the block (time index).
    pub min_recv_mono: u64,
    /// Max recv_mono_ns.
    pub max_recv_mono: u64,
    /// Min recv_wall_ns.
    pub min_recv_wall: u64,
    /// Max recv_wall_ns.
    pub max_recv_wall: u64,
    /// CRC32 of the stored body.
    pub crc: u32,
}

impl BlockHeader {
    /// Total on-disk size of the block (header + body).
    pub fn total_len(&self) -> usize {
        HEADER_LEN + self.body_len as usize
    }
}

/// Encode `events` into `out` (appended). `zstd_level`: None = store raw,
/// Some(level) = compress the column body. Returns the stored size.
pub fn encode_block(
    events: &[Event],
    zstd_level: Option<i32>,
    out: &mut Vec<u8>,
) -> Result<usize, BlockError> {
    if events.is_empty() {
        return Err(BlockError::Encode("empty block"));
    }
    if events.len() > MAX_BLOCK_EVENTS {
        return Err(BlockError::Encode("too many events for one block"));
    }
    if events.iter().any(|e| e.rsvd != 0) {
        return Err(BlockError::Encode("rsvd byte must be 0"));
    }

    let mut body = Vec::with_capacity(events.len() * 12);
    encode_dod_u64(events.iter().map(|e| e.recv_mono_ns), &mut body);
    encode_dod_u64(events.iter().map(|e| e.recv_wall_ns), &mut body);
    encode_delta_u64(events.iter().map(|e| e.venue_ts_ns), &mut body);
    encode_delta_u64(events.iter().map(|e| e.venue_seq), &mut body);
    encode_delta_i64(events.iter().map(|e| e.price), &mut body);
    encode_zz_i64(events.iter().map(|e| e.qty), &mut body);
    encode_delta_u64(events.iter().map(|e| e.aux), &mut body);
    encode_v_u32(events.iter().map(|e| e.instrument), &mut body);
    body.extend(events.iter().map(|e| e.kind));
    body.extend(events.iter().map(|e| e.venue));
    body.extend(events.iter().map(|e| e.flags));

    let raw_len = u32::try_from(body.len()).map_err(|_| BlockError::Encode("body too large"))?;
    let (stored, zstd_used) = match zstd_level {
        Some(level) => {
            let c = zstd::bulk::compress(&body, level)
                .map_err(|_| BlockError::Encode("zstd compress failed"))?;
            // keep the smaller representation; tiny blocks can inflate
            if c.len() < body.len() {
                (c, true)
            } else {
                (body, false)
            }
        }
        None => (body, false),
    };
    let body_len = u32::try_from(stored.len()).map_err(|_| BlockError::Encode("body too large"))?;

    let (mut min_m, mut max_m) = (u64::MAX, 0u64);
    let (mut min_w, mut max_w) = (u64::MAX, 0u64);
    for e in events {
        min_m = min_m.min(e.recv_mono_ns);
        max_m = max_m.max(e.recv_mono_ns);
        min_w = min_w.min(e.recv_wall_ns);
        max_w = max_w.max(e.recv_wall_ns);
    }

    let crc = crc32fast::hash(&stored);
    let start = out.len();
    out.extend_from_slice(&BLOCK_MAGIC.to_le_bytes());
    out.push(BLOCK_VERSION);
    out.push(if zstd_used { FLAG_ZSTD } else { 0 });
    out.extend_from_slice(&0u16.to_le_bytes());
    #[allow(clippy::cast_possible_truncation)]
    out.extend_from_slice(&(events.len() as u32).to_le_bytes());
    out.extend_from_slice(&body_len.to_le_bytes());
    out.extend_from_slice(&raw_len.to_le_bytes());
    out.extend_from_slice(&min_m.to_le_bytes());
    out.extend_from_slice(&max_m.to_le_bytes());
    out.extend_from_slice(&min_w.to_le_bytes());
    out.extend_from_slice(&max_w.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&stored);
    Ok(out.len() - start)
}

/// Parse a header from the front of `b`. Distinguishes "no more blocks"
/// (clean EOF: empty or all-zero remainder shorter than a header) from a
/// torn/corrupt header.
pub fn parse_header(b: &[u8]) -> Result<Option<BlockHeader>, BlockError> {
    if b.is_empty() {
        return Ok(None);
    }
    if b.len() < HEADER_LEN {
        return Err(BlockError::Torn("short header"));
    }
    let magic = u32::from_le_bytes(b[0..4].try_into().unwrap());
    if magic != BLOCK_MAGIC {
        return Err(BlockError::BadHeader("bad magic"));
    }
    if b[4] != BLOCK_VERSION {
        return Err(BlockError::BadHeader("unsupported version"));
    }
    let flags = b[5];
    let n_events = u32::from_le_bytes(b[8..12].try_into().unwrap());
    let body_len = u32::from_le_bytes(b[12..16].try_into().unwrap());
    let raw_len = u32::from_le_bytes(b[16..20].try_into().unwrap());
    if n_events == 0 || n_events as usize > MAX_BLOCK_EVENTS {
        return Err(BlockError::BadHeader("implausible event count"));
    }
    if body_len as usize > MAX_BODY_LEN || raw_len as usize > MAX_BODY_LEN {
        return Err(BlockError::BadHeader("implausible body length"));
    }
    Ok(Some(BlockHeader {
        n_events,
        body_len,
        raw_len,
        zstd: flags & FLAG_ZSTD != 0,
        min_recv_mono: u64::from_le_bytes(b[20..28].try_into().unwrap()),
        max_recv_mono: u64::from_le_bytes(b[28..36].try_into().unwrap()),
        min_recv_wall: u64::from_le_bytes(b[36..44].try_into().unwrap()),
        max_recv_wall: u64::from_le_bytes(b[44..52].try_into().unwrap()),
        crc: u32::from_le_bytes(b[52..56].try_into().unwrap()),
    }))
}

/// Decode the block at the front of `b` into `out` (appended). Returns the
/// header and the total bytes consumed.
pub fn decode_block(b: &[u8], out: &mut Vec<Event>) -> Result<(BlockHeader, usize), BlockError> {
    let header = parse_header(b)?.ok_or(BlockError::Torn("empty"))?;
    let total = header.total_len();
    if b.len() < total {
        return Err(BlockError::Torn("body truncated"));
    }
    let stored = &b[HEADER_LEN..total];
    if crc32fast::hash(stored) != header.crc {
        return Err(BlockError::Torn("crc mismatch"));
    }
    let decompressed;
    let body: &[u8] = if header.zstd {
        decompressed = zstd::bulk::decompress(stored, header.raw_len as usize)
            .map_err(|_| BlockError::Corrupt("zstd decompress failed"))?;
        &decompressed
    } else {
        stored
    };
    if body.len() != header.raw_len as usize {
        return Err(BlockError::Corrupt("raw length mismatch"));
    }

    let n = header.n_events as usize;
    let mut pos = 0usize;
    let mut mono = Vec::with_capacity(n);
    let mut wall = Vec::with_capacity(n);
    let mut vts = Vec::with_capacity(n);
    let mut vseq = Vec::with_capacity(n);
    let mut price = Vec::with_capacity(n);
    let mut qty = Vec::with_capacity(n);
    let mut aux = Vec::with_capacity(n);
    let mut inst = Vec::with_capacity(n);
    decode_dod_u64(body, &mut pos, n, &mut mono).ok_or(BlockError::Corrupt("mono column"))?;
    decode_dod_u64(body, &mut pos, n, &mut wall).ok_or(BlockError::Corrupt("wall column"))?;
    decode_delta_u64(body, &mut pos, n, &mut vts).ok_or(BlockError::Corrupt("venue_ts column"))?;
    decode_delta_u64(body, &mut pos, n, &mut vseq)
        .ok_or(BlockError::Corrupt("venue_seq column"))?;
    decode_delta_i64(body, &mut pos, n, &mut price).ok_or(BlockError::Corrupt("price column"))?;
    decode_zz_i64(body, &mut pos, n, &mut qty).ok_or(BlockError::Corrupt("qty column"))?;
    decode_delta_u64(body, &mut pos, n, &mut aux).ok_or(BlockError::Corrupt("aux column"))?;
    decode_v_u32(body, &mut pos, n, &mut inst).ok_or(BlockError::Corrupt("instrument column"))?;
    if body.len() - pos != 3 * n {
        return Err(BlockError::Corrupt("byte columns length"));
    }
    let kinds = &body[pos..pos + n];
    let venues = &body[pos + n..pos + 2 * n];
    let flags = &body[pos + 2 * n..pos + 3 * n];

    out.reserve(n);
    for i in 0..n {
        out.push(Event {
            recv_mono_ns: mono[i],
            recv_wall_ns: wall[i],
            venue_ts_ns: vts[i],
            venue_seq: vseq[i],
            price: price[i],
            qty: qty[i],
            aux: aux[i],
            instrument: inst[i],
            kind: kinds[i],
            venue: venues[i],
            flags: flags[i],
            rsvd: 0,
        });
    }
    Ok((header, total))
}

/// Encoded-size accounting for compression reporting: (events, raw event
/// bytes at 64 B each, stored bytes).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SizeReport {
    /// Number of events.
    pub events: u64,
    /// events * 64 (in-memory footprint).
    pub event_bytes: u64,
    /// Bytes on disk (headers + stored bodies).
    pub stored_bytes: u64,
}

impl SizeReport {
    /// Fold in one block.
    pub fn add_block(&mut self, h: &BlockHeader) {
        self.events += u64::from(h.n_events);
        self.event_bytes += u64::from(h.n_events) * EVENT_SIZE as u64;
        self.stored_bytes += h.total_len() as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn mk_events(n: usize, seed: u64) -> Vec<Event> {
        // realistic-ish: monotone timestamps, clustered prices, small qtys
        let mut evs = Vec::with_capacity(n);
        let mut t = 1_700_000_000_000_000_000u64;
        let mut price = 6_358_964_000_000i64;
        for i in 0..n {
            let x = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(i as u64);
            t += 1_000_000 + (x % 500_000);
            price += ((x >> 8) % 2001) as i64 - 1000;
            evs.push(Event {
                recv_mono_ns: t - 1_600_000_000_000_000_000,
                recv_wall_ns: t,
                venue_ts_ns: t - 3_000_000,
                venue_seq: 1000 + i as u64,
                price,
                qty: ((x >> 16) % 1_000_000) as i64,
                aux: 500_000 + i as u64,
                instrument: (x % 15) as u32 + 1,
                kind: (x % 11) as u8 + 1,
                venue: (x % 3) as u8 + 1,
                flags: (x % 8) as u8,
                rsvd: 0,
            });
        }
        evs
    }

    #[test]
    fn roundtrip_raw_and_zstd() {
        let evs = mk_events(4096, 42);
        for level in [None, Some(3)] {
            let mut buf = Vec::new();
            let stored = encode_block(&evs, level, &mut buf).unwrap();
            assert_eq!(stored, buf.len());
            let mut back = Vec::new();
            let (h, consumed) = decode_block(&buf, &mut back).unwrap();
            assert_eq!(consumed, buf.len());
            assert_eq!(h.n_events as usize, evs.len());
            assert_eq!(back, evs, "level {level:?}");
            assert_eq!(
                h.min_recv_mono,
                evs.iter().map(|e| e.recv_mono_ns).min().unwrap()
            );
            assert_eq!(
                h.max_recv_wall,
                evs.iter().map(|e| e.recv_wall_ns).max().unwrap()
            );
        }
    }

    #[test]
    fn compression_beats_raw_events_substantially() {
        // sanity floor, not a benchmark: columnar varints on realistic data
        // must be far below 64 B/event
        let evs = mk_events(4096, 7);
        let mut buf = Vec::new();
        encode_block(&evs, None, &mut buf).unwrap();
        let per_event = buf.len() as f64 / evs.len() as f64;
        assert!(per_event < 32.0, "raw columnar {per_event:.1} B/event");
        let mut zbuf = Vec::new();
        encode_block(&evs, Some(3), &mut zbuf).unwrap();
        assert!(zbuf.len() <= buf.len());
    }

    #[test]
    fn rejects_bad_input() {
        let mut buf = Vec::new();
        assert!(matches!(
            encode_block(&[], None, &mut buf),
            Err(BlockError::Encode(_))
        ));
        let mut e = mk_events(1, 1);
        e[0].rsvd = 7;
        assert!(matches!(
            encode_block(&e, None, &mut buf),
            Err(BlockError::Encode(_))
        ));
    }

    #[test]
    fn detects_torn_and_corrupt() {
        let evs = mk_events(256, 9);
        let mut buf = Vec::new();
        encode_block(&evs, Some(3), &mut buf).unwrap();

        // torn: any strict prefix fails with Torn (or short-header)
        for cut in [1, HEADER_LEN - 1, HEADER_LEN, HEADER_LEN + 5, buf.len() - 1] {
            let mut out = Vec::new();
            let r = decode_block(&buf[..cut], &mut out);
            assert!(matches!(r, Err(BlockError::Torn(_))), "cut {cut}: {r:?}");
        }
        // clean EOF: empty slice parses as None
        assert!(parse_header(&[]).unwrap().is_none());

        // corrupt body byte -> crc mismatch
        let mut bad = buf.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        let mut out = Vec::new();
        assert!(matches!(
            decode_block(&bad, &mut out),
            Err(BlockError::Torn(_))
        ));

        // corrupt magic -> BadHeader
        let mut bad = buf.clone();
        bad[0] ^= 0xff;
        assert!(matches!(
            decode_block(&bad, &mut out),
            Err(BlockError::BadHeader(_))
        ));
    }

    #[test]
    fn multiple_blocks_stream() {
        let a = mk_events(100, 1);
        let b = mk_events(50, 2);
        let mut buf = Vec::new();
        encode_block(&a, Some(3), &mut buf).unwrap();
        encode_block(&b, None, &mut buf).unwrap();
        let mut out = Vec::new();
        let (h1, used1) = decode_block(&buf, &mut out).unwrap();
        let (h2, used2) = decode_block(&buf[used1..], &mut out).unwrap();
        assert_eq!(used1 + used2, buf.len());
        assert_eq!(h1.n_events, 100);
        assert_eq!(h2.n_events, 50);
        assert_eq!(&out[..100], &a[..]);
        assert_eq!(&out[100..], &b[..]);
    }

    proptest! {
        #[test]
        fn roundtrip_arbitrary_events(seed in any::<u64>(), n in 1usize..600, z in any::<bool>()) {
            let mut evs = mk_events(n, seed);
            // scatter in fully-random field values to break the "realistic"
            // shapes (encoders must be correct for ANY input, not just
            // compressible input)
            for (i, e) in evs.iter_mut().enumerate() {
                if i % 3 == 0 {
                    let x = seed.wrapping_mul(i as u64 + 1);
                    e.recv_mono_ns = x;
                    e.recv_wall_ns = x.wrapping_mul(31);
                    e.venue_ts_ns = x.wrapping_mul(17);
                    e.venue_seq = x.wrapping_mul(13);
                    #[allow(clippy::cast_possible_wrap)]
                    { e.price = x.wrapping_mul(7) as i64; }
                    #[allow(clippy::cast_possible_wrap)]
                    { e.qty = x.wrapping_mul(3) as i64; }
                    e.aux = x;
                }
            }
            let mut buf = Vec::new();
            encode_block(&evs, if z { Some(1) } else { None }, &mut buf).unwrap();
            let mut back = Vec::new();
            let (_, used) = decode_block(&buf, &mut back).unwrap();
            prop_assert_eq!(used, buf.len());
            prop_assert_eq!(back, evs);
        }

        #[test]
        fn decode_never_panics_on_garbage(bytes in proptest::collection::vec(any::<u8>(), 0..2000)) {
            let mut out = Vec::new();
            let _ = decode_block(&bytes, &mut out);
            let _ = parse_header(&bytes);
        }
    }
}
