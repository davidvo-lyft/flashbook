//! Block format: the unit of storage, compression, indexing and recovery.
//!
//! A block encodes up to [`MAX_BLOCK_EVENTS`] events column-wise (one stream
//! per Event field, each with the encoding that fits its shape — see
//! [`crate::varint`]), optionally zstd-compresses the concatenated column
//! bytes, and self-delimits with a fixed header + CRC32 so files recover
//! from torn writes by truncating at the last valid block.
//!
//! Layout (little-endian; the writer emits version 2, the reader accepts
//! both versions):
//!
//! ```text
//! u32 magic 0xFB5B_10C4 | u8 version (1 or 2) | u8 flags (bit0 = zstd)
//! u16 reserved | u32 n_events | u32 body_len (as stored)
//! u32 raw_len (pre-compression) | u64 min_recv_mono | u64 max_recv_mono
//! u64 min_recv_wall | u64 max_recv_wall | u32 crc32(body as stored)
//! [v2 only] 11 x u32 column byte-lengths, in body column order
//! body bytes
//! ```
//!
//! Column order in the body: recv_mono (DoD), recv_wall (DoD), venue_ts
//! (delta), venue_seq (delta), price (delta zz), qty (zz), aux (delta),
//! instrument (varint), then kind/venue/flags as raw byte runs. `rsvd` is
//! not stored (asserted 0, reconstructed as 0) — roundtrip is byte-exact
//! for every event the writer accepts.
//!
//! **v2 column-offset table.** v1 bodies gave no way to find a column
//! without varint-decoding everything before it, so even a
//! four-column aggregate paid the full 11-column decode. The v2 header
//! records each column stream's byte length; [`decode_block_columns`]
//! uses the table to decode only the columns a [`ColumnSelection`] asks
//! for. Pruning always saves *decode* work; for zstd blocks the stored
//! body is still decompressed once in full (zstd is per-block and not
//! seekable), then sliced. v1 blocks keep working through a decode-all
//! fallback (correct, no pruning speedup — see [`decode_block_columns`]).
//!
//! **Integrity model (unchanged from v1).** The CRC32 covers the stored
//! body only; header fields — including the v2 length table, like v1's
//! min/max ranges — are protected by sanity checks, not a checksum. A
//! truncated or implausible header fails [`parse_header`] and is handled
//! as a torn tail by the segment recovery scan; a v2 length table whose
//! entries don't sum to `raw_len` (or whose byte-column entries aren't
//! `n_events`) is rejected as corrupt at parse time, and every per-column
//! decode additionally requires its stream to consume its recorded length
//! exactly.

use flashbook_proto::event::{EVENT_SIZE, Event};

use crate::varint::{
    decode_delta_i64, decode_delta_u64, decode_dod_u64, decode_v_u32, decode_zz_i64,
    encode_delta_i64, encode_delta_u64, encode_dod_u64, encode_v_u32, encode_zz_i64,
};

/// Block magic.
pub const BLOCK_MAGIC: u32 = 0xFB5B_10C4;
/// Format version written by [`encode_block`].
pub const BLOCK_VERSION: u8 = 2;
/// Legacy format version (no column-offset table); still fully readable.
pub const BLOCK_VERSION_V1: u8 = 1;
/// Number of column streams in a block body.
pub const N_COLUMNS: usize = 11;
/// v1 header size on disk (fixed fields only).
pub const HEADER_LEN_V1: usize = 4 + 1 + 1 + 2 + 4 + 4 + 4 + 8 + 8 + 8 + 8 + 4;
/// v2 header size on disk (fixed fields + the column-offset table).
pub const HEADER_LEN_V2: usize = HEADER_LEN_V1 + N_COLUMNS * 4;
/// Header size written by [`encode_block`] (current version, i.e. v2).
pub const HEADER_LEN: usize = HEADER_LEN_V2;
/// Hard cap on events per block (format limit; writers default lower).
pub const MAX_BLOCK_EVENTS: usize = 65_536;
/// Sanity cap on a stored body (fits any 65536-event block comfortably).
pub const MAX_BODY_LEN: usize = 64 * 1024 * 1024;

/// Flags bit: body is zstd-compressed.
pub const FLAG_ZSTD: u8 = 1;

/// Header size for a given block version (1 or 2).
#[inline]
pub const fn header_len(version: u8) -> usize {
    if version == BLOCK_VERSION_V1 {
        HEADER_LEN_V1
    } else {
        HEADER_LEN_V2
    }
}

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
    /// Format version this block was written with (1 or 2).
    pub version: u8,
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
    /// v2 column-offset table: byte length of each column stream in body
    /// order (validated to sum to `raw_len`). All zeros for v1 blocks,
    /// which have no table.
    pub col_lens: [u32; N_COLUMNS],
}

impl BlockHeader {
    /// On-disk header size for this block's version.
    pub fn header_len(&self) -> usize {
        header_len(self.version)
    }

    /// Total on-disk size of the block (header + body).
    pub fn total_len(&self) -> usize {
        self.header_len() + self.body_len as usize
    }
}

/// Which columns [`decode_block_columns`] should materialize. Fields are in
/// body column order; construct with struct-update syntax, e.g.
/// `ColumnSelection { qty: true, instrument: true, ..ColumnSelection::NONE }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ColumnSelection {
    /// Decode `recv_mono_ns`.
    pub recv_mono: bool,
    /// Decode `recv_wall_ns`.
    pub recv_wall: bool,
    /// Decode `venue_ts_ns`.
    pub venue_ts: bool,
    /// Decode `venue_seq`.
    pub venue_seq: bool,
    /// Decode `price`.
    pub price: bool,
    /// Decode `qty`.
    pub qty: bool,
    /// Decode `aux`.
    pub aux: bool,
    /// Decode `instrument`.
    pub instrument: bool,
    /// Decode `kind`.
    pub kind: bool,
    /// Decode `venue`.
    pub venue: bool,
    /// Decode `flags`.
    pub flags: bool,
}

impl ColumnSelection {
    /// No columns selected.
    pub const NONE: Self = Self {
        recv_mono: false,
        recv_wall: false,
        venue_ts: false,
        venue_seq: false,
        price: false,
        qty: false,
        aux: false,
        instrument: false,
        kind: false,
        venue: false,
        flags: false,
    };

    /// Every column selected.
    pub const ALL: Self = Self {
        recv_mono: true,
        recv_wall: true,
        venue_ts: true,
        venue_seq: true,
        price: true,
        qty: true,
        aux: true,
        instrument: true,
        kind: true,
        venue: true,
        flags: true,
    };
}

/// Decoded column vectors for one block: `Some` (with exactly the block's
/// `n_events` values) for every selected column, `None` for the rest.
/// Reused across [`decode_block_columns`] calls — on v2 blocks, existing
/// vectors for still-selected columns keep their allocations (the v1
/// fallback replaces them). On error the contents are unspecified.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ColumnData {
    /// `recv_mono_ns` values.
    pub recv_mono: Option<Vec<u64>>,
    /// `recv_wall_ns` values.
    pub recv_wall: Option<Vec<u64>>,
    /// `venue_ts_ns` values.
    pub venue_ts: Option<Vec<u64>>,
    /// `venue_seq` values.
    pub venue_seq: Option<Vec<u64>>,
    /// `price` values.
    pub price: Option<Vec<i64>>,
    /// `qty` values.
    pub qty: Option<Vec<i64>>,
    /// `aux` values.
    pub aux: Option<Vec<u64>>,
    /// `instrument` values.
    pub instrument: Option<Vec<u32>>,
    /// `kind` bytes.
    pub kind: Option<Vec<u8>>,
    /// `venue` bytes.
    pub venue: Option<Vec<u8>>,
    /// `flags` bytes.
    pub flags: Option<Vec<u8>>,
}

/// Encode `events` into `out` (appended) as a v2 block. `zstd_level`:
/// None = store raw, Some(level) = compress the column body. Returns the
/// stored size.
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
    let mut col_lens = [0u32; N_COLUMNS];
    let mut mark = 0usize;
    let mut cut = |body: &[u8], idx: usize| -> Result<(), BlockError> {
        col_lens[idx] =
            u32::try_from(body.len() - mark).map_err(|_| BlockError::Encode("body too large"))?;
        mark = body.len();
        Ok(())
    };
    encode_dod_u64(events.iter().map(|e| e.recv_mono_ns), &mut body);
    cut(&body, 0)?;
    encode_dod_u64(events.iter().map(|e| e.recv_wall_ns), &mut body);
    cut(&body, 1)?;
    encode_delta_u64(events.iter().map(|e| e.venue_ts_ns), &mut body);
    cut(&body, 2)?;
    encode_delta_u64(events.iter().map(|e| e.venue_seq), &mut body);
    cut(&body, 3)?;
    encode_delta_i64(events.iter().map(|e| e.price), &mut body);
    cut(&body, 4)?;
    encode_zz_i64(events.iter().map(|e| e.qty), &mut body);
    cut(&body, 5)?;
    encode_delta_u64(events.iter().map(|e| e.aux), &mut body);
    cut(&body, 6)?;
    encode_v_u32(events.iter().map(|e| e.instrument), &mut body);
    cut(&body, 7)?;
    body.extend(events.iter().map(|e| e.kind));
    cut(&body, 8)?;
    body.extend(events.iter().map(|e| e.venue));
    cut(&body, 9)?;
    body.extend(events.iter().map(|e| e.flags));
    cut(&body, 10)?;

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
    for len in col_lens {
        out.extend_from_slice(&len.to_le_bytes());
    }
    out.extend_from_slice(&stored);
    Ok(out.len() - start)
}

/// Parse a header from the front of `b`. Distinguishes "no more blocks"
/// (clean EOF: empty remainder) from a torn/corrupt header. Accepts both
/// block versions; the returned header says which ([`BlockHeader::version`]).
///
/// v2 column-offset table validation (the header has no CRC — see the
/// module notes): the 11 lengths must sum to `raw_len` and the three byte
/// columns (kind/venue/flags) must each be exactly `n_events` long;
/// anything else is [`BlockError::Corrupt`], which the segment recovery
/// scan treats as a torn tail.
pub fn parse_header(b: &[u8]) -> Result<Option<BlockHeader>, BlockError> {
    if b.is_empty() {
        return Ok(None);
    }
    if b.len() < HEADER_LEN_V1 {
        return Err(BlockError::Torn("short header"));
    }
    let magic = u32::from_le_bytes(b[0..4].try_into().unwrap());
    if magic != BLOCK_MAGIC {
        return Err(BlockError::BadHeader("bad magic"));
    }
    let version = b[4];
    if version != BLOCK_VERSION_V1 && version != BLOCK_VERSION {
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
    let mut col_lens = [0u32; N_COLUMNS];
    if version != BLOCK_VERSION_V1 {
        if b.len() < HEADER_LEN_V2 {
            return Err(BlockError::Torn("short header"));
        }
        let mut sum = 0u64;
        for (i, len) in col_lens.iter_mut().enumerate() {
            let at = HEADER_LEN_V1 + i * 4;
            *len = u32::from_le_bytes(b[at..at + 4].try_into().unwrap());
            sum += u64::from(*len);
        }
        if sum != u64::from(raw_len) {
            return Err(BlockError::Corrupt("column lengths do not sum to raw_len"));
        }
        if col_lens[8..].iter().any(|&l| l != n_events) {
            return Err(BlockError::Corrupt("byte column length != n_events"));
        }
    }
    Ok(Some(BlockHeader {
        version,
        n_events,
        body_len,
        raw_len,
        zstd: flags & FLAG_ZSTD != 0,
        min_recv_mono: u64::from_le_bytes(b[20..28].try_into().unwrap()),
        max_recv_mono: u64::from_le_bytes(b[28..36].try_into().unwrap()),
        min_recv_wall: u64::from_le_bytes(b[36..44].try_into().unwrap()),
        max_recv_wall: u64::from_le_bytes(b[44..52].try_into().unwrap()),
        crc: u32::from_le_bytes(b[52..56].try_into().unwrap()),
        col_lens,
    }))
}

/// The (possibly decompressed) block body: borrowed straight from the
/// input for raw blocks, owned for zstd blocks.
enum Body<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl Body<'_> {
    fn bytes(&self) -> &[u8] {
        match self {
            Body::Borrowed(b) => b,
            Body::Owned(v) => v,
        }
    }
}

/// Bounds-check, CRC-check and (for zstd blocks) decompress the body of
/// the block at the front of `b`. Returns the body and the total on-disk
/// bytes consumed. Single source of truth for every decode path.
fn read_body<'a>(b: &'a [u8], header: &BlockHeader) -> Result<(Body<'a>, usize), BlockError> {
    let total = header.total_len();
    if b.len() < total {
        return Err(BlockError::Torn("body truncated"));
    }
    let stored = &b[header.header_len()..total];
    if crc32fast::hash(stored) != header.crc {
        return Err(BlockError::Torn("crc mismatch"));
    }
    let body = if header.zstd {
        let d = zstd::bulk::decompress(stored, header.raw_len as usize)
            .map_err(|_| BlockError::Corrupt("zstd decompress failed"))?;
        Body::Owned(d)
    } else {
        Body::Borrowed(stored)
    };
    if body.bytes().len() != header.raw_len as usize {
        return Err(BlockError::Corrupt("raw length mismatch"));
    }
    Ok((body, total))
}

/// The 8 varint-encoded columns of one body, decoded. The 3 trailing byte
/// columns are cheap slices of the body and are taken separately.
struct VarintColumns {
    mono: Vec<u64>,
    wall: Vec<u64>,
    vts: Vec<u64>,
    vseq: Vec<u64>,
    price: Vec<i64>,
    qty: Vec<i64>,
    aux: Vec<u64>,
    inst: Vec<u32>,
}

/// Decode all 8 varint columns of `body` sequentially (v1 layout: no
/// offset table, each column starts where the previous ended) and locate
/// the 3 byte columns. Returns the columns and the byte-column start.
fn decode_varint_columns_sequential(
    body: &[u8],
    n: usize,
) -> Result<(VarintColumns, usize), BlockError> {
    let mut pos = 0usize;
    let mut c = VarintColumns {
        mono: Vec::with_capacity(n),
        wall: Vec::with_capacity(n),
        vts: Vec::with_capacity(n),
        vseq: Vec::with_capacity(n),
        price: Vec::with_capacity(n),
        qty: Vec::with_capacity(n),
        aux: Vec::with_capacity(n),
        inst: Vec::with_capacity(n),
    };
    decode_dod_u64(body, &mut pos, n, &mut c.mono).ok_or(BlockError::Corrupt("mono column"))?;
    decode_dod_u64(body, &mut pos, n, &mut c.wall).ok_or(BlockError::Corrupt("wall column"))?;
    decode_delta_u64(body, &mut pos, n, &mut c.vts)
        .ok_or(BlockError::Corrupt("venue_ts column"))?;
    decode_delta_u64(body, &mut pos, n, &mut c.vseq)
        .ok_or(BlockError::Corrupt("venue_seq column"))?;
    decode_delta_i64(body, &mut pos, n, &mut c.price).ok_or(BlockError::Corrupt("price column"))?;
    decode_zz_i64(body, &mut pos, n, &mut c.qty).ok_or(BlockError::Corrupt("qty column"))?;
    decode_delta_u64(body, &mut pos, n, &mut c.aux).ok_or(BlockError::Corrupt("aux column"))?;
    decode_v_u32(body, &mut pos, n, &mut c.inst).ok_or(BlockError::Corrupt("instrument column"))?;
    if body.len() - pos != 3 * n {
        return Err(BlockError::Corrupt("byte columns length"));
    }
    Ok((c, pos))
}

/// Column-stream start offsets from a v2 header's length table:
/// `offs[i]..offs[i+1]` is column `i`'s slice of the body.
fn col_offsets(header: &BlockHeader) -> [usize; N_COLUMNS + 1] {
    let mut offs = [0usize; N_COLUMNS + 1];
    for i in 0..N_COLUMNS {
        offs[i + 1] = offs[i] + header.col_lens[i] as usize;
    }
    offs
}

/// Decode the block at the front of `b` into `out` (appended). Returns the
/// header and the total bytes consumed. Handles both block versions; for
/// v2 the column-offset table is cross-checked (every stream must decode
/// exactly its recorded length).
pub fn decode_block(b: &[u8], out: &mut Vec<Event>) -> Result<(BlockHeader, usize), BlockError> {
    let header = parse_header(b)?.ok_or(BlockError::Torn("empty"))?;
    let (body, total) = read_body(b, &header)?;
    let body = body.bytes();
    let n = header.n_events as usize;

    let (c, byte_start) = if header.version == BLOCK_VERSION_V1 {
        decode_varint_columns_sequential(body, n)?
    } else {
        let offs = col_offsets(&header);
        let c = VarintColumns {
            mono: decode_col(body, &offs, 0, n, decode_dod_u64, "mono column")?,
            wall: decode_col(body, &offs, 1, n, decode_dod_u64, "wall column")?,
            vts: decode_col(body, &offs, 2, n, decode_delta_u64, "venue_ts column")?,
            vseq: decode_col(body, &offs, 3, n, decode_delta_u64, "venue_seq column")?,
            price: decode_col(body, &offs, 4, n, decode_delta_i64, "price column")?,
            qty: decode_col(body, &offs, 5, n, decode_zz_i64, "qty column")?,
            aux: decode_col(body, &offs, 6, n, decode_delta_u64, "aux column")?,
            inst: decode_col(body, &offs, 7, n, decode_v_u32, "instrument column")?,
        };
        (c, offs[8])
    };
    let kinds = &body[byte_start..byte_start + n];
    let venues = &body[byte_start + n..byte_start + 2 * n];
    let flags = &body[byte_start + 2 * n..byte_start + 3 * n];

    out.reserve(n);
    for i in 0..n {
        out.push(Event {
            recv_mono_ns: c.mono[i],
            recv_wall_ns: c.wall[i],
            venue_ts_ns: c.vts[i],
            venue_seq: c.vseq[i],
            price: c.price[i],
            qty: c.qty[i],
            aux: c.aux[i],
            instrument: c.inst[i],
            kind: kinds[i],
            venue: venues[i],
            flags: flags[i],
            rsvd: 0,
        });
    }
    Ok((header, total))
}

/// Decode one varint column from its v2 offset-table slice into a fresh
/// vector: exactly `n` values consuming exactly the recorded length.
fn decode_col<T>(
    body: &[u8],
    offs: &[usize; N_COLUMNS + 1],
    idx: usize,
    n: usize,
    dec: impl Fn(&[u8], &mut usize, usize, &mut Vec<T>) -> Option<()>,
    what: &'static str,
) -> Result<Vec<T>, BlockError> {
    let mut out = Vec::with_capacity(n);
    decode_col_into(body, offs, idx, n, dec, what, &mut out)?;
    Ok(out)
}

/// [`decode_col`] into a caller-provided (cleared) vector.
fn decode_col_into<T>(
    body: &[u8],
    offs: &[usize; N_COLUMNS + 1],
    idx: usize,
    n: usize,
    dec: impl Fn(&[u8], &mut usize, usize, &mut Vec<T>) -> Option<()>,
    what: &'static str,
    out: &mut Vec<T>,
) -> Result<(), BlockError> {
    out.clear();
    out.reserve(n);
    let stream = &body[offs[idx]..offs[idx + 1]];
    let mut pos = 0usize;
    dec(stream, &mut pos, n, out).ok_or(BlockError::Corrupt(what))?;
    if pos != stream.len() {
        return Err(BlockError::Corrupt(what));
    }
    Ok(())
}

/// Take a selected column's reusable buffer out of `slot` (allocation
/// preserved across blocks), cleared.
fn take_buf<T>(slot: &mut Option<Vec<T>>) -> Vec<T> {
    let mut v = slot.take().unwrap_or_default();
    v.clear();
    v
}

/// Decode only the columns selected by `sel` from the block at the front
/// of `b` into `out` (see [`ColumnData`] for the fill contract). Returns
/// the header and the total bytes consumed. The body CRC is always
/// verified, whatever the selection.
///
/// - **v2 blocks:** unselected columns are skipped entirely via the
///   header's column-offset table — pruning saves all of their varint
///   decode work. zstd bodies are decompressed once in full first (zstd
///   is per-block, not seekable), so on compressed stores pruning saves
///   the decode stage only; raw stores skip the unselected bytes outright.
/// - **v1 blocks (no offset table):** falls back to decoding every column
///   and keeping the selected ones — correct, but no faster than
///   [`decode_block`]. Re-ingesting with the current writer produces v2.
pub fn decode_block_columns(
    b: &[u8],
    sel: ColumnSelection,
    out: &mut ColumnData,
) -> Result<(BlockHeader, usize), BlockError> {
    let header = parse_header(b)?.ok_or(BlockError::Torn("empty"))?;
    let (body, total) = read_body(b, &header)?;
    let body = body.bytes();
    let n = header.n_events as usize;

    if header.version == BLOCK_VERSION_V1 {
        // Decode-all fallback: without an offset table every varint column
        // must be walked to find the next one's start anyway.
        let (c, byte_start) = decode_varint_columns_sequential(body, n)?;
        out.recv_mono = sel.recv_mono.then_some(c.mono);
        out.recv_wall = sel.recv_wall.then_some(c.wall);
        out.venue_ts = sel.venue_ts.then_some(c.vts);
        out.venue_seq = sel.venue_seq.then_some(c.vseq);
        out.price = sel.price.then_some(c.price);
        out.qty = sel.qty.then_some(c.qty);
        out.aux = sel.aux.then_some(c.aux);
        out.instrument = sel.instrument.then_some(c.inst);
        copy_byte_col(&mut out.kind, sel.kind, &body[byte_start..byte_start + n]);
        copy_byte_col(
            &mut out.venue,
            sel.venue,
            &body[byte_start + n..byte_start + 2 * n],
        );
        copy_byte_col(
            &mut out.flags,
            sel.flags,
            &body[byte_start + 2 * n..byte_start + 3 * n],
        );
        return Ok((header, total));
    }

    let offs = col_offsets(&header);
    macro_rules! varint_col {
        ($field:ident, $idx:expr, $dec:ident, $what:literal) => {
            if sel.$field {
                let mut v = take_buf(&mut out.$field);
                decode_col_into(body, &offs, $idx, n, $dec, $what, &mut v)?;
                out.$field = Some(v);
            } else {
                out.$field = None;
            }
        };
    }
    varint_col!(recv_mono, 0, decode_dod_u64, "mono column");
    varint_col!(recv_wall, 1, decode_dod_u64, "wall column");
    varint_col!(venue_ts, 2, decode_delta_u64, "venue_ts column");
    varint_col!(venue_seq, 3, decode_delta_u64, "venue_seq column");
    varint_col!(price, 4, decode_delta_i64, "price column");
    varint_col!(qty, 5, decode_zz_i64, "qty column");
    varint_col!(aux, 6, decode_delta_u64, "aux column");
    varint_col!(instrument, 7, decode_v_u32, "instrument column");
    // Byte columns: parse_header validated their lengths == n_events.
    copy_byte_col(&mut out.kind, sel.kind, &body[offs[8]..offs[9]]);
    copy_byte_col(&mut out.venue, sel.venue, &body[offs[9]..offs[10]]);
    copy_byte_col(&mut out.flags, sel.flags, &body[offs[10]..offs[11]]);
    Ok((header, total))
}

/// Fill one byte column from its body slice when selected, else `None`.
fn copy_byte_col(slot: &mut Option<Vec<u8>>, on: bool, src: &[u8]) {
    if on {
        let mut v = take_buf(slot);
        v.extend_from_slice(src);
        *slot = Some(v);
    } else {
        *slot = None;
    }
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
            assert_eq!(h.version, BLOCK_VERSION);
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
    fn v2_header_layout_and_offset_table() {
        assert_eq!(header_len(BLOCK_VERSION_V1), 56);
        assert_eq!(header_len(BLOCK_VERSION), 56 + 44);
        let evs = mk_events(512, 3);
        let mut buf = Vec::new();
        encode_block(&evs, None, &mut buf).unwrap();
        assert_eq!(buf[4], BLOCK_VERSION, "version byte");
        let h = parse_header(&buf).unwrap().unwrap();
        assert_eq!(h.header_len(), HEADER_LEN_V2);
        assert_eq!(h.total_len(), buf.len());
        // the table sums to raw_len and byte columns are n each
        let sum: u64 = h.col_lens.iter().map(|&l| u64::from(l)).sum();
        assert_eq!(sum, u64::from(h.raw_len));
        assert_eq!(&h.col_lens[8..], &[512u32; 3], "byte columns are n each");
        // lens match the on-disk table bytes at HEADER_LEN_V1..HEADER_LEN_V2
        for (i, &len) in h.col_lens.iter().enumerate() {
            let at = HEADER_LEN_V1 + i * 4;
            assert_eq!(u32::from_le_bytes(buf[at..at + 4].try_into().unwrap()), len);
        }
    }

    #[test]
    fn pruned_decode_reuses_buffers_and_nones_unselected() {
        let evs = mk_events(300, 5);
        let mut buf = Vec::new();
        encode_block(&evs, Some(3), &mut buf).unwrap();
        let sel = ColumnSelection {
            qty: true,
            instrument: true,
            kind: true,
            ..ColumnSelection::NONE
        };
        // Pre-seed an unselected slot: it must come back None.
        let mut cols = ColumnData {
            price: Some(vec![1, 2, 3]),
            ..ColumnData::default()
        };
        let (h, used) = decode_block_columns(&buf, sel, &mut cols).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(h.n_events, 300);
        assert!(cols.price.is_none() && cols.recv_mono.is_none());
        let qty = cols.qty.as_ref().unwrap();
        let inst = cols.instrument.as_ref().unwrap();
        let kind = cols.kind.as_ref().unwrap();
        assert_eq!(qty.len(), 300);
        for (i, e) in evs.iter().enumerate() {
            assert_eq!((qty[i], inst[i], kind[i]), (e.qty, e.instrument, e.kind));
        }
        // Second decode into the same ColumnData: buffers reused, same data.
        let qty_ptr = qty.as_ptr();
        decode_block_columns(&buf, sel, &mut cols).unwrap();
        assert_eq!(cols.qty.as_ref().unwrap().len(), 300);
        assert_eq!(
            cols.qty.as_ref().unwrap().as_ptr(),
            qty_ptr,
            "selected buffer allocation is reused across blocks"
        );
    }

    #[test]
    fn empty_selection_still_validates_crc() {
        let evs = mk_events(64, 8);
        let mut buf = Vec::new();
        encode_block(&evs, None, &mut buf).unwrap();
        let mut cols = ColumnData::default();
        let (h, _) = decode_block_columns(&buf, ColumnSelection::NONE, &mut cols).unwrap();
        assert_eq!(h.n_events, 64);
        assert_eq!(cols, ColumnData::default());
        // corrupt the body: even a no-column read must reject it
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        assert!(matches!(
            decode_block_columns(&buf, ColumnSelection::NONE, &mut cols),
            Err(BlockError::Torn(_))
        ));
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
            let mut cols = ColumnData::default();
            let _ = decode_block_columns(&bytes, ColumnSelection::ALL, &mut cols);
        }

        #[test]
        fn pruned_equals_full_decode(seed in any::<u64>(), n in 1usize..600, z in any::<bool>(), mask in any::<u16>()) {
            let evs = mk_events(n, seed);
            let mut buf = Vec::new();
            encode_block(&evs, if z { Some(1) } else { None }, &mut buf).unwrap();
            let sel = ColumnSelection {
                recv_mono: mask & 1 != 0,
                recv_wall: mask & 2 != 0,
                venue_ts: mask & 4 != 0,
                venue_seq: mask & 8 != 0,
                price: mask & 16 != 0,
                qty: mask & 32 != 0,
                aux: mask & 64 != 0,
                instrument: mask & 128 != 0,
                kind: mask & 256 != 0,
                venue: mask & 512 != 0,
                flags: mask & 1024 != 0,
            };
            let mut cols = ColumnData::default();
            let (h, used) = decode_block_columns(&buf, sel, &mut cols).unwrap();
            prop_assert_eq!(used, buf.len());
            prop_assert_eq!(h.n_events as usize, n);
            let mut full = Vec::new();
            decode_block(&buf, &mut full).unwrap();
            prop_assert_eq!(&full[..], &evs[..]);
            macro_rules! check {
                ($field:ident, $on:expr, $ev:ident -> $get:expr) => {
                    match (&cols.$field, $on) {
                        (Some(got), true) => {
                            let want: Vec<_> = full.iter().map(|$ev| $get).collect();
                            prop_assert_eq!(&got[..], &want[..]);
                        }
                        (None, false) => {}
                        (got, on) => prop_assert!(false, "{}: {:?} selected={}", stringify!($field), got.is_some(), on),
                    }
                };
            }
            check!(recv_mono, sel.recv_mono, e -> e.recv_mono_ns);
            check!(recv_wall, sel.recv_wall, e -> e.recv_wall_ns);
            check!(venue_ts, sel.venue_ts, e -> e.venue_ts_ns);
            check!(venue_seq, sel.venue_seq, e -> e.venue_seq);
            check!(price, sel.price, e -> e.price);
            check!(qty, sel.qty, e -> e.qty);
            check!(aux, sel.aux, e -> e.aux);
            check!(instrument, sel.instrument, e -> e.instrument);
            check!(kind, sel.kind, e -> e.kind);
            check!(venue, sel.venue, e -> e.venue);
            check!(flags, sel.flags, e -> e.flags);
        }
    }
}
