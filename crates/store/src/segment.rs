//! Store segment files: append-only streams of [`crate::block`] blocks with
//! a small self-describing header, crash recovery, an optional seal footer
//! (per-block directory for O(log n) time-range seeks), and zero-copy mmap
//! reads.
//!
//! Layout (all integers little-endian):
//!
//! ```text
//! header:  magic "FBSTORE1" | version u8=1 | flags u8=0 | rsvd u16 |
//!          meta_len u32 | meta bytes (by convention JSON)
//! blocks:  consecutive blocks, exactly the bytes [`crate::block::encode_block`]
//!          produces
//! footer (sealed files only, at EOF):
//!          directory: per block { file_offset u64 | n_events u32 |
//!            min_recv_mono u64 | max_recv_mono u64 |
//!            min_recv_wall u64 | max_recv_wall u64 }  (tightly packed)
//!          | crc32 u32 (over the directory bytes)
//!          | directory_start_offset u64
//!          | magic "FBSTOREF"   (last 8 bytes of a sealed file)
//! ```
//!
//! Events must be appended in non-decreasing `recv_mono_ns` order; the
//! directory's binary-search range scan depends on it (enforced at append
//! time, checked again by [`StoreReader::verify`]).
//!
//! Recovery model: an unsealed file (crash before [`StoreWriter::seal`], or
//! a sealed file whose footer fails validation) is read by a sequential
//! scan that rebuilds the directory from block headers, CRC-checking each
//! body, and stops at the first torn/corrupt block. The torn tail's offset
//! is exposed via [`StoreReader::torn`]; [`recover_truncate`] chops it off
//! so the file reads cleanly afterwards (mirroring
//! [`flashbook_proto::rawlog::truncate_to_valid`]).
//!
//! Zero-copy notes: [`StoreReader`] memory-maps the file read-only and
//! passes mmap sub-slices straight to [`crate::block::decode_block`] — no
//! staging copies. The only allocations on the read path are the caller's
//! output `Vec<Event>` (and zstd's transient decompress buffer for
//! compressed blocks); that is the design.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use flashbook_proto::event::Event;
use memmap2::Mmap;

use crate::block::{self, BlockError, BlockHeader, MAX_BLOCK_EVENTS};

/// Segment file magic (first 8 bytes).
pub const FILE_MAGIC: &[u8; 8] = b"FBSTORE1";
/// Seal footer magic (last 8 bytes of a sealed file).
pub const FOOTER_MAGIC: &[u8; 8] = b"FBSTOREF";
/// Format version written by this code.
pub const FILE_VERSION: u8 = 1;
/// Fixed file-header size before the meta bytes.
pub const FILE_HEADER_LEN: usize = 8 + 1 + 1 + 2 + 4;
/// On-disk size of one directory entry.
pub const DIR_ENTRY_LEN: usize = 8 + 4 + 8 + 8 + 8 + 8;
/// Footer bytes after the directory: crc32 + directory_start + magic.
pub const FOOTER_TRAILER_LEN: usize = 4 + 8 + 8;
/// Default events per block for [`StoreWriter::create`] callers.
pub const DEFAULT_BLOCK_EVENTS: usize = 8192;
/// Sanity cap on the meta blob.
pub const MAX_META_LEN: usize = 16 * 1024 * 1024;

/// Errors from segment writing/reading.
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    /// Underlying IO error.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// Block-level encode/decode error.
    #[error("block: {0}")]
    Block(#[from] BlockError),
    /// File does not start with a valid segment header.
    #[error("bad header: {0}")]
    BadHeader(&'static str),
    /// Invalid writer configuration or input.
    #[error("config: {0}")]
    Config(&'static str),
    /// Append violated the non-decreasing `recv_mono_ns` invariant.
    #[error("out-of-order append: last recv_mono_ns {last}, next {next}")]
    OutOfOrder {
        /// `recv_mono_ns` of the previously-appended event.
        last: u64,
        /// `recv_mono_ns` of the rejected event.
        next: u64,
    },
    /// Block index out of range.
    #[error("block index {idx} out of range ({blocks} blocks)")]
    BadBlockIndex {
        /// Requested index.
        idx: usize,
        /// Number of blocks in the file.
        blocks: usize,
    },
    /// A consistency check failed (directory vs. block contents).
    #[error("corrupt segment: {0}")]
    Corrupt(&'static str),
}

/// Per-block directory entry: where the block lives and what it covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockMeta {
    /// Byte offset of the block header from the start of the file.
    pub file_offset: u64,
    /// Events in the block.
    pub n_events: u32,
    /// Min `recv_mono_ns` in the block.
    pub min_recv_mono: u64,
    /// Max `recv_mono_ns` in the block.
    pub max_recv_mono: u64,
    /// Min `recv_wall_ns` in the block.
    pub min_recv_wall: u64,
    /// Max `recv_wall_ns` in the block.
    pub max_recv_wall: u64,
}

impl BlockMeta {
    fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.file_offset.to_le_bytes());
        out.extend_from_slice(&self.n_events.to_le_bytes());
        out.extend_from_slice(&self.min_recv_mono.to_le_bytes());
        out.extend_from_slice(&self.max_recv_mono.to_le_bytes());
        out.extend_from_slice(&self.min_recv_wall.to_le_bytes());
        out.extend_from_slice(&self.max_recv_wall.to_le_bytes());
    }

    fn read_from(b: &[u8]) -> Self {
        debug_assert_eq!(b.len(), DIR_ENTRY_LEN);
        Self {
            file_offset: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            n_events: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            min_recv_mono: u64::from_le_bytes(b[12..20].try_into().unwrap()),
            max_recv_mono: u64::from_le_bytes(b[20..28].try_into().unwrap()),
            min_recv_wall: u64::from_le_bytes(b[28..36].try_into().unwrap()),
            max_recv_wall: u64::from_le_bytes(b[36..44].try_into().unwrap()),
        }
    }

    fn from_header(h: &BlockHeader, file_offset: u64) -> Self {
        Self {
            file_offset,
            n_events: h.n_events,
            min_recv_mono: h.min_recv_mono,
            max_recv_mono: h.max_recv_mono,
            min_recv_wall: h.min_recv_wall,
            max_recv_wall: h.max_recv_wall,
        }
    }
}

/// Torn-tail report: everything before `valid_bytes` is intact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Torn {
    /// Safe truncation offset (end of the last valid block).
    pub valid_bytes: u64,
    /// Why the tail was rejected.
    pub reason: &'static str,
}

fn block_err_reason(e: &BlockError) -> &'static str {
    match e {
        BlockError::Encode(r)
        | BlockError::BadHeader(r)
        | BlockError::Torn(r)
        | BlockError::Corrupt(r) => r,
    }
}

/// Writes one segment file: buffers events, emits a block every
/// `block_events` appends, and (optionally) seals with a directory footer.
pub struct StoreWriter {
    w: BufWriter<File>,
    block_events: usize,
    zstd: Option<i32>,
    pending: Vec<Event>,
    scratch: Vec<u8>,
    dir: Vec<BlockMeta>,
    bytes: u64,
    events: u64,
    last_mono: u64,
}

impl StoreWriter {
    /// Create a new segment at `path` (fails if it exists — segments are
    /// immutable once created). `block_events` is the events-per-block cut
    /// point (callers usually pass [`DEFAULT_BLOCK_EVENTS`]; must be
    /// `1..=`[`MAX_BLOCK_EVENTS`]). `zstd`: `None` stores raw column bytes,
    /// `Some(level)` compresses each block body (blocks that don't shrink
    /// are stored raw — files may mix both).
    pub fn create(
        path: &Path,
        meta: &[u8],
        block_events: usize,
        zstd: Option<i32>,
    ) -> Result<Self, SegmentError> {
        if block_events == 0 || block_events > MAX_BLOCK_EVENTS {
            return Err(SegmentError::Config("block_events out of range"));
        }
        if meta.len() > MAX_META_LEN {
            return Err(SegmentError::Config("meta too large"));
        }
        let file = File::options().write(true).create_new(true).open(path)?;
        let mut w = BufWriter::with_capacity(1 << 20, file);
        w.write_all(FILE_MAGIC)?;
        w.write_all(&[FILE_VERSION, 0u8])?;
        w.write_all(&0u16.to_le_bytes())?;
        #[allow(clippy::cast_possible_truncation)]
        w.write_all(&(meta.len() as u32).to_le_bytes())?;
        w.write_all(meta)?;
        Ok(Self {
            w,
            block_events,
            zstd,
            pending: Vec::with_capacity(block_events),
            scratch: Vec::new(),
            dir: Vec::new(),
            bytes: (FILE_HEADER_LEN + meta.len()) as u64,
            events: 0,
            last_mono: 0,
        })
    }

    /// Append one event. Buffers until `block_events` are pending, then
    /// encodes and writes a block. Rejects events whose `recv_mono_ns` goes
    /// backwards (the directory range search requires non-decreasing order)
    /// and events with a non-zero `rsvd` byte.
    pub fn append(&mut self, e: &Event) -> Result<(), SegmentError> {
        if e.rsvd != 0 {
            return Err(SegmentError::Config("rsvd byte must be 0"));
        }
        if e.recv_mono_ns < self.last_mono {
            return Err(SegmentError::OutOfOrder {
                last: self.last_mono,
                next: e.recv_mono_ns,
            });
        }
        self.last_mono = e.recv_mono_ns;
        self.pending.push(*e);
        if self.pending.len() >= self.block_events {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), SegmentError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.scratch.clear();
        block::encode_block(&self.pending, self.zstd, &mut self.scratch)?;
        // Re-parse our own header for the directory entry: one source of
        // truth for the min/max ranges.
        let h =
            block::parse_header(&self.scratch)?.expect("encode_block produced a parseable header");
        self.w.write_all(&self.scratch)?;
        self.dir.push(BlockMeta::from_header(&h, self.bytes));
        self.bytes += self.scratch.len() as u64;
        self.events += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    /// Force-flush any partial block and fsync — the durability point.
    /// Everything appended so far is readable after a crash (via the
    /// recovery scan; the file stays unsealed until [`Self::seal`]).
    pub fn checkpoint(&mut self) -> Result<(), SegmentError> {
        self.flush_block()?;
        self.w.flush()?;
        self.w.get_ref().sync_data()?;
        Ok(())
    }

    /// Flush any partial block, write the directory footer, fsync and
    /// close. Returns the final file size in bytes.
    pub fn seal(mut self) -> Result<u64, SegmentError> {
        self.flush_block()?;
        let dir_start = self.bytes;
        let mut dir_bytes = Vec::with_capacity(self.dir.len() * DIR_ENTRY_LEN);
        for m in &self.dir {
            m.write_to(&mut dir_bytes);
        }
        let crc = crc32fast::hash(&dir_bytes);
        self.w.write_all(&dir_bytes)?;
        self.w.write_all(&crc.to_le_bytes())?;
        self.w.write_all(&dir_start.to_le_bytes())?;
        self.w.write_all(FOOTER_MAGIC)?;
        self.w.flush()?;
        self.w.get_ref().sync_data()?;
        Ok(self.bytes + dir_bytes.len() as u64 + FOOTER_TRAILER_LEN as u64)
    }

    /// Events written into completed blocks (excludes the pending buffer).
    pub fn events(&self) -> u64 {
        self.events
    }

    /// Events buffered but not yet cut into a block.
    pub fn pending_events(&self) -> usize {
        self.pending.len()
    }

    /// Completed blocks so far.
    pub fn n_blocks(&self) -> usize {
        self.dir.len()
    }

    /// Bytes of file content produced so far (header + completed blocks;
    /// excludes the pending buffer and the eventual footer).
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// Reads one segment file via a read-only mmap.
///
/// Sealed files use the footer directory; unsealed (or footer-invalid)
/// files are indexed by a sequential recovery scan — both paths yield the
/// same [`BlockMeta`] directory, so every read API works on either.
pub struct StoreReader {
    map: Mmap,
    meta_off: usize,
    meta_len: usize,
    data_end: usize,
    blocks: Vec<BlockMeta>,
    n_events: u64,
    sealed: bool,
    torn: Option<Torn>,
}

impl StoreReader {
    /// Open and index a segment file.
    ///
    /// Footer handling: if the file ends with [`FOOTER_MAGIC`] and the
    /// directory passes its CRC and geometry checks, the directory is used
    /// as-is (`sealed() == true`). Otherwise — unsealed file, crashed
    /// writer, or a corrupt footer — we fall back to the recovery scan; a
    /// corrupt footer then shows up as a torn tail at the directory's
    /// start offset (the data blocks stay readable, and
    /// [`recover_truncate`] turns the file into a clean unsealed one).
    pub fn open(path: &Path) -> Result<Self, SegmentError> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len < FILE_HEADER_LEN {
            return Err(SegmentError::BadHeader("short header"));
        }
        // SAFETY: read-only mapping of a segment file. Segments are
        // immutable once written (writer uses create_new); concurrent
        // mutation of a mapped file is outside the format's contract.
        let map = unsafe { Mmap::map(&file)? };
        if &map[0..8] != FILE_MAGIC {
            return Err(SegmentError::BadHeader("bad magic"));
        }
        if map[8] != FILE_VERSION {
            return Err(SegmentError::BadHeader("unsupported version"));
        }
        if map[9] != 0 {
            return Err(SegmentError::BadHeader("unknown flags"));
        }
        let meta_len = u32::from_le_bytes(map[12..16].try_into().unwrap()) as usize;
        if meta_len > MAX_META_LEN {
            return Err(SegmentError::BadHeader("meta too large"));
        }
        let data_start = FILE_HEADER_LEN + meta_len;
        if data_start > len {
            return Err(SegmentError::BadHeader("meta truncated"));
        }

        if let Some((blocks, dir_start)) = try_read_footer(&map, data_start) {
            let n_events = blocks.iter().map(|b| u64::from(b.n_events)).sum();
            return Ok(Self {
                map,
                meta_off: FILE_HEADER_LEN,
                meta_len,
                data_end: dir_start,
                blocks,
                n_events,
                sealed: true,
                torn: None,
            });
        }

        // Recovery scan: walk block headers, CRC-check each body, stop at
        // the first torn/corrupt block.
        let mut blocks = Vec::new();
        let mut n_events = 0u64;
        let mut pos = data_start;
        let mut torn = None;
        loop {
            match block::parse_header(&map[pos..]) {
                Ok(None) => break,
                Ok(Some(h)) => {
                    let total = h.total_len();
                    if pos + total > len {
                        torn = Some(Torn {
                            valid_bytes: pos as u64,
                            reason: "body truncated",
                        });
                        break;
                    }
                    let stored = &map[pos + block::HEADER_LEN..pos + total];
                    if crc32fast::hash(stored) != h.crc {
                        torn = Some(Torn {
                            valid_bytes: pos as u64,
                            reason: "crc mismatch",
                        });
                        break;
                    }
                    blocks.push(BlockMeta::from_header(&h, pos as u64));
                    n_events += u64::from(h.n_events);
                    pos += total;
                }
                Err(e) => {
                    torn = Some(Torn {
                        valid_bytes: pos as u64,
                        reason: block_err_reason(&e),
                    });
                    break;
                }
            }
        }
        Ok(Self {
            map,
            meta_off: FILE_HEADER_LEN,
            meta_len,
            data_end: pos,
            blocks,
            n_events,
            sealed: false,
            torn,
        })
    }

    /// The opaque meta bytes from the file header.
    pub fn meta(&self) -> &[u8] {
        &self.map[self.meta_off..self.meta_off + self.meta_len]
    }

    /// The block directory (footer-loaded or scan-built).
    pub fn blocks(&self) -> &[BlockMeta] {
        &self.blocks
    }

    /// Number of blocks.
    pub fn n_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Total events across all indexed blocks.
    pub fn n_events(&self) -> u64 {
        self.n_events
    }

    /// True if the footer directory was present and valid.
    pub fn sealed(&self) -> bool {
        self.sealed
    }

    /// Torn-tail report from the recovery scan (`None` for sealed or
    /// cleanly-unsealed files).
    pub fn torn(&self) -> Option<Torn> {
        self.torn
    }

    /// Decode block `idx` into `out` (appended); CRC and column
    /// consistency are validated by [`crate::block::decode_block`]. The
    /// mmap slice is the decode input — no intermediate copy.
    pub fn decode_block(
        &self,
        idx: usize,
        out: &mut Vec<Event>,
    ) -> Result<BlockHeader, SegmentError> {
        let Some(m) = self.blocks.get(idx) else {
            return Err(SegmentError::BadBlockIndex {
                idx,
                blocks: self.blocks.len(),
            });
        };
        let off = m.file_offset as usize;
        let (h, _consumed) = block::decode_block(&self.map[off..self.data_end], out)?;
        if h.n_events != m.n_events {
            return Err(SegmentError::Corrupt("directory/block n_events mismatch"));
        }
        Ok(h)
    }

    /// Sequential scan over every event in file order. Returns the number
    /// of events visited.
    pub fn scan<F: FnMut(&Event)>(&self, mut sink: F) -> Result<u64, SegmentError> {
        let mut buf = Vec::new();
        let mut n = 0u64;
        for idx in 0..self.blocks.len() {
            buf.clear();
            self.decode_block(idx, &mut buf)?;
            for e in &buf {
                sink(e);
                n += 1;
            }
        }
        Ok(n)
    }

    /// Scan events with `recv_mono_ns` in `[lo, hi]` (inclusive). Binary
    /// search over the directory picks the candidate blocks (blocks are
    /// mono-ordered because appends are); only the edge blocks get a
    /// per-event filter. Returns the number of events visited.
    pub fn scan_mono_range<F: FnMut(&Event)>(
        &self,
        lo: u64,
        hi: u64,
        mut sink: F,
    ) -> Result<u64, SegmentError> {
        if lo > hi {
            return Ok(0);
        }
        // First block that could contain an event >= lo.
        let start = self.blocks.partition_point(|b| b.max_recv_mono < lo);
        let mut buf = Vec::new();
        let mut n = 0u64;
        for idx in start..self.blocks.len() {
            let m = &self.blocks[idx];
            if m.min_recv_mono > hi {
                break;
            }
            buf.clear();
            self.decode_block(idx, &mut buf)?;
            if m.min_recv_mono >= lo && m.max_recv_mono <= hi {
                // fully inside: no per-event filter
                for e in &buf {
                    sink(e);
                    n += 1;
                }
            } else {
                for e in buf.iter().filter(|e| (lo..=hi).contains(&e.recv_mono_ns)) {
                    sink(e);
                    n += 1;
                }
            }
        }
        Ok(n)
    }

    /// Full-file check: decode every block, verify the directory entry
    /// matches the block header, verify block contiguity, and verify
    /// `recv_mono_ns` is non-decreasing within and across blocks. Returns
    /// the total events verified. A torn tail (already excluded from the
    /// index by [`Self::open`]) does not fail verification — check
    /// [`Self::torn`] separately.
    pub fn verify(&self) -> Result<u64, SegmentError> {
        let mut buf = Vec::new();
        let mut n = 0u64;
        let mut expect_off = self.blocks.first().map(|b| b.file_offset);
        let mut last_mono = 0u64;
        for (idx, m) in self.blocks.iter().enumerate() {
            if expect_off != Some(m.file_offset) {
                return Err(SegmentError::Corrupt("directory offsets not contiguous"));
            }
            buf.clear();
            let h = self.decode_block(idx, &mut buf)?;
            if BlockMeta::from_header(&h, m.file_offset) != *m {
                return Err(SegmentError::Corrupt("directory/block header mismatch"));
            }
            for e in &buf {
                if e.recv_mono_ns < last_mono {
                    return Err(SegmentError::Corrupt("recv_mono_ns not non-decreasing"));
                }
                last_mono = e.recv_mono_ns;
            }
            n += buf.len() as u64;
            expect_off = Some(m.file_offset + h.total_len() as u64);
        }
        if let Some(end) = expect_off
            && end != self.data_end as u64
        {
            return Err(SegmentError::Corrupt("directory end != data end"));
        }
        if n != self.n_events {
            return Err(SegmentError::Corrupt("event count mismatch"));
        }
        Ok(n)
    }
}

/// Try to load the seal footer. `None` means "treat the file as unsealed"
/// (missing/incomplete/corrupt footer) — the caller falls back to the
/// recovery scan.
fn try_read_footer(map: &Mmap, data_start: usize) -> Option<(Vec<BlockMeta>, usize)> {
    let len = map.len();
    if len < data_start + FOOTER_TRAILER_LEN {
        return None;
    }
    if &map[len - 8..] != FOOTER_MAGIC {
        return None;
    }
    let dir_start = u64::from_le_bytes(map[len - 16..len - 8].try_into().unwrap());
    let crc = u32::from_le_bytes(map[len - FOOTER_TRAILER_LEN..len - 16].try_into().unwrap());
    let dir_end = len - FOOTER_TRAILER_LEN;
    let dir_start = usize::try_from(dir_start).ok()?;
    if dir_start < data_start || dir_start > dir_end {
        return None;
    }
    let dir_bytes = &map[dir_start..dir_end];
    if !dir_bytes.len().is_multiple_of(DIR_ENTRY_LEN) {
        return None;
    }
    if crc32fast::hash(dir_bytes) != crc {
        return None;
    }
    // Light geometry checks (verify() does the deep, decode-everything
    // one): the first block starts right after the meta, offsets strictly
    // increase, everything lands inside the data region, counts plausible.
    let mut blocks: Vec<BlockMeta> = Vec::with_capacity(dir_bytes.len() / DIR_ENTRY_LEN);
    for (i, chunk) in dir_bytes.chunks_exact(DIR_ENTRY_LEN).enumerate() {
        let m = BlockMeta::read_from(chunk);
        if m.n_events == 0 || m.n_events as usize > MAX_BLOCK_EVENTS {
            return None;
        }
        if m.file_offset as usize >= dir_start {
            return None;
        }
        let ok = if i == 0 {
            m.file_offset == data_start as u64
        } else {
            m.file_offset > blocks[i - 1].file_offset
        };
        if !ok {
            return None;
        }
        blocks.push(m);
    }
    if blocks.is_empty() && dir_start != data_start {
        return None; // unindexed bytes between header and footer
    }
    Some((blocks, dir_start))
}

/// Recover a crashed segment in place: if `path` has a torn tail, truncate
/// to the last valid block (and fsync); a valid sealed file or a cleanly
/// unsealed file is left untouched. Returns the resulting file length.
pub fn recover_truncate(path: &Path) -> Result<u64, SegmentError> {
    let torn = {
        let r = StoreReader::open(path)?;
        r.torn()
    }; // reader (and its mmap) dropped before we touch the file
    match torn {
        None => Ok(std::fs::metadata(path)?.len()),
        Some(t) => {
            let f = File::options().write(true).open(path)?;
            f.set_len(t.valid_bytes)?;
            f.sync_data()?;
            Ok(t.valid_bytes)
        }
    }
}
