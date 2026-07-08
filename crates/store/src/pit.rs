//! Point-in-time (PIT) snapshot index and query.
//!
//! Reconstructing a book "as of time t" from a segment file means finding
//! the latest complete venue snapshot at or before `t` and folding every
//! event for that instrument from the snapshot's `SnapBegin` through `t`.
//! This module provides the two store-side pieces:
//!
//! - [`SnapshotIndex`]: one [`SnapEntry`] per COMPLETE snapshot in a
//!   segment — a `SnapBegin .. SnapEnd` bracket for one instrument with no
//!   intervening `Clear` or restarted `SnapBegin` for that instrument
//!   (events for *other* instruments may interleave freely). Built by
//!   streaming the store once ([`SnapshotIndex::build`]), persisted as a
//!   small CRC-protected sidecar file ([`SnapshotIndex::save`] /
//!   [`SnapshotIndex::load`]), and queried with
//!   [`SnapshotIndex::latest_at`] (binary search).
//! - [`pit_scan`]: stream the events for one entry's instrument from its
//!   `SnapBegin` position through the last event with `recv_mono_ns <= t`.
//!   *Every* event for the instrument in that window is delivered —
//!   book-affecting kinds AND trades/heartbeats/etc. — the sink filters if
//!   it wants.
//!
//! Sidecar layout (all integers little-endian):
//!
//! ```text
//! magic "FBSNPIX1" | version u8=1 | rsvd [u8;3]=0 | n u32
//! n packed entries { instrument u32 | mono u64 | block_idx u32 | event_idx u32 }
//! crc32 u32 (over the packed entry bytes)
//! ```
//!
//! A torn sidecar (truncated mid-write) and a corrupt one (bad
//! magic/version/CRC/sort order/trailing bytes) are reported as distinct
//! errors ([`PitError::TornSidecar`] vs [`PitError::CorruptSidecar`]); in
//! both cases the sidecar is disposable — callers rebuild from the segment
//! with [`SnapshotIndex::build`].
//!
//! Layering note: there is deliberately **no** `pit_book()` convenience
//! here that returns a folded `LadderBook` — `flashbook-store` must not
//! depend on `flashbook-lob` (the store sits below the book layer). The
//! fold lives in the bench/replay side: call [`pit_scan`] and apply each
//! event to an `L2Book` there.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use flashbook_proto::event::{Event, EventKind};

use crate::segment::{SegmentError, StoreReader};

/// Sidecar file magic (first 8 bytes).
pub const PIT_MAGIC: &[u8; 8] = b"FBSNPIX1";
/// Sidecar format version written by this code.
pub const PIT_VERSION: u8 = 1;
/// Fixed sidecar header size (magic + version + rsvd + n).
pub const PIT_HEADER_LEN: usize = 8 + 1 + 3 + 4;
/// On-disk size of one packed entry.
pub const PIT_ENTRY_LEN: usize = 4 + 8 + 4 + 4;

/// Errors from PIT index build/load/save/scan.
#[derive(Debug, thiserror::Error)]
pub enum PitError {
    /// Underlying IO error.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// Error reading the underlying segment.
    #[error("segment: {0}")]
    Segment(#[from] SegmentError),
    /// Sidecar is a truncated prefix of a valid file (torn write).
    /// Rebuild with [`SnapshotIndex::build`].
    #[error("torn sidecar: {0}")]
    TornSidecar(&'static str),
    /// Sidecar bytes are not a valid sidecar (bad magic/version/CRC/...).
    /// Rebuild with [`SnapshotIndex::build`].
    #[error("corrupt sidecar: {0}")]
    CorruptSidecar(&'static str),
    /// An entry does not match the segment it is used against (stale or
    /// foreign index). Rebuild with [`SnapshotIndex::build`].
    #[error("index/store mismatch: {0}")]
    Mismatch(&'static str),
    /// Index construction hit a size limit of the sidecar format.
    #[error("index build: {0}")]
    Build(&'static str),
}

/// One complete snapshot: where its `SnapBegin` lives in the segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapEntry {
    /// Instrument id the snapshot belongs to.
    pub instrument: u32,
    /// `recv_mono_ns` of the `SnapBegin` event.
    pub mono: u64,
    /// Block index (into [`StoreReader::blocks`]) holding the `SnapBegin`.
    pub block_idx: u32,
    /// Position of the `SnapBegin` within that block's events.
    pub event_idx: u32,
}

impl SnapEntry {
    fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.instrument.to_le_bytes());
        out.extend_from_slice(&self.mono.to_le_bytes());
        out.extend_from_slice(&self.block_idx.to_le_bytes());
        out.extend_from_slice(&self.event_idx.to_le_bytes());
    }

    fn read_from(b: &[u8]) -> Self {
        debug_assert_eq!(b.len(), PIT_ENTRY_LEN);
        Self {
            instrument: u32::from_le_bytes(b[0..4].try_into().unwrap()),
            mono: u64::from_le_bytes(b[4..12].try_into().unwrap()),
            block_idx: u32::from_le_bytes(b[12..16].try_into().unwrap()),
            event_idx: u32::from_le_bytes(b[16..20].try_into().unwrap()),
        }
    }
}

/// Index of every complete snapshot in one segment file, sorted by
/// `(instrument, mono)`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SnapshotIndex {
    entries: Vec<SnapEntry>,
}

impl SnapshotIndex {
    /// Build the index by streaming the store once.
    ///
    /// State machine per instrument: `SnapBegin` opens (or restarts —
    /// discarding a prior unfinished bracket) a pending snapshot, `Clear`
    /// discards it, `SnapEnd` completes it into an entry. A `SnapEnd` with
    /// no pending bracket is ignored, as is a bracket still open at EOF
    /// (that includes brackets cut off by a torn tail: the reader's
    /// directory already excludes the tail, so an entry is only ever
    /// produced for a snapshot whose `SnapEnd` is durably on disk).
    pub fn build(reader: &StoreReader) -> Result<Self, PitError> {
        struct Pending {
            mono: u64,
            block_idx: u32,
            event_idx: u32,
        }
        let mut pending: HashMap<u32, Pending> = HashMap::new();
        let mut entries: Vec<SnapEntry> = Vec::new();
        let mut buf: Vec<Event> = Vec::new();
        for bi in 0..reader.n_blocks() {
            let block_idx = u32::try_from(bi).map_err(|_| PitError::Build("too many blocks"))?;
            buf.clear();
            reader.decode_block(bi, &mut buf)?;
            for (ei, e) in buf.iter().enumerate() {
                match e.kind().ok() {
                    Some(EventKind::SnapBegin) => {
                        // fits: blocks hold at most MAX_BLOCK_EVENTS (65536)
                        let event_idx = u32::try_from(ei).expect("event index fits u32");
                        pending.insert(
                            e.instrument,
                            Pending {
                                mono: e.recv_mono_ns,
                                block_idx,
                                event_idx,
                            },
                        );
                    }
                    Some(EventKind::Clear) => {
                        pending.remove(&e.instrument);
                    }
                    Some(EventKind::SnapEnd) => {
                        if let Some(p) = pending.remove(&e.instrument) {
                            entries.push(SnapEntry {
                                instrument: e.instrument,
                                mono: p.mono,
                                block_idx: p.block_idx,
                                event_idx: p.event_idx,
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
        // Stable sort: entries with equal (instrument, mono) keep file
        // order, so `latest_at` returns the latest-in-file among ties.
        entries.sort_by_key(|e| (e.instrument, e.mono));
        Ok(Self { entries })
    }

    /// The entries, sorted by `(instrument, mono)`.
    pub fn entries(&self) -> &[SnapEntry] {
        &self.entries
    }

    /// Number of complete snapshots indexed.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if no complete snapshot was found.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Latest complete snapshot for `instrument` whose `SnapBegin`
    /// `recv_mono_ns <= t_mono` (binary search; `None` if the instrument
    /// has no complete snapshot at or before `t_mono`).
    pub fn latest_at(&self, instrument: u32, t_mono: u64) -> Option<&SnapEntry> {
        let idx = self
            .entries
            .partition_point(|e| (e.instrument, e.mono) <= (instrument, t_mono));
        let e = self.entries.get(idx.checked_sub(1)?)?;
        (e.instrument == instrument).then_some(e)
    }

    /// Write the sidecar to `path` (created or overwritten — the sidecar
    /// is derived data), fsynced.
    pub fn save(&self, path: &Path) -> Result<(), PitError> {
        let n =
            u32::try_from(self.entries.len()).map_err(|_| PitError::Build("too many entries"))?;
        let mut body = Vec::with_capacity(self.entries.len() * PIT_ENTRY_LEN);
        for e in &self.entries {
            e.write_to(&mut body);
        }
        let crc = crc32fast::hash(&body);
        let mut f = File::create(path)?;
        f.write_all(PIT_MAGIC)?;
        f.write_all(&[PIT_VERSION, 0, 0, 0])?;
        f.write_all(&n.to_le_bytes())?;
        f.write_all(&body)?;
        f.write_all(&crc.to_le_bytes())?;
        f.sync_data()?;
        Ok(())
    }

    /// Load a sidecar written by [`Self::save`].
    ///
    /// A truncated file yields [`PitError::TornSidecar`]; bad
    /// magic/version/reserved bytes, CRC mismatch, trailing bytes, or
    /// unsorted entries yield [`PitError::CorruptSidecar`]. Either way the
    /// caller should rebuild via [`Self::build`].
    pub fn load(path: &Path) -> Result<Self, PitError> {
        Self::from_bytes(&std::fs::read(path)?)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, PitError> {
        let m = bytes.len().min(PIT_MAGIC.len());
        if bytes[..m] != PIT_MAGIC[..m] {
            return Err(PitError::CorruptSidecar("bad magic"));
        }
        if bytes.len() < PIT_HEADER_LEN {
            return Err(PitError::TornSidecar("short header"));
        }
        if bytes[8] != PIT_VERSION {
            return Err(PitError::CorruptSidecar("unsupported version"));
        }
        if bytes[9..12] != [0u8; 3] {
            return Err(PitError::CorruptSidecar("nonzero reserved bytes"));
        }
        let n = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        let body_len = n
            .checked_mul(PIT_ENTRY_LEN)
            .ok_or(PitError::CorruptSidecar("entry count overflow"))?;
        let need = PIT_HEADER_LEN
            .checked_add(body_len)
            .and_then(|v| v.checked_add(4))
            .ok_or(PitError::CorruptSidecar("entry count overflow"))?;
        if bytes.len() < need {
            return Err(PitError::TornSidecar("truncated entries"));
        }
        if bytes.len() > need {
            return Err(PitError::CorruptSidecar("trailing bytes"));
        }
        let body = &bytes[PIT_HEADER_LEN..PIT_HEADER_LEN + body_len];
        let crc = u32::from_le_bytes(bytes[need - 4..need].try_into().unwrap());
        if crc32fast::hash(body) != crc {
            return Err(PitError::CorruptSidecar("crc mismatch"));
        }
        let mut entries = Vec::with_capacity(n);
        for chunk in body.chunks_exact(PIT_ENTRY_LEN) {
            entries.push(SnapEntry::read_from(chunk));
        }
        if !entries.is_sorted_by_key(|e| (e.instrument, e.mono)) {
            return Err(PitError::CorruptSidecar("entries not sorted"));
        }
        Ok(Self { entries })
    }
}

/// Stream the PIT event window for one index entry: every event for
/// `entry.instrument` from its `SnapBegin` (inclusive) through the last
/// event in the file with `recv_mono_ns <= t_mono`, in file order. Events
/// for other instruments are skipped; all kinds for the instrument
/// (snapshot levels, deltas, trades, heartbeats, ...) are delivered — the
/// sink filters if it wants. Returns the number of events delivered.
///
/// This is the query the PIT benchmark folds into a book: apply the
/// delivered book-affecting events to an `L2Book` and the book state as of
/// `t_mono` falls out (see the module-level layering note for why the fold
/// itself is not provided here).
///
/// The scan stops at the first event with `recv_mono_ns > t_mono`, relying
/// on the segment invariant that `recv_mono_ns` is non-decreasing in file
/// order (enforced by `StoreWriter::append`). Blocks whose directory range
/// starts past `t_mono` are never decoded.
///
/// The entry is validated against the reader (it must point at a
/// `SnapBegin` with matching instrument and mono); a stale or foreign
/// entry yields [`PitError::Mismatch`]. Callers normally obtain the entry
/// from [`SnapshotIndex::latest_at`], which guarantees
/// `entry.mono <= t_mono`; passing a `t_mono` before `entry.mono` is not
/// an error but delivers zero events.
pub fn pit_scan<F: FnMut(&Event)>(
    reader: &StoreReader,
    entry: &SnapEntry,
    t_mono: u64,
    mut sink: F,
) -> Result<u64, PitError> {
    let start_block = entry.block_idx as usize;
    if start_block >= reader.n_blocks() {
        return Err(PitError::Mismatch("entry block_idx out of range"));
    }
    let mut buf: Vec<Event> = Vec::new();
    let mut n = 0u64;
    for bi in start_block..reader.n_blocks() {
        if bi > start_block && reader.blocks()[bi].min_recv_mono > t_mono {
            break;
        }
        buf.clear();
        reader.decode_block(bi, &mut buf)?;
        let start_ev = if bi == start_block {
            let ei = entry.event_idx as usize;
            let Some(e) = buf.get(ei) else {
                return Err(PitError::Mismatch("entry event_idx out of range"));
            };
            if e.kind().ok() != Some(EventKind::SnapBegin)
                || e.instrument != entry.instrument
                || e.recv_mono_ns != entry.mono
            {
                return Err(PitError::Mismatch("entry does not point at its SnapBegin"));
            }
            ei
        } else {
            0
        };
        for e in &buf[start_ev..] {
            if e.recv_mono_ns > t_mono {
                return Ok(n);
            }
            if e.instrument == entry.instrument {
                sink(e);
                n += 1;
            }
        }
    }
    Ok(n)
}
