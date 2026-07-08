//! L2 book: the common contract for price-level book representations.
//!
//! Two implementations exist (goal mandate: benchmark >= 2, keep the winner):
//! [`crate::btree::BTreeBook`] and [`crate::ladder::LadderBook`]. Both apply
//! the same normalized [`Event`] stream and must produce identical state —
//! enforced by cross-implementation property tests and by
//! [`L2Book::state_digest`], which replay uses to assert byte-identical
//! book states across runs.

use flashbook_proto::event::{Event, EventKind, Side};

/// Result of applying one event to a book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Apply {
    /// A level was set/removed. `top_changed` is true when best bid/ask
    /// (price or size) changed — feeds the top-of-book latency histogram.
    Mutated {
        /// Best bid or ask changed.
        top_changed: bool,
    },
    /// Book-affecting event arrived while unsynced; dropped (counted).
    DroppedUnsynced,
    /// Snapshot started (book cleared, filling).
    SnapshotStarted,
    /// Snapshot complete; book is synced. Carries the venue checksum from
    /// SnapEnd aux (0 if the venue provides none).
    SnapshotComplete {
        /// Venue checksum (e.g. Kraken CRC32), 0 if absent.
        checksum: u32,
    },
    /// A venue Checksum event: verify the book now against `crc`.
    ChecksumToVerify {
        /// Expected CRC32 from the venue.
        crc: u32,
    },
    /// Book cleared and marked unsynced.
    Cleared,
    /// A gap marker: book is now unsynced until the next snapshot.
    GapMarked,
    /// Event does not affect the book (trade, heartbeat).
    NotBook,
}

/// A single-instrument L2 price-level book.
pub trait L2Book: Default {
    /// Apply one normalized event. Events for other instruments must not be
    /// routed here (see [`crate::BookSet`]).
    fn apply(&mut self, ev: &Event) -> Apply;

    /// Best bid (highest price) as (price, qty).
    fn best_bid(&self) -> Option<(i64, i64)>;

    /// Best ask (lowest price) as (price, qty).
    fn best_ask(&self) -> Option<(i64, i64)>;

    /// Number of levels on a side.
    fn depth(&self, side: Side) -> usize;

    /// Copy the top `n` levels into `out` (cleared first): bids descending
    /// by price, asks ascending — the natural display/checksum order.
    fn top_n_into(&self, side: Side, n: usize, out: &mut Vec<(i64, i64)>);

    /// Drop all levels and mark unsynced.
    fn clear(&mut self);

    /// True once a snapshot has completed and no gap/clear followed.
    fn is_synced(&self) -> bool;

    /// Count of book events dropped while unsynced.
    fn dropped_unsynced(&self) -> u64;

    /// Deterministic digest of the full book state (FNV-1a over canonical
    /// level order plus sync flag). Equal sequences of applied events MUST
    /// yield equal digests — replay's byte-identical assertion.
    fn state_digest(&self) -> u64;
}

/// Shared event-decoding logic so every implementation interprets the
/// normalized stream identically. Implementations supply only the level
/// mutation primitives.
pub(crate) trait LevelOps {
    /// Set/replace/delete (qty 0) a level. Returns true if the side's best
    /// level (price or qty) changed.
    fn set_level(&mut self, side: Side, price: i64, qty: i64) -> bool;
    fn clear_levels(&mut self);
}

/// Book sync/snapshot state machine shared by implementations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum SyncState {
    /// No valid book (start, after Clear/Gap).
    #[default]
    Unsynced,
    /// Between SnapBegin and SnapEnd.
    Filling,
    /// Live book.
    Synced,
}

/// Drives the shared state machine; used by both implementations' `apply`.
pub(crate) fn apply_common<B: LevelOps>(
    b: &mut B,
    state: &mut SyncState,
    dropped: &mut u64,
    ev: &Event,
) -> Apply {
    let Ok(kind) = ev.kind() else {
        return Apply::NotBook;
    };
    match kind {
        EventKind::BidSet | EventKind::AskSet => {
            if *state != SyncState::Synced {
                *dropped += 1;
                return Apply::DroppedUnsynced;
            }
            let side = if kind == EventKind::BidSet {
                Side::Bid
            } else {
                Side::Ask
            };
            let top_changed = b.set_level(side, ev.price, ev.qty);
            Apply::Mutated { top_changed }
        }
        EventKind::SnapBegin => {
            b.clear_levels();
            *state = SyncState::Filling;
            Apply::SnapshotStarted
        }
        EventKind::SnapBid | EventKind::SnapAsk => {
            if *state != SyncState::Filling {
                *dropped += 1;
                return Apply::DroppedUnsynced;
            }
            let side = if kind == EventKind::SnapBid {
                Side::Bid
            } else {
                Side::Ask
            };
            b.set_level(side, ev.price, ev.qty);
            Apply::Mutated { top_changed: true }
        }
        EventKind::SnapEnd => {
            if *state != SyncState::Filling {
                *dropped += 1;
                return Apply::DroppedUnsynced;
            }
            *state = SyncState::Synced;
            #[allow(clippy::cast_possible_truncation)]
            Apply::SnapshotComplete {
                checksum: ev.aux as u32,
            }
        }
        EventKind::Checksum =>
        {
            #[allow(clippy::cast_possible_truncation)]
            Apply::ChecksumToVerify { crc: ev.aux as u32 }
        }
        EventKind::Clear => {
            b.clear_levels();
            *state = SyncState::Unsynced;
            Apply::Cleared
        }
        EventKind::Gap => {
            *state = SyncState::Unsynced;
            Apply::GapMarked
        }
        EventKind::Trade | EventKind::Heartbeat => Apply::NotBook,
    }
}

/// FNV-1a 64 building block for [`L2Book::state_digest`] implementations.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Fnv(pub u64);

impl Default for Fnv {
    fn default() -> Self {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv {
    #[inline]
    pub fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    #[inline]
    pub fn write_i64(&mut self, v: i64) {
        self.write(&v.to_le_bytes());
    }
}

/// Compute the canonical digest from ordered level iterators. Asks ascending
/// then bids descending, with side markers and the sync flag folded in.
pub(crate) fn digest_from_levels(
    asks_ascending: impl Iterator<Item = (i64, i64)>,
    bids_descending: impl Iterator<Item = (i64, i64)>,
    synced: bool,
) -> u64 {
    let mut h = Fnv::default();
    h.write(b"A");
    for (p, q) in asks_ascending {
        h.write_i64(p);
        h.write_i64(q);
    }
    h.write(b"B");
    for (p, q) in bids_descending {
        h.write_i64(p);
        h.write_i64(q);
    }
    h.write(&[u8::from(synced)]);
    h.0
}
