//! [`LadderBook`]: the contiguous book representation. One sorted `Vec` per
//! side with the BEST level at the END:
//!
//! - bids stored ascending by price  -> best bid (highest) is `last()`
//! - asks stored descending by price -> best ask (lowest)  is `last()`
//!
//! Real L2 traffic clusters near the top of the book, so insert/remove
//! memmoves touch only the short tail, and best-of-book reads are O(1) with
//! no pointer chasing. The BTreeMap alternative is [`crate::BTreeBook`];
//! the replay benchmark decides which ships.

use flashbook_proto::event::{Event, Side};

use crate::book::{Apply, L2Book, LevelOps, SyncState, apply_common, digest_from_levels};

/// Contiguous sorted-vec L2 book.
#[derive(Debug, Default, Clone)]
pub struct LadderBook {
    /// Ascending price; best bid at end.
    bids: Vec<(i64, i64)>,
    /// Descending price; best ask at end.
    asks: Vec<(i64, i64)>,
    state: SyncState,
    dropped: u64,
    max_depth: Option<usize>,
}

impl LadderBook {
    /// Unlimited-depth book.
    pub fn new() -> Self {
        Self::default()
    }

    /// Book maintained at `n` levels per side (Kraken v2 depth semantics):
    /// after every mutation the worst levels beyond `n` are dropped.
    pub fn with_max_depth(n: usize) -> Self {
        Self {
            max_depth: Some(n),
            ..Self::default()
        }
    }

    /// Binary-search a side for `price`. `Ok(idx)` = present.
    #[inline]
    fn find(levels: &[(i64, i64)], side: Side, price: i64) -> Result<usize, usize> {
        match side {
            // bids ascending
            Side::Bid => levels.binary_search_by(|probe| probe.0.cmp(&price)),
            // asks descending
            Side::Ask => levels.binary_search_by(|probe| price.cmp(&probe.0)),
        }
    }

    fn truncate(&mut self, side: Side) {
        let Some(n) = self.max_depth else { return };
        // Worst levels live at the FRONT of each vec (best is at the end).
        let v = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if v.len() > n {
            v.drain(..v.len() - n);
        }
    }
}

impl LevelOps for LadderBook {
    fn set_level(&mut self, side: Side, price: i64, qty: i64) -> bool {
        let v = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let before = v.last().copied();
        match Self::find(v, side, price) {
            Ok(idx) => {
                if qty == 0 {
                    v.remove(idx);
                } else {
                    v[idx].1 = qty;
                }
            }
            Err(idx) => {
                if qty != 0 {
                    v.insert(idx, (price, qty));
                }
            }
        }
        self.truncate(side);
        let v = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        before != v.last().copied()
    }

    fn clear_levels(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

impl L2Book for LadderBook {
    fn apply(&mut self, ev: &Event) -> Apply {
        let mut state = self.state;
        let mut dropped = self.dropped;
        let r = apply_common(self, &mut state, &mut dropped, ev);
        self.state = state;
        self.dropped = dropped;
        r
    }

    fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids.last().copied()
    }

    fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks.last().copied()
    }

    fn depth(&self, side: Side) -> usize {
        match side {
            Side::Bid => self.bids.len(),
            Side::Ask => self.asks.len(),
        }
    }

    fn top_n_into(&self, side: Side, n: usize, out: &mut Vec<(i64, i64)>) {
        out.clear();
        let v = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        // best at end; walking backwards yields bids descending / asks ascending
        out.extend(v.iter().rev().take(n).copied());
    }

    fn clear(&mut self) {
        self.clear_levels();
        self.state = SyncState::Unsynced;
    }

    fn is_synced(&self) -> bool {
        self.state == SyncState::Synced
    }

    fn dropped_unsynced(&self) -> u64 {
        self.dropped
    }

    fn state_digest(&self) -> u64 {
        digest_from_levels(
            self.asks.iter().rev().copied(),
            self.bids.iter().rev().copied(),
            self.state == SyncState::Synced,
        )
    }
}
