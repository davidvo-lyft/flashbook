//! [`BTreeBook`]: the ordered-map book representation. One `BTreeMap` per
//! side. Simple and algorithmically clean — the baseline the contiguous
//! ladder must beat (or lose to; the benchmark decides, see BENCHMARKS.md).

use std::collections::BTreeMap;

use flashbook_proto::event::{Event, Side};

use crate::book::{Apply, L2Book, LevelOps, SyncState, apply_common, digest_from_levels};

/// BTreeMap-backed L2 book.
#[derive(Debug, Default, Clone)]
pub struct BTreeBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    state: SyncState,
    dropped: u64,
    max_depth: Option<usize>,
}

impl BTreeBook {
    /// Unlimited-depth book.
    pub fn new() -> Self {
        Self::default()
    }

    /// Book maintained at `n` levels per side: after every mutation the
    /// worst levels beyond `n` are dropped (Kraken v2 depth semantics).
    pub fn with_max_depth(n: usize) -> Self {
        Self {
            max_depth: Some(n),
            ..Self::default()
        }
    }

    fn truncate(&mut self, side: Side) {
        let Some(n) = self.max_depth else { return };
        match side {
            Side::Bid => {
                while self.bids.len() > n {
                    self.bids.pop_first(); // worst bid = lowest price
                }
            }
            Side::Ask => {
                while self.asks.len() > n {
                    self.asks.pop_last(); // worst ask = highest price
                }
            }
        }
    }
}

impl LevelOps for BTreeBook {
    fn set_level(&mut self, side: Side, price: i64, qty: i64) -> bool {
        let before = match side {
            Side::Bid => self.bids.last_key_value().map(|(&p, &q)| (p, q)),
            Side::Ask => self.asks.first_key_value().map(|(&p, &q)| (p, q)),
        };
        let map = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if qty == 0 {
            map.remove(&price);
        } else {
            map.insert(price, qty);
        }
        self.truncate(side);
        let after = match side {
            Side::Bid => self.bids.last_key_value().map(|(&p, &q)| (p, q)),
            Side::Ask => self.asks.first_key_value().map(|(&p, &q)| (p, q)),
        };
        before != after
    }

    fn clear_levels(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

impl L2Book for BTreeBook {
    fn apply(&mut self, ev: &Event) -> Apply {
        let mut state = self.state;
        let mut dropped = self.dropped;
        let r = apply_common(self, &mut state, &mut dropped, ev);
        self.state = state;
        self.dropped = dropped;
        r
    }

    fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids.last_key_value().map(|(&p, &q)| (p, q))
    }

    fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks.first_key_value().map(|(&p, &q)| (p, q))
    }

    fn depth(&self, side: Side) -> usize {
        match side {
            Side::Bid => self.bids.len(),
            Side::Ask => self.asks.len(),
        }
    }

    fn top_n_into(&self, side: Side, n: usize, out: &mut Vec<(i64, i64)>) {
        out.clear();
        match side {
            Side::Bid => out.extend(self.bids.iter().rev().take(n).map(|(&p, &q)| (p, q))),
            Side::Ask => out.extend(self.asks.iter().take(n).map(|(&p, &q)| (p, q))),
        }
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
            self.asks.iter().map(|(&p, &q)| (p, q)),
            self.bids.iter().rev().map(|(&p, &q)| (p, q)),
            self.state == SyncState::Synced,
        )
    }
}
