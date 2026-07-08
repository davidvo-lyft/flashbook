//! flashbook-lob: L2 order-book reconstruction engine (Phase 2).
//!
//! Two interchangeable price-level representations behind [`book::L2Book`]:
//! [`BTreeBook`] (ordered maps) and [`LadderBook`] (contiguous sorted vecs,
//! best-at-end). Cross-implementation property tests force identical
//! behavior; the replay benchmark picks the one that ships. [`BookSet`]
//! routes a multi-instrument event stream to per-instrument books.

pub mod book;
pub mod btree;
pub mod ladder;

use std::collections::HashMap;

pub use book::{Apply, L2Book};
pub use btree::BTreeBook;
pub use ladder::LadderBook;

use flashbook_proto::event::Event;

/// Routes events to per-instrument books, creating them on first sight.
#[derive(Debug)]
pub struct BookSet<B: L2Book> {
    books: HashMap<u32, B>,
    /// Per-side depth cap applied to newly created books (venue depth
    /// semantics, e.g. Kraken v2 book@depth=100). `None` = unlimited.
    default_max_depth: Option<usize>,
    make: fn(Option<usize>) -> B,
}

impl<B: L2Book> BookSet<B> {
    /// New set; `make` builds a book given the optional depth cap (use
    /// `|d| d.map_or_else(B::new, B::with_max_depth)` shape per impl).
    pub fn new(default_max_depth: Option<usize>, make: fn(Option<usize>) -> B) -> Self {
        Self {
            books: HashMap::new(),
            default_max_depth,
            make,
        }
    }

    /// Apply an event to its instrument's book.
    pub fn apply(&mut self, ev: &Event) -> Apply {
        let make = self.make;
        let depth = self.default_max_depth;
        self.books
            .entry(ev.instrument)
            .or_insert_with(|| make(depth))
            .apply(ev)
    }

    /// The book for an instrument, if any events have been seen.
    pub fn get(&self, instrument: u32) -> Option<&B> {
        self.books.get(&instrument)
    }

    /// Iterate (instrument, book).
    pub fn iter(&self) -> impl Iterator<Item = (u32, &B)> {
        self.books.iter().map(|(&k, v)| (k, v))
    }

    /// Number of instruments seen.
    pub fn len(&self) -> usize {
        self.books.len()
    }

    /// True if no instruments seen.
    pub fn is_empty(&self) -> bool {
        self.books.is_empty()
    }

    /// Deterministic digest across all books: fold per-instrument digests
    /// in ascending instrument order (replay's cross-run assertion).
    pub fn combined_digest(&self) -> u64 {
        let mut ids: Vec<u32> = self.books.keys().copied().collect();
        ids.sort_unstable();
        let mut h = book::Fnv::default();
        for id in ids {
            h.write(&id.to_le_bytes());
            h.write(&self.books[&id].state_digest().to_le_bytes());
        }
        h.0
    }
}
