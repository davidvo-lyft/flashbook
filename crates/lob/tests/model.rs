//! Cross-implementation and reference-model property tests: BTreeBook and
//! LadderBook must behave identically to each other and to a trivially-
//! correct model under arbitrary event sequences — including depth-capped
//! (Kraken-style) books. This equivalence is what lets the benchmark pick a
//! representation purely on speed.

use std::collections::BTreeMap;

use flashbook_lob::book::Apply;
use flashbook_lob::{BTreeBook, BookSet, L2Book, LadderBook};
use flashbook_proto::event::{Event, EventKind, Side};
use proptest::prelude::*;

/// Trivially-correct reference: BTreeMaps + the same sync rules, truncation
/// done by re-sorting. Kept intentionally naive.
#[derive(Default)]
struct Model {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    synced: bool,
    filling: bool,
    max_depth: Option<usize>,
}

impl Model {
    fn with_max_depth(n: usize) -> Self {
        Model {
            max_depth: Some(n),
            ..Model::default()
        }
    }

    fn truncate(&mut self) {
        if let Some(n) = self.max_depth {
            while self.bids.len() > n {
                let &worst = self.bids.keys().next().unwrap();
                self.bids.remove(&worst);
            }
            while self.asks.len() > n {
                let &worst = self.asks.keys().next_back().unwrap();
                self.asks.remove(&worst);
            }
        }
    }

    fn apply(&mut self, ev: &Event) {
        match ev.kind().unwrap() {
            EventKind::BidSet | EventKind::AskSet => {
                if !self.synced {
                    return;
                }
                let side = if ev.kind == EventKind::BidSet as u8 {
                    &mut self.bids
                } else {
                    &mut self.asks
                };
                if ev.qty == 0 {
                    side.remove(&ev.price);
                } else {
                    side.insert(ev.price, ev.qty);
                }
                self.truncate();
            }
            EventKind::SnapBegin => {
                self.bids.clear();
                self.asks.clear();
                self.filling = true;
                self.synced = false;
            }
            EventKind::SnapBid | EventKind::SnapAsk => {
                if !self.filling {
                    return;
                }
                let side = if ev.kind == EventKind::SnapBid as u8 {
                    &mut self.bids
                } else {
                    &mut self.asks
                };
                if ev.qty == 0 {
                    side.remove(&ev.price);
                } else {
                    side.insert(ev.price, ev.qty);
                }
                self.truncate();
            }
            EventKind::SnapEnd => {
                if self.filling {
                    self.filling = false;
                    self.synced = true;
                }
            }
            EventKind::Clear => {
                self.bids.clear();
                self.asks.clear();
                self.synced = false;
                self.filling = false;
            }
            EventKind::Gap => {
                self.synced = false;
                self.filling = false;
            }
            _ => {}
        }
    }

    fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids.last_key_value().map(|(&p, &q)| (p, q))
    }

    fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks.first_key_value().map(|(&p, &q)| (p, q))
    }
}

fn ev(kind: EventKind, price: i64, qty: i64) -> Event {
    Event {
        price,
        qty,
        kind: kind as u8,
        venue: 3,
        instrument: 11,
        ..Event::ZERO
    }
}

/// Operations generator: mostly sets, some snapshots/clears/gaps, prices in
/// a narrow band to force collisions/replacements/deletions.
fn op_strategy() -> impl Strategy<Value = Event> {
    prop_oneof![
        8 => (prop_oneof![Just(EventKind::BidSet), Just(EventKind::AskSet)],
              1i64..40, 0i64..5)
            .prop_map(|(k, p, q)| ev(k, p * 100, q * 7)),
        1 => Just(ev(EventKind::Clear, 0, 0)),
        1 => Just(ev(EventKind::Gap, 0, 0)),
        1 => Just(ev(EventKind::Trade, 500, 1)),
    ]
}

/// A full snapshot block for injection mid-sequence.
fn snapshot_block() -> impl Strategy<Value = Vec<Event>> {
    (
        proptest::collection::btree_map(1i64..40, 1i64..5, 0..12),
        proptest::collection::btree_map(1i64..40, 1i64..5, 0..12),
    )
        .prop_map(|(bids, asks)| {
            let mut evs = vec![ev(
                EventKind::SnapBegin,
                0,
                i64::try_from(bids.len() + asks.len()).unwrap(),
            )];
            for (&p, &q) in bids.iter().rev() {
                evs.push(ev(EventKind::SnapBid, p * 100, q * 7));
            }
            for (&p, &q) in &asks {
                evs.push(ev(EventKind::SnapAsk, p * 100, q * 7));
            }
            evs.push(ev(EventKind::SnapEnd, 0, 0));
            evs
        })
}

fn sequence() -> impl Strategy<Value = Vec<Event>> {
    proptest::collection::vec(
        prop_oneof![
            6 => op_strategy().prop_map(|e| vec![e]),
            1 => snapshot_block(),
        ],
        0..60,
    )
    .prop_map(|chunks| chunks.into_iter().flatten().collect())
}

fn assert_equivalent(bt: &BTreeBook, ld: &LadderBook, model: &Model, ctx: &str) {
    assert_eq!(
        bt.best_bid(),
        model.best_bid(),
        "{ctx}: btree best_bid vs model"
    );
    assert_eq!(
        ld.best_bid(),
        model.best_bid(),
        "{ctx}: ladder best_bid vs model"
    );
    assert_eq!(
        bt.best_ask(),
        model.best_ask(),
        "{ctx}: btree best_ask vs model"
    );
    assert_eq!(
        ld.best_ask(),
        model.best_ask(),
        "{ctx}: ladder best_ask vs model"
    );
    assert_eq!(
        bt.depth(Side::Bid),
        model.bids.len(),
        "{ctx}: btree bid depth"
    );
    assert_eq!(
        ld.depth(Side::Bid),
        model.bids.len(),
        "{ctx}: ladder bid depth"
    );
    assert_eq!(
        bt.depth(Side::Ask),
        model.asks.len(),
        "{ctx}: btree ask depth"
    );
    assert_eq!(
        ld.depth(Side::Ask),
        model.asks.len(),
        "{ctx}: ladder ask depth"
    );
    assert_eq!(bt.is_synced(), model.synced, "{ctx}: btree synced");
    assert_eq!(ld.is_synced(), model.synced, "{ctx}: ladder synced");
    assert_eq!(
        bt.state_digest(),
        ld.state_digest(),
        "{ctx}: digests diverge"
    );
    let (mut a, mut b) = (Vec::new(), Vec::new());
    bt.top_n_into(Side::Bid, 10, &mut a);
    ld.top_n_into(Side::Bid, 10, &mut b);
    assert_eq!(a, b, "{ctx}: top10 bids");
    let want: Vec<(i64, i64)> = model
        .bids
        .iter()
        .rev()
        .take(10)
        .map(|(&p, &q)| (p, q))
        .collect();
    assert_eq!(a, want, "{ctx}: top10 bids vs model");
    bt.top_n_into(Side::Ask, 10, &mut a);
    ld.top_n_into(Side::Ask, 10, &mut b);
    assert_eq!(a, b, "{ctx}: top10 asks");
    let want: Vec<(i64, i64)> = model.asks.iter().take(10).map(|(&p, &q)| (p, q)).collect();
    assert_eq!(a, want, "{ctx}: top10 asks vs model");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn implementations_match_model(seq in sequence()) {
        let mut bt = BTreeBook::new();
        let mut ld = LadderBook::new();
        let mut model = Model::default();
        for (i, e) in seq.iter().enumerate() {
            let ra = bt.apply(e);
            let rb = ld.apply(e);
            prop_assert_eq!(ra, rb, "apply results diverge at {}", i);
            model.apply(e);
        }
        assert_equivalent(&bt, &ld, &model, "end");
    }

    #[test]
    fn depth_capped_books_match(seq in sequence(), cap in 1usize..8) {
        let mut bt = BTreeBook::with_max_depth(cap);
        let mut ld = LadderBook::with_max_depth(cap);
        let mut model = Model::with_max_depth(cap);
        for e in &seq {
            bt.apply(e);
            ld.apply(e);
            model.apply(e);
        }
        assert_equivalent(&bt, &ld, &model, "end");
        prop_assert!(bt.depth(Side::Bid) <= cap);
        prop_assert!(bt.depth(Side::Ask) <= cap);
    }

    #[test]
    fn same_sequence_same_digest(seq in sequence()) {
        let mut a = LadderBook::new();
        let mut b = LadderBook::new();
        for e in &seq {
            a.apply(e);
        }
        for e in &seq {
            b.apply(e);
        }
        prop_assert_eq!(a.state_digest(), b.state_digest());
    }
}

fn snap(levels_bid: &[(i64, i64)], levels_ask: &[(i64, i64)]) -> Vec<Event> {
    let mut v = vec![ev(
        EventKind::SnapBegin,
        0,
        i64::try_from(levels_bid.len() + levels_ask.len()).unwrap(),
    )];
    for &(p, q) in levels_bid {
        v.push(ev(EventKind::SnapBid, p, q));
    }
    for &(p, q) in levels_ask {
        v.push(ev(EventKind::SnapAsk, p, q));
    }
    v.push(ev(EventKind::SnapEnd, 0, 0));
    v
}

#[test]
fn unsynced_updates_are_dropped_and_counted() {
    let mut b = LadderBook::new();
    assert_eq!(
        b.apply(&ev(EventKind::BidSet, 100, 5)),
        Apply::DroppedUnsynced
    );
    assert_eq!(b.dropped_unsynced(), 1);
    assert_eq!(b.best_bid(), None);
    for e in snap(&[(100, 5)], &[(101, 7)]) {
        b.apply(&e);
    }
    assert!(b.is_synced());
    assert_eq!(
        b.apply(&ev(EventKind::BidSet, 99, 3)),
        Apply::Mutated { top_changed: false }
    );
    assert_eq!(b.best_bid(), Some((100, 5)));
}

#[test]
fn snapshot_lifecycle_and_checksum_signals() {
    let mut b = BTreeBook::new();
    let mut events = snap(&[(100, 5), (99, 4)], &[(101, 7)]);
    let end = events.pop().unwrap();
    let mut end_with_crc = end;
    end_with_crc.aux = 0xDEAD_BEEF;
    for e in &events {
        b.apply(e);
    }
    assert!(!b.is_synced());
    assert_eq!(
        b.apply(&end_with_crc),
        Apply::SnapshotComplete {
            checksum: 0xDEAD_BEEF
        }
    );
    assert!(b.is_synced());
    let mut crc_ev = ev(EventKind::Checksum, 0, 0);
    crc_ev.aux = 42;
    assert_eq!(b.apply(&crc_ev), Apply::ChecksumToVerify { crc: 42 });
    assert_eq!(b.apply(&ev(EventKind::Gap, 0, 0)), Apply::GapMarked);
    assert!(!b.is_synced());
    // book retains levels after a gap (stale but present) until a snapshot
    assert_eq!(b.best_bid(), Some((100, 5)));
    assert_eq!(
        b.apply(&ev(EventKind::BidSet, 100, 9)),
        Apply::DroppedUnsynced
    );
}

#[test]
fn delete_semantics() {
    let mut b = LadderBook::new();
    for e in snap(&[(100, 5), (99, 4), (98, 3)], &[(101, 7), (102, 8)]) {
        b.apply(&e);
    }
    // delete best bid -> top changes
    assert_eq!(
        b.apply(&ev(EventKind::BidSet, 100, 0)),
        Apply::Mutated { top_changed: true }
    );
    assert_eq!(b.best_bid(), Some((99, 4)));
    // delete absent level -> no-op, top unchanged
    assert_eq!(
        b.apply(&ev(EventKind::BidSet, 55, 0)),
        Apply::Mutated { top_changed: false }
    );
    assert_eq!(b.depth(Side::Bid), 2);
    // replace qty at best ask -> top changes (qty component)
    assert_eq!(
        b.apply(&ev(EventKind::AskSet, 101, 1)),
        Apply::Mutated { top_changed: true }
    );
    assert_eq!(b.best_ask(), Some((101, 1)));
    // deep ask update -> top unchanged
    assert_eq!(
        b.apply(&ev(EventKind::AskSet, 102, 2)),
        Apply::Mutated { top_changed: false }
    );
}

#[test]
fn trades_and_heartbeats_do_not_touch_books() {
    let mut b = BTreeBook::new();
    for e in snap(&[(100, 5)], &[(101, 7)]) {
        b.apply(&e);
    }
    let d = b.state_digest();
    assert_eq!(b.apply(&ev(EventKind::Trade, 100, 1)), Apply::NotBook);
    assert_eq!(b.apply(&ev(EventKind::Heartbeat, 0, 0)), Apply::NotBook);
    assert_eq!(b.state_digest(), d);
}

#[test]
fn digest_reflects_any_level_change() {
    let mut a = LadderBook::new();
    let mut b = LadderBook::new();
    for e in snap(&[(100, 5), (99, 4)], &[(101, 7)]) {
        a.apply(&e);
        b.apply(&e);
    }
    assert_eq!(a.state_digest(), b.state_digest());
    b.apply(&ev(EventKind::BidSet, 99, 5));
    assert_ne!(a.state_digest(), b.state_digest());
}

#[test]
fn bookset_routes_and_digests() {
    let mut set: BookSet<LadderBook> = BookSet::new(Some(100), |d| {
        d.map_or_else(LadderBook::new, LadderBook::with_max_depth)
    });
    let mut e1 = ev(EventKind::SnapBegin, 0, 0);
    e1.instrument = 11;
    let mut e2 = ev(EventKind::SnapEnd, 0, 0);
    e2.instrument = 11;
    set.apply(&e1);
    set.apply(&e2);
    let mut e3 = ev(EventKind::BidSet, 100, 5);
    e3.instrument = 11;
    set.apply(&e3);
    let mut other = ev(EventKind::BidSet, 100, 5);
    other.instrument = 12;
    assert_eq!(set.apply(&other), Apply::DroppedUnsynced); // 12 never snapshotted
    assert_eq!(set.len(), 2);
    assert!(set.get(11).unwrap().is_synced());
    assert!(!set.get(12).unwrap().is_synced());
    let d1 = set.combined_digest();
    let mut e4 = ev(EventKind::BidSet, 101, 5);
    e4.instrument = 11;
    set.apply(&e4);
    assert_ne!(set.combined_digest(), d1);
}

#[test]
fn max_depth_truncates_worst_not_best() {
    let mut b = LadderBook::with_max_depth(2);
    for e in snap(
        &[(100, 1), (99, 1), (98, 1)],
        &[(101, 1), (102, 1), (103, 1)],
    ) {
        b.apply(&e);
    }
    assert_eq!(b.depth(Side::Bid), 2);
    assert_eq!(b.depth(Side::Ask), 2);
    assert_eq!(b.best_bid(), Some((100, 1)));
    assert_eq!(b.best_ask(), Some((101, 1)));
    let mut top = Vec::new();
    b.top_n_into(Side::Bid, 10, &mut top);
    assert_eq!(top, vec![(100, 1), (99, 1)]);
    b.top_n_into(Side::Ask, 10, &mut top);
    assert_eq!(top, vec![(101, 1), (102, 1)]);
}
