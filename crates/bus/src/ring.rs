//! Hand-rolled broadcast ring: one producer, any number of independent
//! consumers, each receiving EVERY event. Bounded memory: the producer
//! never blocks and never allocates; a consumer that falls more than
//! `capacity` behind is overrun — it finds out (with a loss count) and
//! jumps forward. Those are market-data semantics: a slow subscriber loses
//! ticks and knows it; it cannot stall the feed.
//!
//! # Soundness
//!
//! Slots hold the 64-byte [`Event`] as `[AtomicU64; 8]` (relaxed loads and
//! stores — never a non-atomic data race) guarded by a per-slot version
//! counter using exactly the seqlock protocol from `crossbeam-utils`'
//! `SeqLock`: writer does `version += 1` (odd = writing), `fence(Release)`,
//! data stores, `version += 1` with `Release`; reader does
//! `version.load(Acquire)`, data loads, `fence(Acquire)`,
//! `version.load(Relaxed)` and accepts only if both version reads agree on
//! the same even value. Global sequence `s` lands in slot `s % capacity`;
//! after it is published that slot's version is exactly
//! `2 * (s / capacity + 1)`, so a consumer can classify a slot as
//! not-yet-written / ready / overwritten from the version alone.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering, fence};

use crossbeam_utils::CachePadded;
use flashbook_proto::Event;

/// Number of u64 words in one event.
const WORDS: usize = 8;

const _: () = assert!(size_of::<Event>() == WORDS * 8);

struct Slot {
    /// Seqlock version: odd while the producer is writing; after global
    /// sequence `s` is published here, equals `2 * (s / capacity + 1)`.
    version: AtomicU64,
    data: [AtomicU64; WORDS],
}

impl Slot {
    fn new() -> Slot {
        Slot {
            version: AtomicU64::new(0),
            data: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Shared state behind producer and consumer handles.
pub struct Ring {
    slots: Box<[CachePadded<Slot>]>,
    mask: u64,
    shift: u32,
    /// Next sequence the producer will publish (== count published so far).
    head: CachePadded<AtomicU64>,
}

impl Ring {
    /// Expected slot version once global sequence `s` has been published.
    #[inline]
    fn expected_version(&self, s: u64) -> u64 {
        ((s >> self.shift) + 1) << 1
    }
}

/// Create a broadcast ring with `capacity` slots (rounded up to a power of
/// two, minimum 2). Returns the producer handle; consumers subscribe via
/// [`Producer::subscribe`] (or [`subscribe_ring`] on a shared [`Arc<Ring>`]).
pub fn ring(capacity: usize) -> Producer {
    let cap = capacity.max(2).next_power_of_two();
    let ring = Arc::new(Ring {
        slots: (0..cap).map(|_| CachePadded::new(Slot::new())).collect(),
        mask: cap as u64 - 1,
        shift: cap.trailing_zeros(),
        head: CachePadded::new(AtomicU64::new(0)),
    });
    Producer { ring }
}

/// The unique producer handle (not Clone: single-writer protocol).
pub struct Producer {
    ring: Arc<Ring>,
}

// SAFETY: all slot access is through atomics; the seqlock protocol provides
// consistency. The producer is unique by construction (no Clone).
unsafe impl Send for Producer {}

impl Producer {
    /// Publish one event. Never blocks, never allocates; overwrites the
    /// oldest slot when full. Returns the event's global sequence.
    #[inline]
    pub fn publish(&mut self, ev: &Event) -> u64 {
        let s = self.ring.head.load(Ordering::Relaxed);
        let slot = &self.ring.slots[(s & self.ring.mask) as usize];
        let words: [u64; WORDS] = bytemuck::cast(*ev);

        // seqlock write (crossbeam-utils SeqLock recipe)
        let v = slot.version.load(Ordering::Relaxed);
        slot.version.store(v + 1, Ordering::Relaxed);
        fence(Ordering::Release);
        for (d, w) in slot.data.iter().zip(words) {
            d.store(w, Ordering::Relaxed);
        }
        slot.version.store(v + 2, Ordering::Release);

        // publish the new head AFTER the slot (consumers gate on versions,
        // head is only advisory for lag math)
        self.ring.head.store(s + 1, Ordering::Release);
        s
    }

    /// Number of events published so far.
    pub fn published(&self) -> u64 {
        self.ring.head.load(Ordering::Acquire)
    }

    /// New consumer starting at the CURRENT head (sees only future events).
    pub fn subscribe(&self) -> Consumer {
        subscribe_ring(&self.ring)
    }

    /// Ring capacity (power of two).
    pub fn capacity(&self) -> usize {
        self.ring.slots.len()
    }

    /// Shared ring for out-of-band subscription.
    pub fn ring_arc(&self) -> Arc<Ring> {
        Arc::clone(&self.ring)
    }
}

/// Subscribe on a shared ring at the current head.
pub fn subscribe_ring(ring: &Arc<Ring>) -> Consumer {
    Consumer {
        ring: Arc::clone(ring),
        next: ring.head.load(Ordering::Acquire),
    }
}

/// Outcome of a consumer poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recv {
    /// One event, in order.
    Event(Event),
    /// Nothing new yet.
    Empty,
    /// Fell behind: `lost` events were overwritten; the cursor has jumped
    /// forward to the oldest still-available event.
    Lagged {
        /// Events irrecoverably missed.
        lost: u64,
    },
}

/// An independent consumer cursor. Cheap to create; does not affect the
/// producer or other consumers.
pub struct Consumer {
    ring: Arc<Ring>,
    next: u64,
}

// SAFETY: same as Producer — atomics only.
unsafe impl Send for Consumer {}

impl Consumer {
    /// Poll for the next event (non-blocking).
    #[inline]
    pub fn try_next(&mut self) -> Recv {
        let s = self.next;
        let ring = &self.ring;
        let slot = &ring.slots[(s & ring.mask) as usize];
        let want = ring.expected_version(s);

        let v1 = slot.version.load(Ordering::Acquire);
        if v1 < want {
            // slot not yet written for our sequence (or mid-write of it)
            return Recv::Empty;
        }
        if v1 == want {
            let mut words = [0u64; WORDS];
            for (w, d) in words.iter_mut().zip(&slot.data) {
                *w = d.load(Ordering::Relaxed);
            }
            fence(Ordering::Acquire);
            let v2 = slot.version.load(Ordering::Relaxed);
            if v2 == want {
                self.next = s + 1;
                return Recv::Event(bytemuck::cast(words));
            }
            // overwritten while copying: fall through to lag handling
        }
        // v1 > want (or torn): we were lapped. Jump to the oldest sequence
        // that is still safely available.
        let head = ring.head.load(Ordering::Acquire);
        let cap = ring.mask + 1;
        let oldest = head.saturating_sub(cap);
        // Everything in [s, oldest) is gone. (head may still be racing
        // forward, but `lost` is what we can prove lost right now.)
        let lost = oldest.saturating_sub(s).max(1);
        self.next = s + lost;
        Recv::Lagged { lost }
    }

    /// The next sequence this consumer expects.
    pub fn cursor(&self) -> u64 {
        self.next
    }

    /// How far behind the producer this consumer is right now.
    pub fn lag(&self) -> u64 {
        self.ring
            .head
            .load(Ordering::Acquire)
            .saturating_sub(self.next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flashbook_proto::event::EventKind;

    /// Event whose every field is derived from `s` — torn copies become
    /// visible as broken relationships.
    fn ev(s: u64) -> Event {
        Event {
            recv_mono_ns: s,
            recv_wall_ns: s.wrapping_mul(3),
            venue_ts_ns: s.wrapping_mul(5),
            venue_seq: s,
            #[allow(clippy::cast_possible_wrap)]
            price: s.wrapping_mul(7) as i64,
            #[allow(clippy::cast_possible_wrap)]
            qty: s.wrapping_mul(11) as i64,
            aux: s.wrapping_mul(13),
            instrument: 1,
            kind: EventKind::Trade as u8,
            venue: 1,
            flags: 0,
            rsvd: 0,
        }
    }

    fn check(e: &Event) {
        let s = e.recv_mono_ns;
        assert_eq!(e.recv_wall_ns, s.wrapping_mul(3), "torn read detected");
        assert_eq!(e.venue_ts_ns, s.wrapping_mul(5), "torn read detected");
        assert_eq!(e.venue_seq, s);
        assert_eq!(e.aux, s.wrapping_mul(13), "torn read detected");
    }

    #[test]
    fn single_thread_fifo() {
        let mut p = ring(8);
        let mut c = p.subscribe();
        assert_eq!(c.try_next(), Recv::Empty);
        for i in 0..5 {
            p.publish(&ev(i));
        }
        for i in 0..5 {
            match c.try_next() {
                Recv::Event(e) => assert_eq!(e.recv_mono_ns, i),
                other => panic!("expected event {i}, got {other:?}"),
            }
        }
        assert_eq!(c.try_next(), Recv::Empty);
    }

    #[test]
    fn capacity_rounds_to_power_of_two() {
        let p = ring(100);
        assert_eq!(p.capacity(), 128);
        let p = ring(0);
        assert_eq!(p.capacity(), 2);
    }

    #[test]
    fn overrun_reports_loss_and_recovers() {
        let mut p = ring(4); // cap 4
        let mut c = p.subscribe();
        for i in 0..10 {
            p.publish(&ev(i));
        }
        // consumer expected seq 0; slots now hold 6..=9
        match c.try_next() {
            Recv::Lagged { lost } => assert_eq!(lost, 6, "seqs 0..6 overwritten"),
            other => panic!("expected lag, got {other:?}"),
        }
        // then reads 6..=9 in order
        for i in 6..10 {
            match c.try_next() {
                Recv::Event(e) => {
                    check(&e);
                    assert_eq!(e.recv_mono_ns, i);
                }
                other => panic!("expected event {i}, got {other:?}"),
            }
        }
        assert_eq!(c.try_next(), Recv::Empty);
        assert_eq!(c.lag(), 0);
    }

    #[test]
    fn late_subscriber_sees_only_future() {
        let mut p = ring(8);
        for i in 0..5 {
            p.publish(&ev(i));
        }
        let mut c = p.subscribe();
        assert_eq!(c.try_next(), Recv::Empty);
        p.publish(&ev(100));
        match c.try_next() {
            Recv::Event(e) => assert_eq!(e.recv_mono_ns, 100),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn multi_consumer_independence() {
        let mut p = ring(16);
        let mut c1 = p.subscribe();
        let mut c2 = p.subscribe();
        for i in 0..8 {
            p.publish(&ev(i));
        }
        for i in 0..8 {
            assert!(matches!(c1.try_next(), Recv::Event(e) if e.recv_mono_ns == i));
        }
        // c2 unaffected by c1's progress
        for i in 0..8 {
            assert!(matches!(c2.try_next(), Recv::Event(e) if e.recv_mono_ns == i));
        }
    }

    /// Stress: one producer, several consumers on threads; every received
    /// event must be internally consistent (no torn reads), sequences must
    /// be strictly increasing per consumer, and received+lost must equal
    /// the total published.
    #[test]
    fn concurrent_stress_no_torn_reads() {
        const N: u64 = 200_000;
        const CONSUMERS: usize = 4;
        let mut p = ring(1024);
        let ring_arc = p.ring_arc();

        let consumers: Vec<_> = (0..CONSUMERS)
            .map(|_| {
                let mut c = subscribe_ring(&ring_arc);
                std::thread::spawn(move || {
                    let mut got: u64 = 0;
                    let mut lost: u64 = 0;
                    let mut last: Option<u64> = None;
                    loop {
                        match c.try_next() {
                            Recv::Event(e) => {
                                check(&e);
                                if e.recv_mono_ns == u64::MAX {
                                    break; // poison pill
                                }
                                if let Some(l) = last {
                                    assert!(e.recv_mono_ns > l, "regressed seq");
                                }
                                last = Some(e.recv_mono_ns);
                                got += 1;
                            }
                            Recv::Lagged { lost: l } => lost += l,
                            Recv::Empty => std::hint::spin_loop(),
                        }
                    }
                    (got, lost)
                })
            })
            .collect();

        for i in 0..N {
            p.publish(&ev(i));
            if i % 64 == 0 {
                std::hint::spin_loop();
            }
        }
        // poison pill, repeated so even badly lagged consumers see it
        for _ in 0..p.capacity() {
            p.publish(&ev(u64::MAX));
        }

        for h in consumers {
            let (got, lost) = h.join().unwrap();
            // received + provably-lost may undercount pills but must cover N
            assert!(got + lost >= N, "got {got} + lost {lost} < {N}");
            assert!(got > 0, "consumer starved entirely");
        }
    }
}
