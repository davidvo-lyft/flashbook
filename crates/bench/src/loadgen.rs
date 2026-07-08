//! Synthetic event load generation (deterministic, seeded) for bus and
//! store benchmarks where captured data would conflate producer cost with
//! the system under test. Real-data benchmarks (feed parse, LOB replay)
//! use the captured corpus instead — see BENCHMARKS.md per section.

use flashbook_proto::event::{Event, EventKind};

/// Deterministic event stream: SplitMix64-driven, book-shaped (clustered
/// prices, small quantities, mostly level sets with occasional trades).
#[derive(Debug, Clone)]
pub struct EventGen {
    state: u64,
    t_ns: u64,
    price: i64,
    seq: u64,
}

impl EventGen {
    /// Seeded generator; equal seeds produce identical streams.
    pub fn new(seed: u64) -> EventGen {
        EventGen {
            state: seed,
            t_ns: 1_000_000_000,
            price: 6_358_964_000_000,
            seq: 1,
        }
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        // SplitMix64
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Next synthetic event.
    #[inline]
    pub fn next_event(&mut self) -> Event {
        let x = self.next_u64();
        self.t_ns += 50_000 + (x % 400_000); // 50-450us cadence
        self.price += ((x >> 8) % 2001) as i64 - 1000; // random walk
        self.seq += 1;
        let kind = match x % 10 {
            0 => EventKind::Trade,
            1..=5 => EventKind::BidSet,
            _ => EventKind::AskSet,
        };
        Event {
            recv_mono_ns: self.t_ns,
            recv_wall_ns: self.t_ns + 1_700_000_000_000_000_000,
            venue_ts_ns: self.t_ns + 1_700_000_000_000_000_000 - 3_000_000,
            venue_seq: self.seq,
            price: self.price,
            qty: ((x >> 16) % 500_000_000) as i64,
            aux: self.seq,
            instrument: (x % 15) as u32 + 1,
            kind: kind as u8,
            venue: (x % 3) as u8 + 1,
            flags: 0,
            rsvd: 0,
        }
    }

    /// Fill a buffer with the next `n` events.
    pub fn fill(&mut self, n: usize, out: &mut Vec<Event>) {
        out.clear();
        out.reserve(n);
        for _ in 0..n {
            out.push(self.next_event());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_across_instances() {
        let mut a = EventGen::new(7);
        let mut b = EventGen::new(7);
        let (mut va, mut vb) = (Vec::new(), Vec::new());
        a.fill(1000, &mut va);
        b.fill(1000, &mut vb);
        assert_eq!(va, vb);
        let mut c = EventGen::new(8);
        let mut vc = Vec::new();
        c.fill(1000, &mut vc);
        assert_ne!(va, vc);
    }

    #[test]
    fn timestamps_monotone_and_kinds_valid() {
        let mut g = EventGen::new(1);
        let mut prev = 0;
        for _ in 0..10_000 {
            let e = g.next_event();
            assert!(e.recv_mono_ns > prev);
            prev = e.recv_mono_ns;
            assert!(e.kind().is_ok());
            assert!((1..=15).contains(&e.instrument));
        }
    }
}
