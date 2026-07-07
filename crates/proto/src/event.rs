//! The normalized wire event: one fixed 64-byte `#[repr(C)]` POD record
//! (exactly one cache line), zero-copy readable via `bytemuck` (D-004).
//!
//! Book snapshots are encoded as bracketed runs of the same record
//! (`SnapBegin`, N x `SnapBid`/`SnapAsk`, `SnapEnd`) so every consumer
//! handles exactly one record shape.

use bytemuck::{Pod, Zeroable};

/// Venue identifiers (stable on the wire; never renumber).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum Venue {
    /// Coinbase Exchange (ws-feed.exchange.coinbase.com)
    Coinbase = 1,
    /// Binance spot (stream.binance.com)
    Binance = 2,
    /// Kraken spot v2 (ws.kraken.com/v2)
    Kraken = 3,
}

impl Venue {
    /// All venues, in wire order.
    pub const ALL: [Venue; 3] = [Venue::Coinbase, Venue::Binance, Venue::Kraken];

    /// Lowercase stable name (used in file names, stats keys).
    pub fn name(self) -> &'static str {
        match self {
            Venue::Coinbase => "coinbase",
            Venue::Binance => "binance",
            Venue::Kraken => "kraken",
        }
    }
}

impl TryFrom<u8> for Venue {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, u8> {
        match v {
            1 => Ok(Venue::Coinbase),
            2 => Ok(Venue::Binance),
            3 => Ok(Venue::Kraken),
            other => Err(other),
        }
    }
}

/// Book side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    /// Buy side.
    Bid,
    /// Sell side.
    Ask,
}

/// Event kinds (stable on the wire; never renumber).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EventKind {
    /// Set absolute quantity at a bid price level (qty 0 removes the level).
    BidSet = 1,
    /// Set absolute quantity at an ask price level (qty 0 removes the level).
    AskSet = 2,
    /// A trade print. `aux` = venue trade id (0 if none);
    /// flag [`flags::TAKER_SELL`] set when the aggressor sold.
    Trade = 3,
    /// Start of a book snapshot; consumers should clear the book.
    /// `aux` = level count if known in advance, else 0.
    SnapBegin = 4,
    /// One bid level of a snapshot.
    SnapBid = 5,
    /// One ask level of a snapshot.
    SnapAsk = 6,
    /// End of a book snapshot. `aux` = venue checksum if provided, else 0.
    SnapEnd = 7,
    /// Venue-provided book checksum to verify against (Kraken CRC32).
    /// `aux` = checksum value.
    Checksum = 8,
    /// Venue heartbeat / liveness marker.
    Heartbeat = 9,
    /// Sequence gap detected by the feed handler. `aux` = number of missed
    /// messages if computable, else 0. A resync (snapshot) follows.
    Gap = 10,
    /// Book must be cleared (reconnect/resync start).
    Clear = 11,
}

impl TryFrom<u8> for EventKind {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, u8> {
        Ok(match v {
            1 => EventKind::BidSet,
            2 => EventKind::AskSet,
            3 => EventKind::Trade,
            4 => EventKind::SnapBegin,
            5 => EventKind::SnapBid,
            6 => EventKind::SnapAsk,
            7 => EventKind::SnapEnd,
            8 => EventKind::Checksum,
            9 => EventKind::Heartbeat,
            10 => EventKind::Gap,
            11 => EventKind::Clear,
            other => return Err(other),
        })
    }
}

/// Bit flags for [`Event::flags`].
pub mod flags {
    /// Trade aggressor was a seller.
    pub const TAKER_SELL: u8 = 1;
    /// Event was derived from a snapshot (vs incremental delta).
    pub const FROM_SNAPSHOT: u8 = 1 << 1;
    /// Event synthesized locally (e.g. from a REST poll), not venue-pushed.
    pub const SYNTHETIC: u8 = 1 << 2;
}

/// The normalized event. Exactly 64 bytes, `#[repr(C)]`, no padding.
///
/// Prices and quantities are i64 mantissas at the global 1e-8 scale
/// ([`crate::fixed::SCALE`]). Timestamps are nanoseconds: `recv_mono_ns` from
/// the process monotonic clock (ordering/latency math), `recv_wall_ns` UNIX
/// wall time (cross-process alignment), `venue_ts_ns` the venue's own event
/// time (0 if the venue provides none for this message).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct Event {
    /// Local monotonic receive timestamp (ns since process clock anchor).
    pub recv_mono_ns: u64,
    /// Local wall-clock receive timestamp (ns since UNIX epoch).
    pub recv_wall_ns: u64,
    /// Venue event timestamp (ns since UNIX epoch; 0 if not provided).
    pub venue_ts_ns: u64,
    /// Venue sequence / update id (0 if not provided).
    pub venue_seq: u64,
    /// Price mantissa at 1e-8 (0 when meaningless for the kind).
    pub price: i64,
    /// Quantity mantissa at 1e-8 (absolute level qty for book events;
    /// trade size for trades; 0 when meaningless).
    pub qty: i64,
    /// Kind-specific auxiliary value (trade id, checksum, level count, gap size).
    pub aux: u64,
    /// Instrument id from the [`crate::instrument::Registry`].
    pub instrument: u32,
    /// [`EventKind`] as u8.
    pub kind: u8,
    /// [`Venue`] as u8.
    pub venue: u8,
    /// Bit flags, see [`flags`].
    pub flags: u8,
    /// Reserved; must be 0.
    pub rsvd: u8,
}

/// Size of one event record on the wire.
pub const EVENT_SIZE: usize = 64;

const _: () = assert!(size_of::<Event>() == EVENT_SIZE);
const _: () = assert!(align_of::<Event>() == 8);

impl Event {
    /// The all-zero event (kind 0 is invalid; useful as a fill value).
    pub const ZERO: Event = Event {
        recv_mono_ns: 0,
        recv_wall_ns: 0,
        venue_ts_ns: 0,
        venue_seq: 0,
        price: 0,
        qty: 0,
        aux: 0,
        instrument: 0,
        kind: 0,
        venue: 0,
        flags: 0,
        rsvd: 0,
    };

    /// Typed kind, or the raw byte if unknown.
    #[inline]
    pub fn kind(&self) -> Result<EventKind, u8> {
        EventKind::try_from(self.kind)
    }

    /// Typed venue, or the raw byte if unknown.
    #[inline]
    pub fn venue(&self) -> Result<Venue, u8> {
        Venue::try_from(self.venue)
    }

    /// Book side for book-affecting kinds.
    #[inline]
    pub fn side(&self) -> Option<Side> {
        match self.kind().ok()? {
            EventKind::BidSet | EventKind::SnapBid => Some(Side::Bid),
            EventKind::AskSet | EventKind::SnapAsk => Some(Side::Ask),
            _ => None,
        }
    }

    /// True for kinds that mutate book levels.
    #[inline]
    pub fn is_book(&self) -> bool {
        matches!(
            self.kind().ok(),
            Some(
                EventKind::BidSet
                    | EventKind::AskSet
                    | EventKind::SnapBegin
                    | EventKind::SnapBid
                    | EventKind::SnapAsk
                    | EventKind::SnapEnd
                    | EventKind::Clear
            )
        )
    }

    /// View a slice of events as raw bytes (zero-copy).
    pub fn slice_as_bytes(events: &[Event]) -> &[u8] {
        bytemuck::cast_slice(events)
    }

    /// View raw bytes as a slice of events (zero-copy). Fails if the buffer
    /// is misaligned or not a multiple of 64 bytes.
    pub fn bytes_as_slice(bytes: &[u8]) -> Result<&[Event], bytemuck::PodCastError> {
        bytemuck::try_cast_slice(bytes)
    }

    /// Copy-decode events from possibly-unaligned bytes.
    pub fn iter_unaligned(bytes: &[u8]) -> impl Iterator<Item = Event> + '_ {
        bytes
            .chunks_exact(EVENT_SIZE)
            .map(bytemuck::pod_read_unaligned::<Event>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(i: u64) -> Event {
        Event {
            recv_mono_ns: i,
            recv_wall_ns: i * 2,
            venue_ts_ns: i * 3,
            venue_seq: i * 4,
            price: 6_358_964_000_000,
            qty: 12_345_678,
            aux: 42,
            instrument: 7,
            kind: EventKind::BidSet as u8,
            venue: Venue::Coinbase as u8,
            flags: flags::FROM_SNAPSHOT,
            rsvd: 0,
        }
    }

    #[test]
    fn event_is_one_cache_line() {
        assert_eq!(size_of::<Event>(), 64);
        assert_eq!(align_of::<Event>(), 8);
    }

    #[test]
    fn cast_slice_roundtrip() {
        let evs: Vec<Event> = (0..10).map(sample).collect();
        let bytes = Event::slice_as_bytes(&evs);
        assert_eq!(bytes.len(), 640);
        let back = Event::bytes_as_slice(bytes).unwrap();
        assert_eq!(back, &evs[..]);
    }

    #[test]
    fn unaligned_iter_matches() {
        let evs: Vec<Event> = (0..4).map(sample).collect();
        let mut buf = vec![0u8; 1 + 4 * EVENT_SIZE];
        buf[1..].copy_from_slice(Event::slice_as_bytes(&evs));
        // misaligned view starting at offset 1
        let decoded: Vec<Event> = Event::iter_unaligned(&buf[1..]).collect();
        assert_eq!(decoded, evs);
        // strict cast on the misaligned view must fail (alignment), while the
        // aligned original succeeds
        assert!(Event::bytes_as_slice(&buf[1..]).is_err());
    }

    #[test]
    fn kind_and_venue_roundtrip_and_reject_unknown() {
        for k in 1u8..=11 {
            let kind = EventKind::try_from(k).unwrap();
            assert_eq!(kind as u8, k);
        }
        assert!(EventKind::try_from(0).is_err());
        assert!(EventKind::try_from(12).is_err());
        for v in Venue::ALL {
            assert_eq!(Venue::try_from(v as u8), Ok(v));
        }
        assert!(Venue::try_from(0).is_err());
        assert!(Venue::try_from(4).is_err());
    }

    #[test]
    fn side_helpers() {
        let mut e = sample(1);
        assert_eq!(e.side(), Some(Side::Bid));
        assert!(e.is_book());
        e.kind = EventKind::AskSet as u8;
        assert_eq!(e.side(), Some(Side::Ask));
        e.kind = EventKind::Trade as u8;
        assert_eq!(e.side(), None);
        assert!(!e.is_book());
        e.kind = EventKind::Clear as u8;
        assert!(e.is_book());
    }
}
