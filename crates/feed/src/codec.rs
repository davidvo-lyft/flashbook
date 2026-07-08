//! The venue codec contract: raw WS text frame -> [`Event`]s.
//!
//! Every venue implements [`VenueCodec`] twice over the same semantics:
//! `parse` is the hand-rolled zero-allocation fast path; `parse_slow` is the
//! serde_json reference implementation. `parse_slow` is simultaneously
//! (a) the fallback when `parse` meets an unexpected shape, (b) the
//! differential-testing oracle (fast == slow over every captured fixture
//! line), and (c) the published "naive serde_json baseline" of
//! BENCHMARKS.md section 3a.

use flashbook_proto::{Event, ParseFixedError, Venue};

/// Non-error outcome of parsing one frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// Events were (possibly) emitted; nothing else to do.
    None,
    /// Control-plane message (subscription ack, status, pong); no events.
    Control,
    /// Message type this codec intentionally ignores.
    Ignored,
    /// Sequence break detected. Events (a Gap marker) were emitted; the
    /// connection layer must trigger this venue's documented resync
    /// procedure for `instrument`.
    NeedResync {
        /// Instrument that lost sync.
        instrument: u32,
    },
}

/// Parse failure. The connection layer's policy on `Structure` errors from
/// the fast path is: retry the same payload via `parse_slow` (counting a
/// fallback); if that also fails, count a parse error and log the payload.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// Payload didn't match the expected JSON shape.
    #[error("json structure: {0}")]
    Structure(&'static str),
    /// A price/qty token failed exact fixed-point conversion.
    #[error("fixed-point: {0}")]
    Fixed(#[from] ParseFixedError),
    /// Message referenced a symbol we didn't subscribe to.
    #[error("unknown instrument")]
    UnknownInstrument,
}

/// Steady-state counters, read by the capture stats emitter.
#[derive(Debug, Default, Clone)]
pub struct CodecStats {
    /// Frames parsed by the fast path.
    pub fast_msgs: u64,
    /// Frames that fell back to `parse_slow` after a fast-path Structure error.
    pub fallbacks: u64,
    /// Frames neither path could parse.
    pub errors: u64,
    /// Sequence gaps detected.
    pub gaps: u64,
    /// Events emitted.
    pub events: u64,
}

/// A per-venue streaming codec. Implementations are stateful (per-symbol
/// sequence tracking) and single-connection: create one codec per WS
/// connection, and a fresh one after a reconnect.
pub trait VenueCodec: Send {
    /// Which venue this codec speaks.
    fn venue(&self) -> Venue;

    /// WebSocket URL to connect to (may encode the subscription, e.g.
    /// Binance combined streams).
    fn ws_url(&self) -> String;

    /// Messages to send after connect to subscribe (empty if URL-encoded).
    fn subscribe_messages(&self) -> Vec<String>;

    /// Hand-rolled fast parse. Appends events to `out` and returns a
    /// [`Signal`]. MUST NOT allocate on the steady-state path (buffers are
    /// reused; `out` is caller-provided and amortized).
    fn parse(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError>;

    /// serde_json reference parse with identical semantics and event output.
    fn parse_slow(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError>;

    /// Parse a REST book-snapshot body (venues that resync via REST).
    /// Emits SnapBegin / SnapBid / SnapAsk / SnapEnd events. `instrument`
    /// is passed by the caller because REST bodies don't self-identify
    /// (neither Coinbase's `/book` nor Binance's `/depth` echo the symbol).
    fn parse_rest_snapshot(
        &mut self,
        _instrument: u32,
        _body: &[u8],
        _recv_mono_ns: u64,
        _recv_wall_ns: u64,
        _out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        Ok(Signal::Ignored)
    }

    /// Steady-state counters.
    fn stats(&self) -> &CodecStats;
}

/// Small fixed symbol table: venue symbol bytes -> instrument id, allocation-
/// free lookup (linear memcmp over a handful of subscribed symbols).
#[derive(Debug, Clone, Default)]
pub struct SymbolTable {
    entries: Vec<(Vec<u8>, u32)>,
}

impl SymbolTable {
    /// Build from (venue_symbol, instrument_id) pairs.
    pub fn new(pairs: impl IntoIterator<Item = (String, u32)>) -> Self {
        Self {
            entries: pairs
                .into_iter()
                .map(|(s, id)| (s.into_bytes(), id))
                .collect(),
        }
    }

    /// Look up a symbol token (exact bytes).
    #[inline]
    pub fn lookup(&self, sym: &[u8]) -> Option<u32> {
        self.entries
            .iter()
            .find(|(s, _)| s.as_slice() == sym)
            .map(|&(_, id)| id)
    }

    /// Number of symbols.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate (symbol, id).
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], u32)> {
        self.entries.iter().map(|(s, id)| (s.as_slice(), *id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_table_lookup() {
        let t = SymbolTable::new([("BTC-USD".to_string(), 1), ("ETH-USD".to_string(), 2)]);
        assert_eq!(t.lookup(b"BTC-USD"), Some(1));
        assert_eq!(t.lookup(b"ETH-USD"), Some(2));
        assert_eq!(t.lookup(b"DOGE-USD"), None);
        assert_eq!(t.lookup(b"BTC-US"), None);
        assert_eq!(t.lookup(b""), None);
        assert_eq!(t.len(), 2);
        assert!(!t.is_empty());
    }
}
