//! Coinbase Exchange codec: `level2_batch` (snapshot + l2update), `matches`,
//! and `heartbeat` channels -> normalized [`Event`]s.
//!
//! Message dispatch is on the leading `"type"` key:
//!
//! - `snapshot` -> `Clear` + `SnapBegin` + `SnapBid`* + `SnapAsk`* + `SnapEnd`
//!   (canonical bids-then-asks even though the JSON carries asks first), all
//!   flagged [`flags::FROM_SNAPSHOT`].
//! - `l2update` -> `BidSet`/`AskSet` per change, in payload order (Coinbase's
//!   `changes` list interleaves sides; that exact order is preserved).
//! - `match` / `last_match` -> `Trade` (plus a leading `Gap` on a trade-id
//!   jump). `last_match` seeds the per-instrument trade-id baseline and is
//!   never gap-checked.
//! - `heartbeat` -> `Heartbeat` (plus a trailing `Gap` when the venue's
//!   `last_trade_id` ran ahead of the trades we saw).
//! - `subscriptions` / `error` -> [`Signal::Control`]; anything else ->
//!   [`Signal::Ignored`].
//!
//! Coinbase documents that dropped messages do occur, and `l2update`
//! carries no per-message sequence, so a trade-id or heartbeat gap is the
//! only in-band evidence that the book may have silently lost updates. Any
//! detected gap therefore returns [`Signal::NeedResync`] for the affected
//! instrument; the connection layer re-snapshots it via REST
//! `/book?level=2` ([`VenueCodec::parse_rest_snapshot`]). Out-of-order
//! matches (lower trade ids, which Coinbase says "can be ignored or
//! represent a message that has arrived out of order") are still emitted as
//! `Trade`s but never lower the gap baseline, so a gap is reported (and a
//! resync requested) exactly once.

use flashbook_proto::{Event, EventKind, Venue, event::flags, parse_fixed};
use memchr::memmem::{Finder, rfind};
use serde_json::Value;

use crate::codec::{CodecError, CodecStats, Signal, SymbolTable, VenueCodec};
use crate::scan::{Cursor, parse_rfc3339_ns};

/// Coinbase Exchange market-data WebSocket endpoint.
const WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";

/// Per-frame constants stamped onto every event emitted for one message.
#[derive(Debug, Clone, Copy)]
struct Frame {
    recv_mono_ns: u64,
    recv_wall_ns: u64,
    venue_ts_ns: u64,
    venue_seq: u64,
    instrument: u32,
    flags: u8,
}

impl Frame {
    /// Build one event with this frame's constants.
    #[inline]
    fn ev(&self, kind: EventKind, price: i64, qty: i64, aux: u64) -> Event {
        Event {
            recv_mono_ns: self.recv_mono_ns,
            recv_wall_ns: self.recv_wall_ns,
            venue_ts_ns: self.venue_ts_ns,
            venue_seq: self.venue_seq,
            price,
            qty,
            aux,
            instrument: self.instrument,
            kind: kind as u8,
            venue: Venue::Coinbase as u8,
            flags: self.flags,
            rsvd: 0,
        }
    }
}

/// Codec for the Coinbase Exchange WebSocket feed. Stateful: tracks the last
/// seen trade id per instrument for trade-gap and heartbeat cross-checks.
/// One instance per connection; recreate after a reconnect.
#[derive(Debug)]
pub struct CoinbaseCodec {
    table: SymbolTable,
    stats: CodecStats,
    /// Per-instrument last seen trade id (linear scan; a handful of symbols).
    last_trade: Vec<(u32, u64)>,
    f_type: Finder<'static>,
    f_product: Finder<'static>,
    f_changes: Finder<'static>,
    f_bids: Finder<'static>,
    f_asks: Finder<'static>,
    f_time: Finder<'static>,
    f_trade_id: Finder<'static>,
    f_side: Finder<'static>,
    f_size: Finder<'static>,
    f_price: Finder<'static>,
    f_sequence: Finder<'static>,
    f_last_trade_id: Finder<'static>,
}

impl CoinbaseCodec {
    /// New codec over the given symbol table (venue symbol -> instrument id).
    /// All `memmem` finders are precomputed here so the fast path never
    /// builds a searcher per frame.
    pub fn new(table: SymbolTable) -> Self {
        Self {
            table,
            stats: CodecStats::default(),
            last_trade: Vec::new(),
            f_type: Finder::new(b"\"type\":"),
            f_product: Finder::new(b"\"product_id\":"),
            f_changes: Finder::new(b"\"changes\":"),
            f_bids: Finder::new(b"\"bids\":"),
            f_asks: Finder::new(b"\"asks\":"),
            f_time: Finder::new(b"\"time\":"),
            f_trade_id: Finder::new(b"\"trade_id\":"),
            f_side: Finder::new(b"\"side\":"),
            f_size: Finder::new(b"\"size\":"),
            f_price: Finder::new(b"\"price\":"),
            f_sequence: Finder::new(b"\"sequence\":"),
            f_last_trade_id: Finder::new(b"\"last_trade_id\":"),
        }
    }

    /// Instrument id for a venue symbol token.
    #[inline]
    fn instrument(&self, sym: &[u8]) -> Result<u32, CodecError> {
        self.table.lookup(sym).ok_or(CodecError::UnknownInstrument)
    }

    /// Record a trade id; returns the missed count when a gap-checked trade
    /// id jumps past `last + 1`. `last_match` messages pass
    /// `gap_check = false`: they seed the baseline (first message after
    /// subscribe) but never trigger a gap themselves. The baseline is
    /// advance-only: an out-of-order match with `trade_id <= last` never
    /// lowers it, so an already-reported gap is not re-reported when the
    /// straggler (or the next heartbeat) arrives.
    fn note_trade(&mut self, instrument: u32, trade_id: u64, gap_check: bool) -> Option<u64> {
        if let Some((_, last)) = self.last_trade.iter_mut().find(|(i, _)| *i == instrument) {
            let missed =
                (gap_check && trade_id > last.saturating_add(1)).then(|| trade_id - *last - 1);
            if trade_id > *last {
                *last = trade_id;
            }
            missed
        } else {
            // No baseline yet: the first (last_)match never triggers a gap.
            self.last_trade.push((instrument, trade_id));
            None
        }
    }

    /// Cross-check a heartbeat's `last_trade_id` against the trades we saw.
    /// Returns the missed count (and advances the baseline so the same gap
    /// is reported once) when the venue ran ahead of us. No-op until a
    /// (last_)match established a baseline.
    fn note_heartbeat(&mut self, instrument: u32, last_trade_id: u64) -> Option<u64> {
        let (_, last) = self.last_trade.iter_mut().find(|(i, _)| *i == instrument)?;
        if last_trade_id > *last {
            let missed = last_trade_id - *last;
            *last = last_trade_id;
            Some(missed)
        } else {
            None
        }
    }

    /// Shared match emission (fast + slow paths): optional `Gap` (aux =
    /// missed count, flags cleared) BEFORE the `Trade` (aux = trade id).
    /// A gap means the book may have silently dropped messages too, so it
    /// returns [`Signal::NeedResync`] for the instrument.
    fn emit_match(
        &mut self,
        frame: Frame,
        trade_id: u64,
        gap_check: bool,
        price: i64,
        qty: i64,
        out: &mut Vec<Event>,
    ) -> Signal {
        let mut sig = Signal::None;
        if let Some(missed) = self.note_trade(frame.instrument, trade_id, gap_check) {
            let gap = Frame { flags: 0, ..frame };
            out.push(gap.ev(EventKind::Gap, 0, 0, missed));
            self.stats.gaps += 1;
            sig = Signal::NeedResync {
                instrument: frame.instrument,
            };
        }
        out.push(frame.ev(EventKind::Trade, price, qty, trade_id));
        sig
    }

    /// Shared heartbeat emission (fast + slow paths): `Heartbeat` (aux =
    /// venue `last_trade_id`), then a `Gap` (aux = missed trades) AFTER it
    /// when the venue reports trades we never received. A gap returns
    /// [`Signal::NeedResync`] for the instrument (see [`Self::emit_match`]).
    fn emit_heartbeat(&mut self, frame: Frame, last_trade_id: u64, out: &mut Vec<Event>) -> Signal {
        out.push(frame.ev(EventKind::Heartbeat, 0, 0, last_trade_id));
        if let Some(missed) = self.note_heartbeat(frame.instrument, last_trade_id) {
            out.push(frame.ev(EventKind::Gap, 0, 0, missed));
            self.stats.gaps += 1;
            return Signal::NeedResync {
                instrument: frame.instrument,
            };
        }
        Signal::None
    }

    // ---------------------------------------------------------------- fast

    /// Venue timestamp from the `"time"` key, searched over the whole
    /// payload (it trails the large arrays). Absent -> 0; unparseable ->
    /// structure error (matching the slow path).
    fn fast_ts(&self, payload: &[u8]) -> Result<u64, CodecError> {
        let mut c = Cursor::new(payload);
        if c.skip_past_finder(&self.f_time).is_none() {
            return Ok(0);
        }
        c.skip_ws();
        let s = c.read_string().ok_or(CodecError::Structure("time"))?;
        parse_rfc3339_ns(s).ok_or(CodecError::Structure("time"))
    }

    /// Fast-path frame dispatch. May leave partial events in `out` on error;
    /// the [`VenueCodec::parse`] wrapper restores the buffer length.
    fn fast_inner(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let mut c = Cursor::new(payload);
        c.skip_ws();
        if !c.eat(b'{') {
            return Err(CodecError::Structure("not an object"));
        }
        c.skip_past_finder(&self.f_type)
            .ok_or(CodecError::Structure("type"))?;
        c.skip_ws();
        let ty = c.read_string().ok_or(CodecError::Structure("type"))?;
        match ty {
            b"l2update" => self.fast_l2update(payload, recv_mono_ns, recv_wall_ns, out),
            b"match" => self.fast_match(payload, true, recv_mono_ns, recv_wall_ns, out),
            b"last_match" => self.fast_match(payload, false, recv_mono_ns, recv_wall_ns, out),
            b"heartbeat" => self.fast_heartbeat(payload, recv_mono_ns, recv_wall_ns, out),
            b"snapshot" => self.fast_snapshot(payload, recv_mono_ns, recv_wall_ns, out),
            b"subscriptions" | b"error" => Ok(Signal::Control),
            _ => Ok(Signal::Ignored),
        }
    }

    /// Fast `l2update`: absolute-qty `BidSet`/`AskSet` per change, in the
    /// exact (side-interleaved) payload order.
    fn fast_l2update(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let venue_ts_ns = self.fast_ts(payload)?;
        let mut c = Cursor::new(payload);
        c.skip_past_finder(&self.f_product)
            .ok_or(CodecError::Structure("product_id"))?;
        c.skip_ws();
        let sym = c.read_string().ok_or(CodecError::Structure("product_id"))?;
        let instrument = self.instrument(sym)?;
        c.skip_past_finder(&self.f_changes)
            .ok_or(CodecError::Structure("changes"))?;
        c.skip_ws();
        if !c.eat(b'[') {
            return Err(CodecError::Structure("changes array"));
        }
        let frame = Frame {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq: 0,
            instrument,
            flags: 0,
        };
        c.skip_ws();
        if c.eat(b']') {
            return Ok(Signal::None);
        }
        loop {
            c.skip_ws();
            if !c.eat(b'[') {
                return Err(CodecError::Structure("change entry"));
            }
            c.skip_ws();
            let kind = match c
                .read_string()
                .ok_or(CodecError::Structure("change side"))?
            {
                b"buy" => EventKind::BidSet,
                b"sell" => EventKind::AskSet,
                _ => return Err(CodecError::Structure("change side")),
            };
            c.skip_ws();
            if !c.eat(b',') {
                return Err(CodecError::Structure("change sep"));
            }
            c.skip_ws();
            let price = parse_fixed(
                c.read_string()
                    .ok_or(CodecError::Structure("change price"))?,
            )?;
            c.skip_ws();
            if !c.eat(b',') {
                return Err(CodecError::Structure("change sep"));
            }
            c.skip_ws();
            let qty = parse_fixed(c.read_string().ok_or(CodecError::Structure("change qty"))?)?;
            c.skip_ws();
            if !c.eat(b']') {
                return Err(CodecError::Structure("change end"));
            }
            out.push(frame.ev(kind, price, qty, 0));
            c.skip_ws();
            if c.eat(b',') {
                continue;
            }
            if c.eat(b']') {
                break;
            }
            return Err(CodecError::Structure("changes end"));
        }
        Ok(Signal::None)
    }

    /// Fast `match` / `last_match` -> `Trade` (+ optional leading `Gap`).
    fn fast_match(
        &mut self,
        payload: &[u8],
        gap_check: bool,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let venue_ts_ns = self.fast_ts(payload)?;
        let mut c = Cursor::new(payload);
        c.skip_past_finder(&self.f_trade_id)
            .ok_or(CodecError::Structure("trade_id"))?;
        c.skip_ws();
        let trade_id = c.read_u64().ok_or(CodecError::Structure("trade_id"))?;
        c.skip_past_finder(&self.f_side)
            .ok_or(CodecError::Structure("side"))?;
        c.skip_ws();
        // Coinbase's match `side` is the MAKER order's side, not the
        // aggressor's: side == "buy" means a resting bid was hit, i.e. the
        // taker (aggressor) SOLD -> TAKER_SELL. side == "sell" means a
        // resting ask was lifted, i.e. the taker bought.
        let taker_sell = match c.read_string().ok_or(CodecError::Structure("side"))? {
            b"buy" => true,
            b"sell" => false,
            _ => return Err(CodecError::Structure("side")),
        };
        c.skip_past_finder(&self.f_size)
            .ok_or(CodecError::Structure("size"))?;
        c.skip_ws();
        let qty = parse_fixed(c.read_string().ok_or(CodecError::Structure("size"))?)?;
        c.skip_past_finder(&self.f_price)
            .ok_or(CodecError::Structure("price"))?;
        c.skip_ws();
        let price = parse_fixed(c.read_string().ok_or(CodecError::Structure("price"))?)?;
        c.skip_past_finder(&self.f_product)
            .ok_or(CodecError::Structure("product_id"))?;
        c.skip_ws();
        let sym = c.read_string().ok_or(CodecError::Structure("product_id"))?;
        let instrument = self.instrument(sym)?;
        c.skip_past_finder(&self.f_sequence)
            .ok_or(CodecError::Structure("sequence"))?;
        c.skip_ws();
        let venue_seq = c.read_u64().ok_or(CodecError::Structure("sequence"))?;
        let frame = Frame {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq,
            instrument,
            flags: if taker_sell { flags::TAKER_SELL } else { 0 },
        };
        Ok(self.emit_match(frame, trade_id, gap_check, price, qty, out))
    }

    /// Fast `heartbeat` -> `Heartbeat` (+ optional trailing `Gap`).
    fn fast_heartbeat(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let venue_ts_ns = self.fast_ts(payload)?;
        let mut c = Cursor::new(payload);
        c.skip_past_finder(&self.f_last_trade_id)
            .ok_or(CodecError::Structure("last_trade_id"))?;
        c.skip_ws();
        let last_trade_id = c.read_u64().ok_or(CodecError::Structure("last_trade_id"))?;
        c.skip_past_finder(&self.f_product)
            .ok_or(CodecError::Structure("product_id"))?;
        c.skip_ws();
        let sym = c.read_string().ok_or(CodecError::Structure("product_id"))?;
        let instrument = self.instrument(sym)?;
        c.skip_past_finder(&self.f_sequence)
            .ok_or(CodecError::Structure("sequence"))?;
        c.skip_ws();
        let venue_seq = c.read_u64().ok_or(CodecError::Structure("sequence"))?;
        let frame = Frame {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq,
            instrument,
            flags: 0,
        };
        Ok(self.emit_heartbeat(frame, last_trade_id, out))
    }

    /// Fast WS `snapshot` -> full snapshot bracket, flags `FROM_SNAPSHOT`.
    fn fast_snapshot(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let venue_ts_ns = self.fast_ts(payload)?;
        let mut c = Cursor::new(payload);
        c.skip_past_finder(&self.f_product)
            .ok_or(CodecError::Structure("product_id"))?;
        c.skip_ws();
        let sym = c.read_string().ok_or(CodecError::Structure("product_id"))?;
        let instrument = self.instrument(sym)?;
        let frame = Frame {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq: 0,
            instrument,
            flags: flags::FROM_SNAPSHOT,
        };
        self.emit_levels(payload, frame, false, out)
    }

    /// Stream both level arrays of a snapshot body: `Clear`, `SnapBegin`
    /// (aux patched to the total level count), all bids, all asks, `SnapEnd`
    /// — canonical bids-then-asks regardless of JSON key order.
    /// `order_count` selects REST 3-element levels (`[price, size, orders]`).
    fn emit_levels(
        &mut self,
        payload: &[u8],
        frame: Frame,
        order_count: bool,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let mut bids = Cursor::new(payload);
        bids.skip_past_finder(&self.f_bids)
            .ok_or(CodecError::Structure("bids"))?;
        let mut asks = Cursor::new(payload);
        asks.skip_past_finder(&self.f_asks)
            .ok_or(CodecError::Structure("asks"))?;
        out.push(frame.ev(EventKind::Clear, 0, 0, 0));
        let begin = out.len();
        out.push(frame.ev(EventKind::SnapBegin, 0, 0, 0));
        let nb = Self::fast_levels(&mut bids, frame, EventKind::SnapBid, order_count, out)?;
        let na = Self::fast_levels(&mut asks, frame, EventKind::SnapAsk, order_count, out)?;
        out.push(frame.ev(EventKind::SnapEnd, 0, 0, 0));
        out[begin].aux = nb + na;
        Ok(Signal::None)
    }

    /// Walk one `[["price","size"(,orders)],...]` array with the cursor just
    /// past its key; emits one `kind` event per level, returns the count.
    fn fast_levels(
        c: &mut Cursor,
        frame: Frame,
        kind: EventKind,
        order_count: bool,
        out: &mut Vec<Event>,
    ) -> Result<u64, CodecError> {
        c.skip_ws();
        if !c.eat(b'[') {
            return Err(CodecError::Structure("level array"));
        }
        c.skip_ws();
        if c.eat(b']') {
            return Ok(0);
        }
        let mut n = 0u64;
        loop {
            c.skip_ws();
            if !c.eat(b'[') {
                return Err(CodecError::Structure("level entry"));
            }
            c.skip_ws();
            let price = parse_fixed(
                c.read_string()
                    .ok_or(CodecError::Structure("level price"))?,
            )?;
            c.skip_ws();
            if !c.eat(b',') {
                return Err(CodecError::Structure("level sep"));
            }
            c.skip_ws();
            let qty = parse_fixed(c.read_string().ok_or(CodecError::Structure("level qty"))?)?;
            c.skip_ws();
            if order_count {
                // REST /book?level=2 carries a third element (resting order
                // count); consume and ignore it.
                if !c.eat(b',') {
                    return Err(CodecError::Structure("level orders"));
                }
                c.skip_ws();
                c.read_u64().ok_or(CodecError::Structure("level orders"))?;
                c.skip_ws();
            }
            if !c.eat(b']') {
                return Err(CodecError::Structure("level end"));
            }
            out.push(frame.ev(kind, price, qty, 0));
            n += 1;
            c.skip_ws();
            if c.eat(b',') {
                continue;
            }
            if c.eat(b']') {
                break;
            }
            return Err(CodecError::Structure("level array end"));
        }
        Ok(n)
    }

    // ---------------------------------------------------------------- slow

    /// Venue timestamp from a DOM: absent -> 0, non-string or unparseable ->
    /// structure error (matching [`CoinbaseCodec::fast_ts`]).
    fn slow_ts(v: &Value) -> Result<u64, CodecError> {
        match v.get("time") {
            None => Ok(0),
            Some(Value::String(s)) => {
                parse_rfc3339_ns(s.as_bytes()).ok_or(CodecError::Structure("time"))
            }
            Some(_) => Err(CodecError::Structure("time")),
        }
    }

    /// Required string field from a DOM.
    fn slow_str<'v>(v: &'v Value, key: &'static str) -> Result<&'v str, CodecError> {
        v.get(key)
            .and_then(Value::as_str)
            .ok_or(CodecError::Structure(key))
    }

    /// Required u64 field from a DOM.
    fn slow_u64(v: &Value, key: &'static str) -> Result<u64, CodecError> {
        v.get(key)
            .and_then(Value::as_u64)
            .ok_or(CodecError::Structure(key))
    }

    /// Required fixed-point-in-string field from a DOM.
    fn slow_fixed(v: &Value, key: &'static str) -> Result<i64, CodecError> {
        Ok(parse_fixed(Self::slow_str(v, key)?.as_bytes())?)
    }

    /// Slow-path frame dispatch over a serde_json DOM.
    fn slow_inner(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let v: Value =
            serde_json::from_slice(payload).map_err(|_| CodecError::Structure("invalid json"))?;
        match Self::slow_str(&v, "type")? {
            "l2update" => self.slow_l2update(&v, recv_mono_ns, recv_wall_ns, out),
            "match" => self.slow_match(&v, true, recv_mono_ns, recv_wall_ns, out),
            "last_match" => self.slow_match(&v, false, recv_mono_ns, recv_wall_ns, out),
            "heartbeat" => self.slow_heartbeat(&v, recv_mono_ns, recv_wall_ns, out),
            "snapshot" => self.slow_snapshot(&v, recv_mono_ns, recv_wall_ns, out),
            "subscriptions" | "error" => Ok(Signal::Control),
            _ => Ok(Signal::Ignored),
        }
    }

    /// Slow `l2update` (see [`CoinbaseCodec::fast_l2update`]).
    fn slow_l2update(
        &mut self,
        v: &Value,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let venue_ts_ns = Self::slow_ts(v)?;
        let instrument = self.instrument(Self::slow_str(v, "product_id")?.as_bytes())?;
        let changes = v
            .get("changes")
            .and_then(Value::as_array)
            .ok_or(CodecError::Structure("changes"))?;
        let frame = Frame {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq: 0,
            instrument,
            flags: 0,
        };
        for ch in changes {
            let entry = ch
                .as_array()
                .filter(|a| a.len() == 3)
                .ok_or(CodecError::Structure("change entry"))?;
            let kind = match entry[0].as_str() {
                Some("buy") => EventKind::BidSet,
                Some("sell") => EventKind::AskSet,
                _ => return Err(CodecError::Structure("change side")),
            };
            let price = parse_fixed(
                entry[1]
                    .as_str()
                    .ok_or(CodecError::Structure("change price"))?
                    .as_bytes(),
            )?;
            let qty = parse_fixed(
                entry[2]
                    .as_str()
                    .ok_or(CodecError::Structure("change qty"))?
                    .as_bytes(),
            )?;
            out.push(frame.ev(kind, price, qty, 0));
        }
        Ok(Signal::None)
    }

    /// Slow `match` / `last_match` (see [`CoinbaseCodec::fast_match`]).
    fn slow_match(
        &mut self,
        v: &Value,
        gap_check: bool,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let venue_ts_ns = Self::slow_ts(v)?;
        let trade_id = Self::slow_u64(v, "trade_id")?;
        // Maker-side semantics: see the comment in `fast_match`.
        let taker_sell = match Self::slow_str(v, "side")? {
            "buy" => true,
            "sell" => false,
            _ => return Err(CodecError::Structure("side")),
        };
        let qty = Self::slow_fixed(v, "size")?;
        let price = Self::slow_fixed(v, "price")?;
        let instrument = self.instrument(Self::slow_str(v, "product_id")?.as_bytes())?;
        let venue_seq = Self::slow_u64(v, "sequence")?;
        let frame = Frame {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq,
            instrument,
            flags: if taker_sell { flags::TAKER_SELL } else { 0 },
        };
        Ok(self.emit_match(frame, trade_id, gap_check, price, qty, out))
    }

    /// Slow `heartbeat` (see [`CoinbaseCodec::fast_heartbeat`]).
    fn slow_heartbeat(
        &mut self,
        v: &Value,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let venue_ts_ns = Self::slow_ts(v)?;
        let last_trade_id = Self::slow_u64(v, "last_trade_id")?;
        let instrument = self.instrument(Self::slow_str(v, "product_id")?.as_bytes())?;
        let venue_seq = Self::slow_u64(v, "sequence")?;
        let frame = Frame {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq,
            instrument,
            flags: 0,
        };
        Ok(self.emit_heartbeat(frame, last_trade_id, out))
    }

    /// Slow WS `snapshot` (see [`CoinbaseCodec::fast_snapshot`]).
    fn slow_snapshot(
        &mut self,
        v: &Value,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let venue_ts_ns = Self::slow_ts(v)?;
        let instrument = self.instrument(Self::slow_str(v, "product_id")?.as_bytes())?;
        let bids = v
            .get("bids")
            .and_then(Value::as_array)
            .ok_or(CodecError::Structure("bids"))?;
        let asks = v
            .get("asks")
            .and_then(Value::as_array)
            .ok_or(CodecError::Structure("asks"))?;
        let frame = Frame {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq: 0,
            instrument,
            flags: flags::FROM_SNAPSHOT,
        };
        out.push(frame.ev(EventKind::Clear, 0, 0, 0));
        out.push(frame.ev(EventKind::SnapBegin, 0, 0, (bids.len() + asks.len()) as u64));
        for (levels, kind) in [(bids, EventKind::SnapBid), (asks, EventKind::SnapAsk)] {
            for lvl in levels {
                let entry = lvl
                    .as_array()
                    .filter(|a| a.len() == 2)
                    .ok_or(CodecError::Structure("level entry"))?;
                let price = parse_fixed(
                    entry[0]
                        .as_str()
                        .ok_or(CodecError::Structure("level price"))?
                        .as_bytes(),
                )?;
                let qty = parse_fixed(
                    entry[1]
                        .as_str()
                        .ok_or(CodecError::Structure("level qty"))?
                        .as_bytes(),
                )?;
                out.push(frame.ev(kind, price, qty, 0));
            }
        }
        out.push(frame.ev(EventKind::SnapEnd, 0, 0, 0));
        Ok(Signal::None)
    }

    /// Venue timestamp for a REST book body: the LAST `"time":` occurrence.
    /// The body is serialized bids, asks, sequence, auction_mode, auction,
    /// time — and in auction mode the `auction` object carries its own
    /// `time` property, so the first occurrence would be the wrong one; the
    /// top-level `time` is serialized last. Absent -> 0; unparseable ->
    /// structure error (matching [`CoinbaseCodec::fast_ts`]).
    fn rest_ts(body: &[u8]) -> Result<u64, CodecError> {
        const KEY: &[u8] = b"\"time\":";
        let Some(at) = rfind(body, KEY) else {
            return Ok(0);
        };
        let mut c = Cursor::new(body);
        c.set_pos(at + KEY.len());
        c.skip_ws();
        let s = c.read_string().ok_or(CodecError::Structure("time"))?;
        parse_rfc3339_ns(s).ok_or(CodecError::Structure("time"))
    }

    /// REST `/products/<id>/book?level=2` body -> snapshot bracket.
    fn rest_inner(
        &mut self,
        instrument: u32,
        body: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let venue_ts_ns = Self::rest_ts(body)?;
        let mut c = Cursor::new(body);
        c.skip_past_finder(&self.f_sequence)
            .ok_or(CodecError::Structure("sequence"))?;
        c.skip_ws();
        let venue_seq = c.read_u64().ok_or(CodecError::Structure("sequence"))?;
        let frame = Frame {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq,
            instrument,
            flags: flags::FROM_SNAPSHOT | flags::SYNTHETIC,
        };
        self.emit_levels(body, frame, true, out)
    }
}

impl VenueCodec for CoinbaseCodec {
    fn venue(&self) -> Venue {
        Venue::Coinbase
    }

    fn ws_url(&self) -> String {
        WS_URL.to_string()
    }

    fn subscribe_messages(&self) -> Vec<String> {
        let mut ids = String::new();
        for (sym, _) in self.table.iter() {
            if !ids.is_empty() {
                ids.push(',');
            }
            ids.push('"');
            ids.push_str(&String::from_utf8_lossy(sym));
            ids.push('"');
        }
        vec![format!(
            "{{\"type\":\"subscribe\",\"product_ids\":[{ids}],\
             \"channels\":[\"level2_batch\",\"matches\",\"heartbeat\"]}}"
        )]
    }

    fn parse(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let start = out.len();
        match self.fast_inner(payload, recv_mono_ns, recv_wall_ns, out) {
            Ok(sig) => {
                self.stats.fast_msgs += 1;
                self.stats.events += (out.len() - start) as u64;
                Ok(sig)
            }
            Err(e) => {
                out.truncate(start);
                Err(e)
            }
        }
    }

    fn parse_slow(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let start = out.len();
        match self.slow_inner(payload, recv_mono_ns, recv_wall_ns, out) {
            Ok(sig) => {
                self.stats.events += (out.len() - start) as u64;
                Ok(sig)
            }
            Err(e) => {
                out.truncate(start);
                Err(e)
            }
        }
    }

    fn parse_rest_snapshot(
        &mut self,
        instrument: u32,
        body: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let start = out.len();
        match self.rest_inner(instrument, body, recv_mono_ns, recv_wall_ns, out) {
            Ok(sig) => {
                self.stats.events += (out.len() - start) as u64;
                Ok(sig)
            }
            Err(e) => {
                out.truncate(start);
                Err(e)
            }
        }
    }

    fn stats(&self) -> &CodecStats {
        &self.stats
    }
}
