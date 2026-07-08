//! Binance spot codec.
//!
//! Speaks the combined-stream endpoint (`/stream?streams=...`) with
//! `<sym>@depth@100ms` diff-depth updates and `<sym>@trade` trade prints,
//! wrapped in a `{"stream":...,"data":{...}}` envelope. Book sync follows
//! Binance's documented diff-depth protocol: a REST `/api/v3/depth` snapshot
//! (`lastUpdateId`) anchors a per-instrument `Synced { last_u }` state;
//! diffs with `u <= last_u` are stale duplicates, a diff whose `U` exceeds
//! `last_u + 1` is a sequence gap that forces a resync, and everything else
//! (including the documented first-diff straddle of `lastUpdateId + 1`)
//! applies with absolute-quantity semantics.

use flashbook_proto::event::flags;
use flashbook_proto::{Event, EventKind, Venue, parse_fixed};
use memchr::memmem;

use crate::codec::{CodecError, CodecStats, Signal, SymbolTable, VenueCodec};
use crate::scan::Cursor;

/// Diff-depth sync state for one instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DepthState {
    /// No snapshot anchor yet: diffs are fully validated but not applied.
    Unsynced,
    /// Anchored; `last_u` is the final update id of the last applied diff
    /// (or the snapshot's `lastUpdateId` right after a snapshot).
    Synced {
        /// Final update id last applied.
        last_u: u64,
    },
}

/// Per-instrument codec state.
#[derive(Debug, Clone)]
struct InstState {
    /// Instrument id (from the registry, via the symbol table).
    id: u32,
    /// Depth sync state machine.
    depth: DepthState,
    /// Last seen trade id, for trade-stream continuity checks.
    last_trade: Option<u64>,
}

/// Binance spot codec: combined-stream WS frames -> normalized [`Event`]s.
///
/// Stateful per instrument (depth sync + trade-id continuity); create one
/// per WS connection and a fresh one after reconnect.
#[derive(Debug)]
pub struct BinanceCodec {
    table: SymbolTable,
    stats: CodecStats,
    states: Vec<InstState>,
    // Precomputed searchers for the fast path (zero steady-state allocation).
    f_data_e: memmem::Finder<'static>,
    f_ecap: memmem::Finder<'static>,
    f_s: memmem::Finder<'static>,
    f_ucap: memmem::Finder<'static>,
    f_ulow: memmem::Finder<'static>,
    f_b: memmem::Finder<'static>,
    f_a: memmem::Finder<'static>,
    f_t: memmem::Finder<'static>,
    f_p: memmem::Finder<'static>,
    f_q: memmem::Finder<'static>,
    f_tcap: memmem::Finder<'static>,
    f_m: memmem::Finder<'static>,
}

/// Exact shape of a subscription acknowledgement, e.g. `{"result":null,"id":1}`.
const CONTROL_PREFIX: &[u8] = b"{\"result\":null,\"id\":";

impl BinanceCodec {
    /// New codec over `table` (venue symbols are UPPERCASE, e.g. `BTCUSDT`,
    /// matching the `"s"` field).
    pub fn new(table: SymbolTable) -> Self {
        let states = table
            .iter()
            .map(|(_, id)| InstState {
                id,
                depth: DepthState::Unsynced,
                last_trade: None,
            })
            .collect();
        Self {
            table,
            stats: CodecStats::default(),
            states,
            f_data_e: memmem::Finder::new(b"\"data\":{\"e\":\""),
            f_ecap: memmem::Finder::new(b"\"E\":"),
            f_s: memmem::Finder::new(b"\"s\":"),
            f_ucap: memmem::Finder::new(b"\"U\":"),
            f_ulow: memmem::Finder::new(b"\"u\":"),
            f_b: memmem::Finder::new(b"\"b\":["),
            f_a: memmem::Finder::new(b"\"a\":["),
            f_t: memmem::Finder::new(b"\"t\":"),
            f_p: memmem::Finder::new(b"\"p\":"),
            f_q: memmem::Finder::new(b"\"q\":"),
            f_tcap: memmem::Finder::new(b"\"T\":"),
            f_m: memmem::Finder::new(b"\"m\":"),
        }
    }

    #[inline]
    fn state_index(&self, instrument: u32) -> Option<usize> {
        self.states.iter().position(|s| s.id == instrument)
    }

    /// Build one normalized event with the shared Binance fields filled in.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn ev(
        kind: EventKind,
        instrument: u32,
        price: i64,
        qty: i64,
        aux: u64,
        venue_seq: u64,
        venue_ts_ns: u64,
        ev_flags: u8,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
    ) -> Event {
        Event {
            recv_mono_ns,
            recv_wall_ns,
            venue_ts_ns,
            venue_seq,
            price,
            qty,
            aux,
            instrument,
            kind: kind as u8,
            venue: Venue::Binance as u8,
            flags: ev_flags,
            rsvd: 0,
        }
    }

    /// Shared depth decision: events for this diff were already appended to
    /// `out` starting at `start`; keep, drop, or replace them with a Gap
    /// according to the sync state machine.
    #[allow(clippy::too_many_arguments)]
    fn finish_depth(
        &mut self,
        instrument: u32,
        first_u: u64,
        final_u: u64,
        venue_ts_ns: u64,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
        start: usize,
    ) -> Result<Signal, CodecError> {
        let idx = self
            .state_index(instrument)
            .ok_or(CodecError::UnknownInstrument)?;
        match self.states[idx].depth {
            DepthState::Unsynced => {
                // Fully validated but not applied: replay re-runs this same
                // state machine, so dropping pre-snapshot diffs is deterministic.
                out.truncate(start);
                Ok(Signal::None)
            }
            DepthState::Synced { last_u } => {
                if final_u <= last_u {
                    // Stale duplicate (covers pre-snapshot diffs after a
                    // REST resync).
                    out.truncate(start);
                    Ok(Signal::None)
                } else if first_u > last_u.saturating_add(1) {
                    // Sequence gap: never apply across it.
                    out.truncate(start);
                    out.push(Self::ev(
                        EventKind::Gap,
                        instrument,
                        0,
                        0,
                        first_u - last_u - 1,
                        final_u,
                        venue_ts_ns,
                        0,
                        recv_mono_ns,
                        recv_wall_ns,
                    ));
                    self.states[idx].depth = DepthState::Unsynced;
                    self.stats.gaps += 1;
                    Ok(Signal::NeedResync { instrument })
                } else {
                    // U <= last_u + 1 <= u: applies (covers the documented
                    // first-diff straddle of lastUpdateId + 1).
                    self.states[idx].depth = DepthState::Synced { last_u: final_u };
                    Ok(Signal::None)
                }
            }
        }
    }

    /// Shared trade emission: trade-id continuity Gap (no resync) + Trade.
    #[allow(clippy::too_many_arguments)]
    fn finish_trade(
        &mut self,
        instrument: u32,
        trade_id: u64,
        price: i64,
        qty: i64,
        venue_ts_ns: u64,
        taker_sell: bool,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let idx = self
            .state_index(instrument)
            .ok_or(CodecError::UnknownInstrument)?;
        if let Some(prev) = self.states[idx].last_trade
            && trade_id > prev.saturating_add(1)
        {
            out.push(Self::ev(
                EventKind::Gap,
                instrument,
                0,
                0,
                trade_id - prev - 1,
                trade_id,
                venue_ts_ns,
                0,
                recv_mono_ns,
                recv_wall_ns,
            ));
            self.stats.gaps += 1;
        }
        out.push(Self::ev(
            EventKind::Trade,
            instrument,
            price,
            qty,
            trade_id,
            trade_id,
            venue_ts_ns,
            if taker_sell { flags::TAKER_SELL } else { 0 },
            recv_mono_ns,
            recv_wall_ns,
        ));
        self.states[idx].last_trade = Some(trade_id);
        Ok(Signal::None)
    }

    /// Read a `u64` field value and require a `,`/`}` terminator so the fast
    /// path rejects non-integer tokens exactly like `as_u64` on the slow path.
    #[inline]
    fn read_u64_field(c: &mut Cursor<'_>, what: &'static str) -> Result<u64, CodecError> {
        let v = c.read_u64().ok_or(CodecError::Structure(what))?;
        if matches!(c.peek(), Some(b',' | b'}')) {
            Ok(v)
        } else {
            Err(CodecError::Structure(what))
        }
    }

    /// Fast-path parse of one `[["price","qty"],...]` level array; the cursor
    /// must sit just past the opening `[`. Appends one `kind` event per level.
    #[allow(clippy::too_many_arguments)]
    fn fast_levels(
        c: &mut Cursor<'_>,
        kind: EventKind,
        instrument: u32,
        venue_seq: u64,
        venue_ts_ns: u64,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<(), CodecError> {
        if c.eat(b']') {
            return Ok(());
        }
        loop {
            if !c.eat(b'[') {
                return Err(CodecError::Structure("level: expected ["));
            }
            let p = c
                .read_string()
                .ok_or(CodecError::Structure("level: price"))?;
            if !c.eat(b',') {
                return Err(CodecError::Structure("level: expected ,"));
            }
            let q = c.read_string().ok_or(CodecError::Structure("level: qty"))?;
            if !c.eat(b']') {
                return Err(CodecError::Structure("level: expected ]"));
            }
            out.push(Self::ev(
                kind,
                instrument,
                parse_fixed(p)?,
                parse_fixed(q)?,
                0,
                venue_seq,
                venue_ts_ns,
                0,
                recv_mono_ns,
                recv_wall_ns,
            ));
            if c.eat(b',') {
                continue;
            }
            if c.eat(b']') {
                return Ok(());
            }
            return Err(CodecError::Structure("level: expected , or ]"));
        }
    }

    /// Fast path for one frame (events appended from `start`; the caller
    /// truncates on error and updates stats on success).
    fn fast_inner(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
        start: usize,
    ) -> Result<Signal, CodecError> {
        let mut c = Cursor::new(payload);
        c.skip_ws();
        let head = c.rest();
        if head.starts_with(b"{\"result\":") {
            if head.starts_with(CONTROL_PREFIX) {
                let mut cc = Cursor::new(&head[CONTROL_PREFIX.len()..]);
                if cc.read_u64().is_some() && cc.eat(b'}') {
                    cc.skip_ws();
                    if cc.done() {
                        return Ok(Signal::Control);
                    }
                }
            }
            return Err(CodecError::Structure("control: unexpected shape"));
        }
        if !head.starts_with(b"{\"stream\":\"") {
            return Err(CodecError::Structure("envelope: no stream"));
        }
        c.skip_past_finder(&self.f_data_e)
            .ok_or(CodecError::Structure("envelope: no data.e"))?;
        let rest = c.rest();
        if rest.starts_with(b"depthUpdate\"") {
            self.fast_depth(c, recv_mono_ns, recv_wall_ns, out, start)
        } else if rest.starts_with(b"trade\"") {
            self.fast_trade(c, recv_mono_ns, recv_wall_ns, out)
        } else {
            // Recognized envelope, event type we don't handle.
            Ok(Signal::Ignored)
        }
    }

    /// Fast path for a `depthUpdate` payload; `c` sits just past `"e":"`.
    fn fast_depth(
        &mut self,
        mut c: Cursor<'_>,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
        start: usize,
    ) -> Result<Signal, CodecError> {
        c.skip_past_finder(&self.f_ecap)
            .ok_or(CodecError::Structure("depth: no E"))?;
        let e_ms = Self::read_u64_field(&mut c, "depth: E")?;
        c.skip_past_finder(&self.f_s)
            .ok_or(CodecError::Structure("depth: no s"))?;
        let sym = c.read_string().ok_or(CodecError::Structure("depth: s"))?;
        let instrument = self
            .table
            .lookup(sym)
            .ok_or(CodecError::UnknownInstrument)?;
        c.skip_past_finder(&self.f_ucap)
            .ok_or(CodecError::Structure("depth: no U"))?;
        let first_u = Self::read_u64_field(&mut c, "depth: U")?;
        c.skip_past_finder(&self.f_ulow)
            .ok_or(CodecError::Structure("depth: no u"))?;
        let final_u = Self::read_u64_field(&mut c, "depth: u")?;
        let venue_ts_ns = e_ms.saturating_mul(1_000_000);
        c.skip_past_finder(&self.f_b)
            .ok_or(CodecError::Structure("depth: no b"))?;
        Self::fast_levels(
            &mut c,
            EventKind::BidSet,
            instrument,
            final_u,
            venue_ts_ns,
            recv_mono_ns,
            recv_wall_ns,
            out,
        )?;
        c.skip_past_finder(&self.f_a)
            .ok_or(CodecError::Structure("depth: no a"))?;
        Self::fast_levels(
            &mut c,
            EventKind::AskSet,
            instrument,
            final_u,
            venue_ts_ns,
            recv_mono_ns,
            recv_wall_ns,
            out,
        )?;
        self.finish_depth(
            instrument,
            first_u,
            final_u,
            venue_ts_ns,
            recv_mono_ns,
            recv_wall_ns,
            out,
            start,
        )
    }

    /// Fast path for a `trade` payload; `c` sits just past `"e":"`.
    fn fast_trade(
        &mut self,
        mut c: Cursor<'_>,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        c.skip_past_finder(&self.f_s)
            .ok_or(CodecError::Structure("trade: no s"))?;
        let sym = c.read_string().ok_or(CodecError::Structure("trade: s"))?;
        let instrument = self
            .table
            .lookup(sym)
            .ok_or(CodecError::UnknownInstrument)?;
        c.skip_past_finder(&self.f_t)
            .ok_or(CodecError::Structure("trade: no t"))?;
        let trade_id = Self::read_u64_field(&mut c, "trade: t")?;
        c.skip_past_finder(&self.f_p)
            .ok_or(CodecError::Structure("trade: no p"))?;
        let p = c.read_string().ok_or(CodecError::Structure("trade: p"))?;
        c.skip_past_finder(&self.f_q)
            .ok_or(CodecError::Structure("trade: no q"))?;
        let q = c.read_string().ok_or(CodecError::Structure("trade: q"))?;
        c.skip_past_finder(&self.f_tcap)
            .ok_or(CodecError::Structure("trade: no T"))?;
        let t_ms = Self::read_u64_field(&mut c, "trade: T")?;
        c.skip_past_finder(&self.f_m)
            .ok_or(CodecError::Structure("trade: no m"))?;
        let taker_sell = if c.rest().starts_with(b"true") {
            true
        } else if c.rest().starts_with(b"false") {
            false
        } else {
            return Err(CodecError::Structure("trade: m"));
        };
        self.finish_trade(
            instrument,
            trade_id,
            parse_fixed(p)?,
            parse_fixed(q)?,
            t_ms.saturating_mul(1_000_000),
            taker_sell,
            recv_mono_ns,
            recv_wall_ns,
            out,
        )
    }

    /// serde_json reference path for one frame.
    fn slow_inner(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
        start: usize,
    ) -> Result<Signal, CodecError> {
        let v: serde_json::Value = serde_json::from_slice(payload)
            .map_err(|_| CodecError::Structure("slow: invalid json"))?;
        let obj = v
            .as_object()
            .ok_or(CodecError::Structure("slow: not an object"))?;
        if let Some(result) = obj.get("result") {
            let ok =
                result.is_null() && obj.get("id").and_then(serde_json::Value::as_u64).is_some();
            return if ok {
                Ok(Signal::Control)
            } else {
                Err(CodecError::Structure("slow: control shape"))
            };
        }
        obj.get("stream")
            .and_then(serde_json::Value::as_str)
            .ok_or(CodecError::Structure("slow: no stream"))?;
        let data = obj
            .get("data")
            .and_then(serde_json::Value::as_object)
            .ok_or(CodecError::Structure("slow: no data"))?;
        let Some(e) = data.get("e").and_then(serde_json::Value::as_str) else {
            return Ok(Signal::Ignored);
        };
        match e {
            "depthUpdate" => {
                let e_ms = data
                    .get("E")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(CodecError::Structure("slow depth: E"))?;
                let sym = data
                    .get("s")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(CodecError::Structure("slow depth: s"))?;
                let instrument = self
                    .table
                    .lookup(sym.as_bytes())
                    .ok_or(CodecError::UnknownInstrument)?;
                let first_u = data
                    .get("U")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(CodecError::Structure("slow depth: U"))?;
                let final_u = data
                    .get("u")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(CodecError::Structure("slow depth: u"))?;
                let venue_ts_ns = e_ms.saturating_mul(1_000_000);
                for (key, kind) in [("b", EventKind::BidSet), ("a", EventKind::AskSet)] {
                    let levels = data
                        .get(key)
                        .and_then(serde_json::Value::as_array)
                        .ok_or(CodecError::Structure("slow depth: levels"))?;
                    for level in levels {
                        let (price, qty) = Self::slow_level(level)?;
                        out.push(Self::ev(
                            kind,
                            instrument,
                            price,
                            qty,
                            0,
                            final_u,
                            venue_ts_ns,
                            0,
                            recv_mono_ns,
                            recv_wall_ns,
                        ));
                    }
                }
                self.finish_depth(
                    instrument,
                    first_u,
                    final_u,
                    venue_ts_ns,
                    recv_mono_ns,
                    recv_wall_ns,
                    out,
                    start,
                )
            }
            "trade" => {
                let sym = data
                    .get("s")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(CodecError::Structure("slow trade: s"))?;
                let instrument = self
                    .table
                    .lookup(sym.as_bytes())
                    .ok_or(CodecError::UnknownInstrument)?;
                let trade_id = data
                    .get("t")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(CodecError::Structure("slow trade: t"))?;
                let p = data
                    .get("p")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(CodecError::Structure("slow trade: p"))?;
                let q = data
                    .get("q")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(CodecError::Structure("slow trade: q"))?;
                let t_ms = data
                    .get("T")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(CodecError::Structure("slow trade: T"))?;
                let taker_sell = data
                    .get("m")
                    .and_then(serde_json::Value::as_bool)
                    .ok_or(CodecError::Structure("slow trade: m"))?;
                self.finish_trade(
                    instrument,
                    trade_id,
                    parse_fixed(p.as_bytes())?,
                    parse_fixed(q.as_bytes())?,
                    t_ms.saturating_mul(1_000_000),
                    taker_sell,
                    recv_mono_ns,
                    recv_wall_ns,
                    out,
                )
            }
            _ => Ok(Signal::Ignored),
        }
    }

    /// Extract one `["price","qty"]` level from a DOM value.
    fn slow_level(level: &serde_json::Value) -> Result<(i64, i64), CodecError> {
        let pair = level
            .as_array()
            .ok_or(CodecError::Structure("slow level: not array"))?;
        if pair.len() != 2 {
            return Err(CodecError::Structure("slow level: arity"));
        }
        let p = pair[0]
            .as_str()
            .ok_or(CodecError::Structure("slow level: price"))?;
        let q = pair[1]
            .as_str()
            .ok_or(CodecError::Structure("slow level: qty"))?;
        Ok((parse_fixed(p.as_bytes())?, parse_fixed(q.as_bytes())?))
    }

    /// REST `/api/v3/depth` body -> snapshot event run + `Synced` anchor.
    fn snapshot_inner(
        &mut self,
        instrument: u32,
        body: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let idx = self
            .state_index(instrument)
            .ok_or(CodecError::UnknownInstrument)?;
        let v: serde_json::Value = serde_json::from_slice(body)
            .map_err(|_| CodecError::Structure("snapshot: invalid json"))?;
        let last_update_id = v
            .get("lastUpdateId")
            .and_then(serde_json::Value::as_u64)
            .ok_or(CodecError::Structure("snapshot: lastUpdateId"))?;
        let bids = v
            .get("bids")
            .and_then(serde_json::Value::as_array)
            .ok_or(CodecError::Structure("snapshot: bids"))?;
        let asks = v
            .get("asks")
            .and_then(serde_json::Value::as_array)
            .ok_or(CodecError::Structure("snapshot: asks"))?;
        let snap_flags = flags::FROM_SNAPSHOT | flags::SYNTHETIC;
        let mk = |kind: EventKind, price: i64, qty: i64, aux: u64| {
            Self::ev(
                kind,
                instrument,
                price,
                qty,
                aux,
                last_update_id,
                0,
                snap_flags,
                recv_mono_ns,
                recv_wall_ns,
            )
        };
        out.push(mk(EventKind::Clear, 0, 0, 0));
        out.push(mk(
            EventKind::SnapBegin,
            0,
            0,
            (bids.len() + asks.len()) as u64,
        ));
        for (levels, kind) in [(bids, EventKind::SnapBid), (asks, EventKind::SnapAsk)] {
            for level in levels {
                let (price, qty) = Self::slow_level(level)?;
                out.push(mk(kind, price, qty, 0));
            }
        }
        out.push(mk(EventKind::SnapEnd, 0, 0, 0));
        self.states[idx].depth = DepthState::Synced {
            last_u: last_update_id,
        };
        Ok(Signal::None)
    }
}

impl VenueCodec for BinanceCodec {
    fn venue(&self) -> Venue {
        Venue::Binance
    }

    fn ws_url(&self) -> String {
        let streams: Vec<String> = self
            .table
            .iter()
            .map(|(sym, _)| String::from_utf8_lossy(sym).to_lowercase())
            .flat_map(|s| [format!("{s}@depth@100ms"), format!("{s}@trade")])
            .collect();
        format!(
            "wss://stream.binance.com:9443/stream?streams={}",
            streams.join("/")
        )
    }

    fn subscribe_messages(&self) -> Vec<String> {
        Vec::new()
    }

    fn parse(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let start = out.len();
        match self.fast_inner(payload, recv_mono_ns, recv_wall_ns, out, start) {
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
        match self.slow_inner(payload, recv_mono_ns, recv_wall_ns, out, start) {
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
        match self.snapshot_inner(instrument, body, recv_mono_ns, recv_wall_ns, out) {
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
