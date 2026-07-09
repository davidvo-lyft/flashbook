//! Kraken spot WebSocket v2 codec.
//!
//! Channels: `book` (snapshot + update; every book message carries a CRC32
//! checksum over the top-10 levels, verified by
//! [`flashbook_proto::kraken_crc::kraken_book_crc32`]) and `trade`.
//! Control plane: `status`, method acks, `heartbeat`.
//!
//! Kraken v2 has no per-message sequence numbers; book integrity is
//! guaranteed by the per-message checksum. A mismatch (detected downstream
//! by the LOB engine) is repaired by re-subscribing, so this codec never
//! emits `Gap` events and there is no REST resync path.
//!
//! Prices and quantities arrive as JSON *number* tokens (e.g. `63501.6`,
//! `0.00005100` — Kraken emits trailing zeros). Both paths hand the raw
//! token bytes to [`parse_fixed`]; nothing ever goes through `f64`. The
//! workspace's serde_json has neither `raw_value` nor `arbitrary_precision`
//! enabled, so `parse_slow` pre-quotes the `"price":`/`"qty":` number tokens
//! (a pure textual transform) and deserializes them as strings, feeding the
//! untouched token bytes to [`parse_fixed`].

use flashbook_proto::event::flags;
use flashbook_proto::{Event, EventKind, Venue, parse_fixed};
use memchr::memmem::Finder;

use crate::codec::{CodecError, CodecStats, Signal, SymbolTable, VenueCodec};
use crate::scan::{Cursor, parse_rfc3339_ns};

/// Venue (price, quantity) decimal precisions for the subscribed pairs, as
/// required by the book checksum. Values fetched from Kraken's
/// `/0/public/AssetPairs` (`pair_decimals`, `lot_decimals`) on 2026-07-07.
///
/// NOTE: this table is a point-in-time snapshot — Kraken can change a
/// pair's precisions (it has repriced `pair_decimals` before). If a value
/// drifts, the CRC oracle is the tripwire: every book message's checksum is
/// recomputed downstream from these decimals, so a change surfaces
/// immediately as persistent checksum mismatches for that pair. On such a
/// mismatch storm, re-fetch `/0/public/AssetPairs` and update this table.
pub fn pair_decimals(venue_symbol: &str) -> Option<(u32, u32)> {
    Some(match venue_symbol {
        "BTC/USD" => (1, 8),
        "ETH/USD" | "SOL/USD" => (2, 8),
        "XRP/USD" => (5, 8),
        "DOGE/USD" => (7, 8),
        _ => return None,
    })
}

/// Kraken REST endpoint the precision tripwire fetches (public, no auth).
pub const ASSET_PAIRS_URL: &str = "https://api.kraken.com/0/public/AssetPairs";

/// Map a WS v2 modern pair name to the v1 `wsname` used by the REST
/// `AssetPairs` response. Kraken's REST layer still speaks the legacy
/// currency codes for two of our bases: `BTC` is `XBT` and `DOGE` is `XDG`
/// (e.g. v2 `BTC/USD` appears as `wsname` `XBT/USD`); every other code we
/// subscribe to is identical on both sides.
pub fn v1_wsname(v2_symbol: &str) -> String {
    v2_symbol
        .split('/')
        .map(|code| match code {
            "BTC" => "XBT",
            "DOGE" => "XDG",
            other => other,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Compare the pinned [`pair_decimals`] snapshot against a raw
/// `/0/public/AssetPairs` response body (the pure-logic half of
/// [`verify_pair_decimals`], directly testable on fixture JSON).
///
/// `pinned` is `(v2 symbol, (pair_decimals, lot_decimals))` per subscribed
/// pair; lookup into the response goes through [`v1_wsname`]. Returns one
/// human-readable drift line per missing pair or changed precision — an
/// empty vec means the pinned snapshot still matches the venue. `Err` means
/// the response could not be interpreted at all (bad JSON, venue error,
/// missing fields), which callers must treat as "check unavailable", never
/// as drift.
pub fn check_pair_decimals(
    assetpairs_body: &str,
    pinned: &[(String, (u32, u32))],
) -> Result<Vec<String>, String> {
    let v: serde_json::Value =
        serde_json::from_str(assetpairs_body).map_err(|e| format!("AssetPairs: bad JSON: {e}"))?;
    if let Some(errs) = v.get("error").and_then(serde_json::Value::as_array)
        && !errs.is_empty()
    {
        return Err(format!("AssetPairs: venue error: {errs:?}"));
    }
    let result = v
        .get("result")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "AssetPairs: missing result object".to_string())?;
    let mut by_wsname = std::collections::HashMap::new();
    for pair in result.values() {
        // Entries without a wsname (if any) can't correspond to a WS pair.
        let Some(wsname) = pair.get("wsname").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let pd = pair
            .get("pair_decimals")
            .and_then(serde_json::Value::as_u64);
        let ld = pair.get("lot_decimals").and_then(serde_json::Value::as_u64);
        if let (Some(pd), Some(ld)) = (pd, ld) {
            by_wsname.insert(wsname.to_string(), (pd, ld));
        }
    }
    let mut drifts = Vec::new();
    for (symbol, (pin_pd, pin_ld)) in pinned {
        let wsname = v1_wsname(symbol);
        match by_wsname.get(&wsname) {
            None => drifts.push(format!(
                "{symbol} (wsname {wsname}): missing from AssetPairs response"
            )),
            Some(&(pd, ld)) => {
                if pd != u64::from(*pin_pd) {
                    drifts.push(format!(
                        "{symbol} (wsname {wsname}): pair_decimals pinned {pin_pd}, venue now {pd}"
                    ));
                }
                if ld != u64::from(*pin_ld) {
                    drifts.push(format!(
                        "{symbol} (wsname {wsname}): lot_decimals pinned {pin_ld}, venue now {ld}"
                    ));
                }
            }
        }
    }
    Ok(drifts)
}

/// Precision-drift tripwire: fetch [`ASSET_PAIRS_URL`] and compare the
/// venue's current `pair_decimals`/`lot_decimals` against the pinned
/// snapshot via [`check_pair_decimals`].
///
/// Returns drift descriptions (empty = pinned table matches the venue).
/// This is an *early warning* only — the CRC oracle remains the hard
/// tripwire: a real precision change also surfaces immediately as
/// persistent checksum mismatches on the affected pair. Network or parse
/// failure is an `Err` and must be non-fatal for callers.
pub async fn verify_pair_decimals(
    client: &reqwest::Client,
    symbols: &[(String, (u32, u32))],
) -> Result<Vec<String>, String> {
    let resp = client
        .get(ASSET_PAIRS_URL)
        .send()
        .await
        .map_err(|e| format!("AssetPairs: fetch: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("AssetPairs: HTTP {status}"));
    }
    let text = resp
        .text()
        .await
        .map_err(|e| format!("AssetPairs: read body: {e}"))?;
    check_pair_decimals(&text, symbols)
}

/// Book depth we subscribe at. The checksum always covers only the top 10
/// levels, but a deeper book survives longer between resyncs.
const BOOK_DEPTH: u32 = 100;

/// Consume `lit` if the cursor is exactly at it.
#[inline]
fn eat_lit(c: &mut Cursor<'_>, lit: &[u8]) -> bool {
    if c.rest().starts_with(lit) {
        c.set_pos(c.pos() + lit.len());
        true
    } else {
        false
    }
}

/// Kraken spot v2 codec. One instance per WS connection.
#[derive(Debug)]
pub struct KrakenCodec {
    table: SymbolTable,
    stats: CodecStats,
    f_type: Finder<'static>,
    f_data: Finder<'static>,
    f_symbol: Finder<'static>,
}

impl KrakenCodec {
    /// Build a codec over the subscribed symbol universe.
    pub fn new(table: SymbolTable) -> Self {
        Self {
            table,
            stats: CodecStats::default(),
            f_type: Finder::new(b"\"type\":"),
            f_data: Finder::new(b"\"data\":"),
            f_symbol: Finder::new(b"\"symbol\":"),
        }
    }

    /// Blank event carrying the frame's receive timestamps.
    #[inline]
    fn ev(kind: EventKind, instrument: u32, recv_mono_ns: u64, recv_wall_ns: u64) -> Event {
        Event {
            recv_mono_ns,
            recv_wall_ns,
            instrument,
            kind: kind as u8,
            venue: Venue::Kraken as u8,
            ..Event::ZERO
        }
    }

    /// Fast path body; `parse` wraps it with stats + buffer-restore.
    fn fast_inner(
        &self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let mut c = Cursor::new(payload);
        c.skip_ws();
        let rest = c.rest();
        if rest.starts_with(b"{\"channel\":\"book\"") {
            self.fast_book(&mut c, recv_mono_ns, recv_wall_ns, out)
        } else if rest.starts_with(b"{\"channel\":\"trade\"") {
            self.fast_trade(&mut c, recv_mono_ns, recv_wall_ns, out)
        } else if rest.starts_with(b"{\"channel\":\"heartbeat\"") {
            out.push(Self::ev(
                EventKind::Heartbeat,
                0,
                recv_mono_ns,
                recv_wall_ns,
            ));
            Ok(Signal::None)
        } else if rest.starts_with(b"{\"channel\":\"status\"") || rest.starts_with(b"{\"method\":")
        {
            Ok(Signal::Control)
        } else if rest.starts_with(b"{\"channel\":\"") {
            Ok(Signal::Ignored)
        } else {
            Err(CodecError::Structure("unrecognized frame"))
        }
    }

    /// `book` snapshot/update message (cursor at `{`).
    fn fast_book(
        &self,
        c: &mut Cursor<'_>,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let is_snapshot = Self::fast_msg_type(c, &self.f_type)?;
        c.skip_past_finder(&self.f_data)
            .ok_or(CodecError::Structure("book data"))?;
        if !c.eat(b'[') {
            return Err(CodecError::Structure("book data array"));
        }
        let mut first_entry = true;
        loop {
            c.skip_ws();
            if c.eat(b']') {
                break;
            }
            if !first_entry && !c.eat(b',') {
                return Err(CodecError::Structure("book entry separator"));
            }
            first_entry = false;
            c.skip_ws();
            if !c.eat(b'{') {
                return Err(CodecError::Structure("book entry"));
            }
            c.skip_past_finder(&self.f_symbol)
                .ok_or(CodecError::Structure("book symbol"))?;
            let sym = c
                .read_string()
                .ok_or(CodecError::Structure("book symbol string"))?;
            let instrument = self
                .table
                .lookup(sym)
                .ok_or(CodecError::UnknownInstrument)?;

            let first = out.len();
            let (bid_kind, ask_kind, fl) = if is_snapshot {
                out.push(Self::snap_ev(
                    EventKind::Clear,
                    instrument,
                    recv_mono_ns,
                    recv_wall_ns,
                ));
                out.push(Self::snap_ev(
                    EventKind::SnapBegin,
                    instrument,
                    recv_mono_ns,
                    recv_wall_ns,
                ));
                (EventKind::SnapBid, EventKind::SnapAsk, flags::FROM_SNAPSHOT)
            } else {
                (EventKind::BidSet, EventKind::AskSet, 0)
            };

            if !eat_lit(c, b",\"bids\":[") {
                return Err(CodecError::Structure("book bids"));
            }
            let nb =
                Self::fast_levels(c, bid_kind, fl, instrument, recv_mono_ns, recv_wall_ns, out)?;
            if !eat_lit(c, b",\"asks\":[") {
                return Err(CodecError::Structure("book asks"));
            }
            let na =
                Self::fast_levels(c, ask_kind, fl, instrument, recv_mono_ns, recv_wall_ns, out)?;

            if !eat_lit(c, b",\"checksum\":") {
                return Err(CodecError::Structure("book checksum"));
            }
            let checksum = c
                .read_u64()
                .ok_or(CodecError::Structure("book checksum value"))?;

            // Optional trailing timestamp (updates always carry one; some
            // snapshot captures do too — for snapshots it is skipped and
            // venue_ts stays 0, matching the convention).
            let mut venue_ts: Option<u64> = None;
            if eat_lit(c, b",\"timestamp\":") {
                let ts = c
                    .read_string()
                    .ok_or(CodecError::Structure("book timestamp"))?;
                if !is_snapshot {
                    venue_ts =
                        Some(parse_rfc3339_ns(ts).ok_or(CodecError::Structure("bad timestamp"))?);
                }
            }
            c.skip_ws();
            if !c.eat(b'}') {
                return Err(CodecError::Structure("book entry end"));
            }

            if is_snapshot {
                out[first + 1].aux = nb + na;
                let mut end =
                    Self::snap_ev(EventKind::SnapEnd, instrument, recv_mono_ns, recv_wall_ns);
                end.aux = checksum;
                out.push(end);
            } else {
                let ts = venue_ts.ok_or(CodecError::Structure("update missing timestamp"))?;
                let mut ck = Self::ev(EventKind::Checksum, instrument, recv_mono_ns, recv_wall_ns);
                ck.aux = checksum;
                out.push(ck);
                for e in &mut out[first..] {
                    e.venue_ts_ns = ts;
                }
            }
        }
        Ok(Signal::None)
    }

    /// One `[{"price":N,"qty":N},...]` levels array (cursor just past `[`).
    /// Returns the level count.
    fn fast_levels(
        c: &mut Cursor<'_>,
        kind: EventKind,
        fl: u8,
        instrument: u32,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<u64, CodecError> {
        let mut n = 0u64;
        loop {
            c.skip_ws();
            if c.eat(b']') {
                return Ok(n);
            }
            if n > 0 && !c.eat(b',') {
                return Err(CodecError::Structure("level separator"));
            }
            c.skip_ws();
            if !eat_lit(c, b"{\"price\":") {
                return Err(CodecError::Structure("level price"));
            }
            let p = c
                .read_number()
                .ok_or(CodecError::Structure("price token"))?;
            if !eat_lit(c, b",\"qty\":") {
                return Err(CodecError::Structure("level qty"));
            }
            let q = c.read_number().ok_or(CodecError::Structure("qty token"))?;
            if !c.eat(b'}') {
                return Err(CodecError::Structure("level end"));
            }
            let mut e = Self::ev(kind, instrument, recv_mono_ns, recv_wall_ns);
            e.price = parse_fixed(p)?;
            e.qty = parse_fixed(q)?;
            e.flags = fl;
            out.push(e);
            n += 1;
        }
    }

    /// `trade` snapshot/update message (cursor at `{`).
    fn fast_trade(
        &self,
        c: &mut Cursor<'_>,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let is_snapshot = Self::fast_msg_type(c, &self.f_type)?;
        let fl = if is_snapshot { flags::FROM_SNAPSHOT } else { 0 };
        c.skip_past_finder(&self.f_data)
            .ok_or(CodecError::Structure("trade data"))?;
        if !c.eat(b'[') {
            return Err(CodecError::Structure("trade data array"));
        }
        let mut first_entry = true;
        loop {
            c.skip_ws();
            if c.eat(b']') {
                break;
            }
            if !first_entry && !c.eat(b',') {
                return Err(CodecError::Structure("trade separator"));
            }
            first_entry = false;
            c.skip_ws();
            if !c.eat(b'{') || !eat_lit(c, b"\"symbol\":") {
                return Err(CodecError::Structure("trade symbol"));
            }
            let sym = c
                .read_string()
                .ok_or(CodecError::Structure("trade symbol string"))?;
            let instrument = self
                .table
                .lookup(sym)
                .ok_or(CodecError::UnknownInstrument)?;
            if !eat_lit(c, b",\"side\":") {
                return Err(CodecError::Structure("trade side"));
            }
            let side = c
                .read_string()
                .ok_or(CodecError::Structure("trade side string"))?;
            let taker_sell = match side {
                b"sell" => true,
                b"buy" => false,
                _ => return Err(CodecError::Structure("trade side value")),
            };
            if !eat_lit(c, b",\"price\":") {
                return Err(CodecError::Structure("trade price"));
            }
            let p = c
                .read_number()
                .ok_or(CodecError::Structure("price token"))?;
            if !eat_lit(c, b",\"qty\":") {
                return Err(CodecError::Structure("trade qty"));
            }
            let q = c.read_number().ok_or(CodecError::Structure("qty token"))?;
            if !eat_lit(c, b",\"ord_type\":") {
                return Err(CodecError::Structure("trade ord_type"));
            }
            c.read_string()
                .ok_or(CodecError::Structure("ord_type string"))?;
            if !eat_lit(c, b",\"trade_id\":") {
                return Err(CodecError::Structure("trade id"));
            }
            let trade_id = c
                .read_u64()
                .ok_or(CodecError::Structure("trade id value"))?;
            if !eat_lit(c, b",\"timestamp\":") {
                return Err(CodecError::Structure("trade timestamp"));
            }
            let ts = c
                .read_string()
                .ok_or(CodecError::Structure("timestamp string"))?;
            let venue_ts = parse_rfc3339_ns(ts).ok_or(CodecError::Structure("bad timestamp"))?;
            c.skip_ws();
            if !c.eat(b'}') {
                return Err(CodecError::Structure("trade entry end"));
            }

            let mut e = Self::ev(EventKind::Trade, instrument, recv_mono_ns, recv_wall_ns);
            e.venue_ts_ns = venue_ts;
            e.venue_seq = trade_id;
            e.aux = trade_id;
            e.price = parse_fixed(p)?;
            e.qty = parse_fixed(q)?;
            e.flags = fl | if taker_sell { flags::TAKER_SELL } else { 0 };
            out.push(e);
        }
        Ok(Signal::None)
    }

    /// Read `"type":"snapshot"|"update"`; true for snapshot.
    fn fast_msg_type(c: &mut Cursor<'_>, f_type: &Finder<'static>) -> Result<bool, CodecError> {
        c.skip_past_finder(f_type)
            .ok_or(CodecError::Structure("msg type"))?;
        match c.read_string() {
            Some(b"snapshot") => Ok(true),
            Some(b"update") => Ok(false),
            _ => Err(CodecError::Structure("msg type value")),
        }
    }

    /// Blank snapshot event (FROM_SNAPSHOT, venue_ts 0, venue_seq 0).
    #[inline]
    fn snap_ev(kind: EventKind, instrument: u32, recv_mono_ns: u64, recv_wall_ns: u64) -> Event {
        let mut e = Self::ev(kind, instrument, recv_mono_ns, recv_wall_ns);
        e.flags = flags::FROM_SNAPSHOT;
        e
    }

    /// Slow path body; `parse_slow` wraps it with stats + buffer-restore.
    fn slow_inner(
        &self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let text = std::str::from_utf8(payload).map_err(|_| CodecError::Structure("not utf-8"))?;
        let header: SlowHeader<'_> =
            serde_json::from_str(text).map_err(|_| CodecError::Structure("envelope"))?;
        if header.method.is_some() {
            return Ok(Signal::Control);
        }
        match header.channel {
            Some("status") => Ok(Signal::Control),
            Some("heartbeat") => {
                out.push(Self::ev(
                    EventKind::Heartbeat,
                    0,
                    recv_mono_ns,
                    recv_wall_ns,
                ));
                Ok(Signal::None)
            }
            Some("book") => self.slow_book(text, &header, recv_mono_ns, recv_wall_ns, out),
            Some("trade") => self.slow_trade(text, &header, recv_mono_ns, recv_wall_ns, out),
            Some(_) => Ok(Signal::Ignored),
            None => Err(CodecError::Structure("no channel or method")),
        }
    }

    fn slow_book(
        &self,
        text: &str,
        header: &SlowHeader<'_>,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let is_snapshot = Self::slow_msg_type(header)?;
        let quoted = quote_numeric_tokens(text);
        let msg: SlowBookMsg<'_> =
            serde_json::from_str(&quoted).map_err(|_| CodecError::Structure("book entries"))?;
        for entry in &msg.data {
            let instrument = self
                .table
                .lookup(entry.symbol.as_bytes())
                .ok_or(CodecError::UnknownInstrument)?;
            let venue_ts = if is_snapshot {
                0
            } else {
                let ts = entry
                    .timestamp
                    .ok_or(CodecError::Structure("update missing timestamp"))?;
                parse_rfc3339_ns(ts.as_bytes()).ok_or(CodecError::Structure("bad timestamp"))?
            };
            if is_snapshot {
                out.push(Self::snap_ev(
                    EventKind::Clear,
                    instrument,
                    recv_mono_ns,
                    recv_wall_ns,
                ));
                let mut begin =
                    Self::snap_ev(EventKind::SnapBegin, instrument, recv_mono_ns, recv_wall_ns);
                begin.aux = (entry.bids.len() + entry.asks.len()) as u64;
                out.push(begin);
            }
            let (bid_kind, ask_kind, fl) = if is_snapshot {
                (EventKind::SnapBid, EventKind::SnapAsk, flags::FROM_SNAPSHOT)
            } else {
                (EventKind::BidSet, EventKind::AskSet, 0)
            };
            for (kind, levels) in [(bid_kind, &entry.bids), (ask_kind, &entry.asks)] {
                for lvl in levels {
                    let mut e = Self::ev(kind, instrument, recv_mono_ns, recv_wall_ns);
                    e.venue_ts_ns = venue_ts;
                    e.price = parse_fixed(lvl.price.as_bytes())?;
                    e.qty = parse_fixed(lvl.qty.as_bytes())?;
                    e.flags = fl;
                    out.push(e);
                }
            }
            if is_snapshot {
                let mut end =
                    Self::snap_ev(EventKind::SnapEnd, instrument, recv_mono_ns, recv_wall_ns);
                end.aux = entry.checksum;
                out.push(end);
            } else {
                let mut ck = Self::ev(EventKind::Checksum, instrument, recv_mono_ns, recv_wall_ns);
                ck.venue_ts_ns = venue_ts;
                ck.aux = entry.checksum;
                out.push(ck);
            }
        }
        Ok(Signal::None)
    }

    fn slow_trade(
        &self,
        text: &str,
        header: &SlowHeader<'_>,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        let is_snapshot = Self::slow_msg_type(header)?;
        let fl = if is_snapshot { flags::FROM_SNAPSHOT } else { 0 };
        let quoted = quote_numeric_tokens(text);
        let msg: SlowTradeMsg<'_> =
            serde_json::from_str(&quoted).map_err(|_| CodecError::Structure("trade entries"))?;
        for entry in &msg.data {
            let instrument = self
                .table
                .lookup(entry.symbol.as_bytes())
                .ok_or(CodecError::UnknownInstrument)?;
            let taker_sell = match entry.side {
                "sell" => true,
                "buy" => false,
                _ => return Err(CodecError::Structure("trade side value")),
            };
            let venue_ts = parse_rfc3339_ns(entry.timestamp.as_bytes())
                .ok_or(CodecError::Structure("bad timestamp"))?;
            let mut e = Self::ev(EventKind::Trade, instrument, recv_mono_ns, recv_wall_ns);
            e.venue_ts_ns = venue_ts;
            e.venue_seq = entry.trade_id;
            e.aux = entry.trade_id;
            e.price = parse_fixed(entry.price.as_bytes())?;
            e.qty = parse_fixed(entry.qty.as_bytes())?;
            e.flags = fl | if taker_sell { flags::TAKER_SELL } else { 0 };
            out.push(e);
        }
        Ok(Signal::None)
    }

    /// `"type"` field of a data message; true for snapshot.
    fn slow_msg_type(header: &SlowHeader<'_>) -> Result<bool, CodecError> {
        match header.msg_type {
            Some("snapshot") => Ok(true),
            Some("update") => Ok(false),
            _ => Err(CodecError::Structure("msg type value")),
        }
    }
}

impl VenueCodec for KrakenCodec {
    fn venue(&self) -> Venue {
        Venue::Kraken
    }

    fn ws_url(&self) -> String {
        "wss://ws.kraken.com/v2".to_string()
    }

    fn subscribe_messages(&self) -> Vec<String> {
        let symbols: Vec<String> = self
            .table
            .iter()
            .map(|(s, _)| String::from_utf8_lossy(s).into_owned())
            .collect();
        vec![
            serde_json::json!({
                "method": "subscribe",
                "params": {"channel": "book", "symbol": symbols, "depth": BOOK_DEPTH},
            })
            .to_string(),
            // `"snapshot": false` matches the capture configuration the
            // fixtures were recorded with: no historical-trade snapshot on
            // subscribe, only live trades.
            serde_json::json!({
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": symbols, "snapshot": false},
            })
            .to_string(),
        ]
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

    fn stats(&self) -> &CodecStats {
        &self.stats
    }
}

/// Rewrite `"price":<number>` / `"qty":<number>` into `"price":"<number>"` /
/// `"qty":"<number>"` so serde_json hands the untouched decimal token back
/// as a string (the workspace serde_json lacks the `raw_value` feature, and
/// its `Number` would round-trip through `f64`). Legal JSON whitespace
/// between the key's colon and the number is emitted verbatim and the
/// number that follows it is still quoted. The transform is purely textual
/// and leaves everything else — including already-quoted or non-numeric
/// values — byte-for-byte intact. Slow path only; allocates.
fn quote_numeric_tokens(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 128);
    let mut rest = text;
    loop {
        let (at, key_len) = match (rest.find("\"price\":"), rest.find("\"qty\":")) {
            (Some(p), Some(q)) => {
                if p < q {
                    (p, "\"price\":".len())
                } else {
                    (q, "\"qty\":".len())
                }
            }
            (Some(p), None) => (p, "\"price\":".len()),
            (None, Some(q)) => (q, "\"qty\":".len()),
            (None, None) => {
                out.push_str(rest);
                return out;
            }
        };
        let (head, tail) = rest.split_at(at + key_len);
        out.push_str(head);
        // JSON allows whitespace after the colon; pass it through unchanged
        // so the value scan below starts at the token itself.
        let ws = tail
            .bytes()
            .take_while(|c| matches!(c, b' ' | b'\t' | b'\n' | b'\r'))
            .count();
        out.push_str(&tail[..ws]);
        let tail = &tail[ws..];
        let n = tail
            .bytes()
            .take_while(|c| matches!(c, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'))
            .count();
        if n > 0 {
            out.push('"');
            out.push_str(&tail[..n]);
            out.push('"');
        }
        rest = &tail[n..];
    }
}

/// Top-level v2 frame fields for slow-path dispatch (`data` is re-parsed
/// per channel from the quoted text).
#[derive(serde::Deserialize)]
struct SlowHeader<'a> {
    #[serde(borrow, default)]
    channel: Option<&'a str>,
    #[serde(borrow, default)]
    method: Option<&'a str>,
    #[serde(borrow, default, rename = "type")]
    msg_type: Option<&'a str>,
}

/// A `book` frame after [`quote_numeric_tokens`].
#[derive(serde::Deserialize)]
struct SlowBookMsg<'a> {
    #[serde(borrow)]
    data: Vec<SlowBookEntry<'a>>,
}

/// One `book` data entry (per symbol).
#[derive(serde::Deserialize)]
struct SlowBookEntry<'a> {
    #[serde(borrow)]
    symbol: &'a str,
    #[serde(borrow)]
    bids: Vec<SlowLevel<'a>>,
    #[serde(borrow)]
    asks: Vec<SlowLevel<'a>>,
    checksum: u64,
    #[serde(borrow, default)]
    timestamp: Option<&'a str>,
}

/// One price level; the quoted decimal tokens are fed verbatim to
/// [`parse_fixed`] so the slow path is exactly as lossless as the fast path.
#[derive(serde::Deserialize)]
struct SlowLevel<'a> {
    #[serde(borrow)]
    price: &'a str,
    #[serde(borrow)]
    qty: &'a str,
}

/// A `trade` frame after [`quote_numeric_tokens`].
#[derive(serde::Deserialize)]
struct SlowTradeMsg<'a> {
    #[serde(borrow)]
    data: Vec<SlowTradeEntry<'a>>,
}

/// One `trade` data entry.
#[derive(serde::Deserialize)]
struct SlowTradeEntry<'a> {
    #[serde(borrow)]
    symbol: &'a str,
    #[serde(borrow)]
    side: &'a str,
    #[serde(borrow)]
    price: &'a str,
    #[serde(borrow)]
    qty: &'a str,
    trade_id: u64,
    #[serde(borrow)]
    timestamp: &'a str,
}
