//! DuckDB / SQLite / Parquet head-to-head harness (feature `compare`).
//!
//! Correctness first, speed second: every query here has an "ours"
//! implementation over [`StoreReader`] and SQL implementations over the SAME
//! rows loaded into DuckDB and SQLite. Callers (bench-store, the integration
//! tests) assert the three backends return identical results before quoting
//! any timing.
//!
//! Methodology / fairness notes:
//!
//! - **Schema** ([`SCHEMA_SQL`]): one flat `events` table mirroring the
//!   64-byte [`Event`] record, identical text for both engines (SQLite maps
//!   the width names to INTEGER affinity; DuckDB takes them literally).
//! - **u64 columns**: `recv_mono/recv_wall/venue_ts/venue_seq/aux` are u64
//!   in [`Event`] but BIGINT (i64) in SQL. Values above `i64::MAX` cannot be
//!   represented; the loads *check* every value and fail rather than corrupt
//!   (our capture values — ns since boot / epoch ns / venue seqs / trade ids
//!   — are far below the limit).
//! - **DuckDB load**: the Appender API (DuckDB's documented bulk-load path),
//!   one flush at the end. No index is created: DuckDB's own zone-map /
//!   columnar machinery is its answer to range predicates.
//! - **SQLite load**: a single transaction with one prepared INSERT.
//!   Pragmas, all documented here, deviating from defaults for the LOAD
//!   only: `journal_mode=MEMORY` and `synchronous=OFF` (we do not benchmark
//!   SQLite's crash durability; queries afterwards are read-only and
//!   unaffected by either pragma). Everything else is SQLite defaults.
//!   After the inserts: `CREATE INDEX idx_inst_mono ON events(instrument,
//!   recv_mono)` + `ANALYZE`, charged to the load time — the PIT query needs
//!   that index, just as ours charges the snapshot-index build to ingest.
//! - **PIT parity**: the SQL anchor query (`max(recv_mono) WHERE instrument
//!   = ? AND kind = 4 AND recv_mono <= ?`; kind 4 = `SnapBegin`) can
//!   legitimately pick an INCOMPLETE snapshot — a dangling `SnapBegin` whose
//!   `SnapEnd` never arrived — which [`SnapshotIndex`] refuses by design.
//!   The SQL PIT functions therefore take ours' anchor, fold from it, and
//!   report when the naive SQL anchor disagreed ([`SqlPit::sql_anchor`] /
//!   [`SqlPit::anchor_diverged`]); correctness parity is the point, and the
//!   divergence count is published rather than hidden.
//! - **PIT fold**: identical Rust code for all three backends — events are
//!   applied to a fresh unbounded [`LadderBook`] and the resulting top of
//!   book compared. (No Kraken depth cap is applied in the fold; it is the
//!   same fold on every backend, which is what the parity assertion needs.)

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use flashbook_lob::{L2Book, LadderBook};
use flashbook_proto::event::{Event, EventKind};
use flashbook_store::pit::{SnapshotIndex, pit_scan};
use flashbook_store::segment::StoreReader;

/// Flat SQL schema for the `events` table, shared verbatim by both engines.
pub const SCHEMA_SQL: &str = "CREATE TABLE events(\
     recv_mono BIGINT, recv_wall BIGINT, venue_ts BIGINT, venue_seq BIGINT, \
     price BIGINT, qty BIGINT, aux BIGINT, \
     instrument INTEGER, kind TINYINT, venue INTEGER, flags INTEGER)";

/// The full-scan aggregate, identical text on both engines.
///
/// `sum(qty)` over the full corpus exceeds i64 (observed: 1.06e19 across
/// 226M events), which DuckDB refuses to down-cast and SQLite raises on.
/// The sum is therefore split into two never-overflowing BIGINT sums and
/// recombined as i128 on the Rust side — exact on every backend.
///
/// The split uses BIT ops, not `/` and `%`: DuckDB's `/` on integers is
/// FLOAT division (Postgres-style), which silently rounded the hi terms
/// (caught by the parity assertion on the first full-corpus run). `>>`
/// and `&` are integer ops with identical semantics on DuckDB, SQLite and
/// Rust for the non-negative `qty` domain.
const SCAN_SQL: &str = "SELECT instrument, count(*), \
     sum(qty >> 31), sum(qty & 2147483647), \
     min(price), max(price), max(recv_mono) \
     FROM events GROUP BY instrument ORDER BY instrument";

/// One row of the full-scan aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct ScanRow {
    /// Instrument id.
    pub instrument: u32,
    /// Event count.
    pub count: u64,
    /// Sum of `qty` mantissas (exceeds i64 on the full corpus; serialized
    /// as a string for JSON portability).
    #[serde(serialize_with = "ser_i128")]
    pub sum_qty: i128,
    /// Minimum `price` mantissa (0 events with meaningless price included —
    /// identically on every backend).
    pub min_price: i64,
    /// Maximum `price` mantissa.
    pub max_price: i64,
    /// Maximum `recv_mono_ns`.
    pub max_mono: u64,
}

/// Point-in-time top of book, plus the anchor it was folded from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PitTop {
    /// `recv_mono_ns` of the `SnapBegin` the fold started at (`None` = no
    /// complete snapshot at or before `t`; empty book).
    pub anchor_mono: Option<u64>,
    /// Best bid (price, qty).
    pub best_bid: Option<(i64, i64)>,
    /// Best ask (price, qty).
    pub best_ask: Option<(i64, i64)>,
}

/// Result of one SQL-backend PIT query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SqlPit {
    /// Wall seconds for the whole query (anchor + fetch + fold).
    pub seconds: f64,
    /// The folded top of book (from ours' anchor — see module notes).
    pub top: PitTop,
    /// What the naive SQL anchor query said (may point at an incomplete
    /// snapshot).
    pub sql_anchor: Option<u64>,
    /// True when the SQL anchor differed from ours' (incomplete-snapshot
    /// case); the fold still used ours' anchor.
    pub anchor_diverged: bool,
}

/// The 11 SQL columns of one event, in table order.
type SqlEventRow = (i64, i64, i64, i64, i64, i64, i64, i32, i8, i32, i32);

/// Checked u64 -> BIGINT conversion; values above `i64::MAX` are
/// unrepresentable and must fail the load, not wrap.
fn as_i64(v: u64, field: &'static str) -> Result<i64> {
    i64::try_from(v).with_context(|| format!("{field} value {v} exceeds i64::MAX (BIGINT)"))
}

/// An [`Event`] as its SQL row (checked narrowing).
fn sql_row(e: &Event) -> Result<SqlEventRow> {
    Ok((
        as_i64(e.recv_mono_ns, "recv_mono")?,
        as_i64(e.recv_wall_ns, "recv_wall")?,
        as_i64(e.venue_ts_ns, "venue_ts")?,
        as_i64(e.venue_seq, "venue_seq")?,
        e.price,
        e.qty,
        as_i64(e.aux, "aux")?,
        i32::try_from(e.instrument).context("instrument exceeds INTEGER")?,
        i8::try_from(e.kind).context("kind exceeds TINYINT")?,
        i32::from(e.venue),
        i32::from(e.flags),
    ))
}

/// A SQL row back into an [`Event`] (`rsvd` reconstructed as 0) — the
/// inverse of [`sql_row`], used by the SQL PIT folds.
fn event_from_sql(r: SqlEventRow) -> Result<Event> {
    Ok(Event {
        recv_mono_ns: u64::try_from(r.0).context("negative recv_mono")?,
        recv_wall_ns: u64::try_from(r.1).context("negative recv_wall")?,
        venue_ts_ns: u64::try_from(r.2).context("negative venue_ts")?,
        venue_seq: u64::try_from(r.3).context("negative venue_seq")?,
        price: r.4,
        qty: r.5,
        aux: u64::try_from(r.6).context("negative aux")?,
        instrument: u32::try_from(r.7).context("negative instrument")?,
        kind: u8::try_from(r.8).context("negative kind")?,
        venue: u8::try_from(r.9).context("venue out of u8")?,
        flags: u8::try_from(r.10).context("flags out of u8")?,
        rsvd: 0,
    })
}

/// Escape a path for embedding in a single-quoted SQL string literal.
fn sql_str_literal(path: &Path) -> Result<String> {
    let s = path
        .to_str()
        .context("path must be UTF-8 for SQL embedding")?;
    Ok(format!("'{}'", s.replace('\'', "''")))
}

/// Load every event of `reader` into a fresh DuckDB database at `path` via
/// the Appender API. Returns (load seconds, open connection). See the
/// module notes for methodology.
pub fn load_duckdb(reader: &StoreReader, path: &Path) -> Result<(f64, duckdb::Connection)> {
    let conn = duckdb::Connection::open(path).context("open duckdb")?;
    conn.execute_batch(SCHEMA_SQL).context("duckdb schema")?;
    let t0 = Instant::now();
    let mut first_err: Option<anyhow::Error> = None;
    {
        let mut app = conn.appender("events").context("duckdb appender")?;
        reader.scan(|e| {
            if first_err.is_some() {
                return;
            }
            let r = sql_row(e).and_then(|v| {
                app.append_row(duckdb::params![
                    v.0, v.1, v.2, v.3, v.4, v.5, v.6, v.7, v.8, v.9, v.10
                ])
                .context("duckdb append_row")
            });
            if let Err(e) = r {
                first_err = Some(e);
            }
        })?;
        app.flush().context("duckdb appender flush")?;
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok((t0.elapsed().as_secs_f64(), conn))
}

/// Load every event of `reader` into a fresh SQLite database at `path`:
/// one transaction, one prepared INSERT, then the `(instrument, recv_mono)`
/// index and `ANALYZE`. Returns (load seconds, open connection). Pragmas
/// (`journal_mode=MEMORY`, `synchronous=OFF`, load-only fairness) are
/// documented in the module notes.
pub fn load_sqlite(reader: &StoreReader, path: &Path) -> Result<(f64, rusqlite::Connection)> {
    let mut conn = rusqlite::Connection::open(path).context("open sqlite")?;
    // journal_mode returns its new value as a row; assert it applied.
    let mode: String = conn
        .query_row("PRAGMA journal_mode=MEMORY", [], |r| r.get(0))
        .context("sqlite journal_mode")?;
    if !mode.eq_ignore_ascii_case("memory") {
        bail!("journal_mode=MEMORY not applied (got {mode})");
    }
    conn.pragma_update(None, "synchronous", "OFF")
        .context("sqlite synchronous")?;
    conn.execute_batch(SCHEMA_SQL).context("sqlite schema")?;

    let t0 = Instant::now();
    let tx = conn.transaction().context("sqlite tx")?;
    let mut first_err: Option<anyhow::Error> = None;
    {
        let mut stmt = tx
            .prepare("INSERT INTO events VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)")
            .context("sqlite insert prepare")?;
        reader.scan(|e| {
            if first_err.is_some() {
                return;
            }
            let r = sql_row(e).and_then(|v| {
                stmt.execute(rusqlite::params![
                    v.0, v.1, v.2, v.3, v.4, v.5, v.6, v.7, v.8, v.9, v.10
                ])
                .context("sqlite insert")
                .map(|_| ())
            });
            if let Err(e) = r {
                first_err = Some(e);
            }
        })?;
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    tx.commit().context("sqlite commit")?;
    conn.execute_batch("CREATE INDEX idx_inst_mono ON events(instrument, recv_mono); ANALYZE;")
        .context("sqlite index+analyze")?;
    Ok((t0.elapsed().as_secs_f64(), conn))
}

/// `COPY events TO '<path>' (FORMAT PARQUET, COMPRESSION ZSTD)` from an
/// already-loaded DuckDB connection. Returns (seconds, parquet file bytes).
pub fn write_parquet_via_duckdb(conn: &duckdb::Connection, path: &Path) -> Result<(f64, u64)> {
    let lit = sql_str_literal(path)?;
    let t0 = Instant::now();
    conn.execute_batch(&format!(
        "COPY events TO {lit} (FORMAT PARQUET, COMPRESSION ZSTD)"
    ))
    .context("duckdb COPY TO parquet")?;
    let seconds = t0.elapsed().as_secs_f64();
    let bytes = std::fs::metadata(path).context("stat parquet")?.len();
    Ok((seconds, bytes))
}

/// The full-scan aggregate, ours: one hand-written fold over
/// [`StoreReader::scan`]. Returns (seconds, rows sorted by instrument).
pub fn full_scan_ours(reader: &StoreReader) -> Result<(f64, Vec<ScanRow>)> {
    let t0 = Instant::now();
    let mut acc: HashMap<u32, ScanRow> = HashMap::new();
    reader.scan(|e| {
        let r = acc.entry(e.instrument).or_insert(ScanRow {
            instrument: e.instrument,
            count: 0,
            sum_qty: 0,
            min_price: i64::MAX,
            max_price: i64::MIN,
            max_mono: 0,
        });
        r.count += 1;
        r.sum_qty += i128::from(e.qty);
        r.min_price = r.min_price.min(e.price);
        r.max_price = r.max_price.max(e.price);
        r.max_mono = r.max_mono.max(e.recv_mono_ns);
    })?;
    let mut rows: Vec<ScanRow> = acc.into_values().collect();
    rows.sort_unstable_by_key(|r| r.instrument);
    Ok((t0.elapsed().as_secs_f64(), rows))
}

/// Serialize an i128 as a decimal string (JSON number portability).
fn ser_i128<S: serde::Serializer>(v: &i128, s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

/// One raw aggregate row into a [`ScanRow`] (shared by both SQL backends):
/// recombines the hi/lo split sums exactly.
fn scan_row_from_sql(t: (i32, i64, i64, i64, i64, i64, i64)) -> Result<ScanRow> {
    Ok(ScanRow {
        instrument: u32::try_from(t.0).context("negative instrument")?,
        count: u64::try_from(t.1).context("negative count")?,
        sum_qty: i128::from(t.2) * 2_147_483_648i128 + i128::from(t.3),
        min_price: t.4,
        max_price: t.5,
        max_mono: u64::try_from(t.6).context("negative max recv_mono")?,
    })
}

/// The full-scan aggregate on DuckDB (one GROUP BY query).
pub fn full_scan_duckdb(conn: &duckdb::Connection) -> Result<(f64, Vec<ScanRow>)> {
    let t0 = Instant::now();
    let mut stmt = conn.prepare(SCAN_SQL).context("duckdb scan prepare")?;
    let raw: Vec<(i32, i64, i64, i64, i64, i64, i64)> = stmt
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        })
        .context("duckdb scan query")?
        .collect::<std::result::Result<_, _>>()
        .context("duckdb scan rows")?;
    let seconds = t0.elapsed().as_secs_f64();
    let rows = raw
        .into_iter()
        .map(scan_row_from_sql)
        .collect::<Result<Vec<_>>>()?;
    Ok((seconds, rows))
}

/// The full-scan aggregate on SQLite (one GROUP BY query).
pub fn full_scan_sqlite(conn: &rusqlite::Connection) -> Result<(f64, Vec<ScanRow>)> {
    let t0 = Instant::now();
    let mut stmt = conn.prepare(SCAN_SQL).context("sqlite scan prepare")?;
    let raw: Vec<(i32, i64, i64, i64, i64, i64, i64)> = stmt
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        })
        .context("sqlite scan query")?
        .collect::<std::result::Result<_, _>>()
        .context("sqlite scan rows")?;
    let seconds = t0.elapsed().as_secs_f64();
    let rows = raw
        .into_iter()
        .map(scan_row_from_sql)
        .collect::<Result<Vec<_>>>()?;
    Ok((seconds, rows))
}

/// Apply one event to the PIT fold book (the SAME fold on every backend).
fn fold_event(book: &mut LadderBook, e: &Event) {
    book.apply(e);
}

/// Point-in-time top of book, ours: [`SnapshotIndex::latest_at`] +
/// [`pit_scan`] folded into a fresh [`LadderBook`]. Returns (seconds, top).
pub fn pit_ours(
    reader: &StoreReader,
    index: &SnapshotIndex,
    instrument: u32,
    t_mono: u64,
) -> Result<(f64, PitTop)> {
    let t0 = Instant::now();
    let Some(entry) = index.latest_at(instrument, t_mono) else {
        return Ok((
            t0.elapsed().as_secs_f64(),
            PitTop {
                anchor_mono: None,
                best_bid: None,
                best_ask: None,
            },
        ));
    };
    let mut book = LadderBook::new();
    pit_scan(reader, entry, t_mono, |e| fold_event(&mut book, e))?;
    let top = PitTop {
        anchor_mono: Some(entry.mono),
        best_bid: book.best_bid(),
        best_ask: book.best_ask(),
    };
    Ok((t0.elapsed().as_secs_f64(), top))
}

/// The anchor query (kind 4 = `SnapBegin`; asserted at compile time below).
const ANCHOR_SQL: &str = "SELECT max(recv_mono) FROM events \
     WHERE instrument = ? AND kind = 4 AND recv_mono <= ?";
const _: () = assert!(EventKind::SnapBegin as u8 == 4);

/// The window fetch: every event for the instrument between the anchor and
/// `t`, in file order (`rowid` breaks `recv_mono` ties — insertion order on
/// both engines because both loads append in file order).
const FETCH_SQL: &str = "SELECT recv_mono, recv_wall, venue_ts, venue_seq, price, qty, aux, \
     instrument, kind, venue, flags \
     FROM events WHERE instrument = ? AND recv_mono BETWEEN ? AND ? \
     ORDER BY recv_mono, rowid";

/// Build the [`SqlPit`] from the pieces shared by both SQL backends.
fn finish_sql_pit(
    seconds: f64,
    ours_anchor: Option<u64>,
    sql_anchor: Option<u64>,
    book: &LadderBook,
) -> SqlPit {
    SqlPit {
        seconds,
        top: PitTop {
            anchor_mono: ours_anchor,
            best_bid: if ours_anchor.is_some() {
                book.best_bid()
            } else {
                None
            },
            best_ask: if ours_anchor.is_some() {
                book.best_ask()
            } else {
                None
            },
        },
        sql_anchor,
        anchor_diverged: sql_anchor != ours_anchor,
    }
}

/// Point-in-time top of book on DuckDB: anchor query + window fetch + the
/// shared Rust fold. `ours_anchor` is the validated anchor from
/// [`pit_ours`]; the fold uses it (see module notes on incomplete-snapshot
/// divergence, reported via the returned [`SqlPit`]).
pub fn pit_duckdb(
    conn: &duckdb::Connection,
    instrument: u32,
    t_mono: u64,
    ours_anchor: Option<u64>,
) -> Result<SqlPit> {
    let inst = i64::from(instrument);
    let t = as_i64(t_mono, "t_mono")?;
    let t0 = Instant::now();
    let sql_anchor: Option<i64> = conn
        .query_row(ANCHOR_SQL, duckdb::params![inst, t], |r| r.get(0))
        .context("duckdb anchor query")?;
    let mut book = LadderBook::new();
    if let Some(anchor) = ours_anchor {
        let a = as_i64(anchor, "anchor")?;
        let mut stmt = conn.prepare(FETCH_SQL).context("duckdb fetch prepare")?;
        let raw: Vec<SqlEventRow> = stmt
            .query_map(duckdb::params![inst, a, t], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            })
            .context("duckdb fetch query")?
            .collect::<std::result::Result<_, _>>()
            .context("duckdb fetch rows")?;
        for vals in raw {
            fold_event(&mut book, &event_from_sql(vals)?);
        }
    }
    let seconds = t0.elapsed().as_secs_f64();
    let sql_anchor = sql_anchor
        .map(|v| u64::try_from(v).context("negative sql anchor"))
        .transpose()?;
    Ok(finish_sql_pit(seconds, ours_anchor, sql_anchor, &book))
}

/// Point-in-time top of book on SQLite: anchor query + window fetch (via
/// `idx_inst_mono`) + the shared Rust fold. Same anchor semantics as
/// [`pit_duckdb`].
pub fn pit_sqlite(
    conn: &rusqlite::Connection,
    instrument: u32,
    t_mono: u64,
    ours_anchor: Option<u64>,
) -> Result<SqlPit> {
    let inst = i64::from(instrument);
    let t = as_i64(t_mono, "t_mono")?;
    let t0 = Instant::now();
    let sql_anchor: Option<i64> = conn
        .query_row(ANCHOR_SQL, rusqlite::params![inst, t], |r| r.get(0))
        .context("sqlite anchor query")?;
    let mut book = LadderBook::new();
    if let Some(anchor) = ours_anchor {
        let a = as_i64(anchor, "anchor")?;
        let mut stmt = conn.prepare(FETCH_SQL).context("sqlite fetch prepare")?;
        let raw: Vec<SqlEventRow> = stmt
            .query_map(rusqlite::params![inst, a, t], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            })
            .context("sqlite fetch query")?
            .collect::<std::result::Result<_, _>>()
            .context("sqlite fetch rows")?;
        for vals in raw {
            fold_event(&mut book, &event_from_sql(vals)?);
        }
    }
    let seconds = t0.elapsed().as_secs_f64();
    let sql_anchor = sql_anchor
        .map(|v| u64::try_from(v).context("negative sql anchor"))
        .transpose()?;
    Ok(finish_sql_pit(seconds, ours_anchor, sql_anchor, &book))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flashbook_proto::event::Venue;

    fn ev(mono: u64, kind: EventKind, instrument: u32, price: i64, qty: i64) -> Event {
        Event {
            recv_mono_ns: mono,
            recv_wall_ns: mono + 1,
            venue_ts_ns: mono + 2,
            venue_seq: mono + 3,
            price,
            qty,
            aux: 7,
            instrument,
            kind: kind as u8,
            venue: Venue::Coinbase as u8,
            flags: 2,
            rsvd: 0,
        }
    }

    #[test]
    fn sql_row_roundtrips_and_rejects_overflow() {
        let e = ev(123, EventKind::BidSet, 5, 100_00000000, 3_00000000);
        let row = sql_row(&e).unwrap();
        assert_eq!(event_from_sql(row).unwrap(), e);

        let mut big = e;
        big.aux = u64::MAX; // > i64::MAX: unrepresentable in BIGINT
        let err = sql_row(&big).unwrap_err();
        assert!(err.to_string().contains("aux"), "{err}");

        let mut big = e;
        big.recv_mono_ns = (1u64 << 63) + 1;
        assert!(sql_row(&big).is_err(), "recv_mono over i64::MAX rejected");
    }

    #[test]
    fn scan_row_from_sql_rejects_negatives_and_recombines_split_sum() {
        let ok = scan_row_from_sql((1, 2, 3, 4, 5, 6, 7)).unwrap();
        // hi/lo recombination: 3 * 2^31 + 4
        assert_eq!(ok.sum_qty, 3i128 * 2_147_483_648 + 4);
        assert!(scan_row_from_sql((-1, 2, 3, 4, 5, 6, 7)).is_err());
        assert!(scan_row_from_sql((1, -2, 3, 4, 5, 6, 7)).is_err());
        assert!(scan_row_from_sql((1, 2, 3, 4, 5, 6, -7)).is_err());
        // a sum that would overflow i64 recombines exactly in i128
        let big = scan_row_from_sql((1, 2, i64::MAX / 4, 100, 0, 0, 1)).unwrap();
        assert_eq!(big.sum_qty, i128::from(i64::MAX / 4) * 2_147_483_648 + 100);
        assert!(big.sum_qty > i128::from(i64::MAX));
    }

    #[test]
    fn sql_literal_escapes_quotes() {
        let p = Path::new("/tmp/it's a dir/out.parquet");
        assert_eq!(
            sql_str_literal(p).unwrap(),
            "'/tmp/it''s a dir/out.parquet'"
        );
    }

    #[test]
    fn finish_sql_pit_reports_divergence_and_empty_anchor() {
        let mut book = LadderBook::new();
        for e in [
            ev(10, EventKind::SnapBegin, 1, 0, 0),
            ev(11, EventKind::SnapBid, 1, 100, 5),
            ev(12, EventKind::SnapAsk, 1, 101, 6),
            ev(13, EventKind::SnapEnd, 1, 0, 0),
        ] {
            fold_event(&mut book, &e);
        }
        let agree = finish_sql_pit(0.1, Some(10), Some(10), &book);
        assert!(!agree.anchor_diverged);
        assert_eq!(agree.top.best_bid, Some((100, 5)));
        assert_eq!(agree.top.best_ask, Some((101, 6)));

        let diverged = finish_sql_pit(0.1, Some(10), Some(50), &book);
        assert!(diverged.anchor_diverged);
        assert_eq!(diverged.top.anchor_mono, Some(10));

        // No ours-anchor: empty top regardless of the folded book state.
        let none = finish_sql_pit(0.1, None, Some(50), &book);
        assert_eq!(none.top.best_bid, None);
        assert_eq!(none.top.best_ask, None);
        assert!(none.anchor_diverged);
    }
}
