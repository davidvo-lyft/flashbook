//! Soak statistics: lock-free per-venue counters plus a periodic emitter
//! that appends one JSON line per venue (and one `"total"` line) to a stats
//! file every tick. The file is opened append-only and closed on every tick
//! so a crash never loses buffered stats.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use flashbook_proto::{Venue, clock};
use tokio::sync::watch;

use crate::sink::SinkGauges;

/// Lock-free counters for one venue, incremented by the connection task and
/// read by the stats emitter.
#[derive(Debug, Default)]
pub struct VenueStats {
    /// WS frames received (text + binary).
    pub msgs: AtomicU64,
    /// WS payload bytes received.
    pub bytes: AtomicU64,
    /// Normalized events emitted by the codec.
    pub events: AtomicU64,
    /// Gap events emitted (sequence breaks detected).
    pub gaps: AtomicU64,
    /// REST resyncs triggered by `Signal::NeedResync`.
    pub resyncs: AtomicU64,
    /// Reconnects after an established connection dropped (incl. idle kills).
    pub reconnects: AtomicU64,
    /// Frames that fell back from `parse` to `parse_slow`.
    pub fallbacks: AtomicU64,
    /// Frames neither parse path could handle.
    pub parse_errors: AtomicU64,
    /// REST snapshots successfully fetched and recorded.
    pub rest_snaps: AtomicU64,
}

/// Owned point-in-time copy of [`VenueStats`], serializable for the stats
/// JSONL file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StatsSnapshot {
    /// WS frames received.
    pub msgs: u64,
    /// WS payload bytes received.
    pub bytes: u64,
    /// Normalized events emitted.
    pub events: u64,
    /// Gap events emitted.
    pub gaps: u64,
    /// REST resyncs triggered.
    pub resyncs: u64,
    /// Reconnects after an established connection dropped.
    pub reconnects: u64,
    /// Fast-path to slow-path fallbacks.
    pub fallbacks: u64,
    /// Frames neither parse path could handle.
    pub parse_errors: u64,
    /// REST snapshots recorded.
    pub rest_snaps: u64,
}

impl VenueStats {
    /// Relaxed-load an owned snapshot of all counters.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            msgs: self.msgs.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            events: self.events.load(Ordering::Relaxed),
            gaps: self.gaps.load(Ordering::Relaxed),
            resyncs: self.resyncs.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            fallbacks: self.fallbacks.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            rest_snaps: self.rest_snaps.load(Ordering::Relaxed),
        }
    }
}

impl StatsSnapshot {
    /// Field-wise sum (for the `"total"` line).
    pub fn add(&self, other: &StatsSnapshot) -> StatsSnapshot {
        StatsSnapshot {
            msgs: self.msgs + other.msgs,
            bytes: self.bytes + other.bytes,
            events: self.events + other.events,
            gaps: self.gaps + other.gaps,
            resyncs: self.resyncs + other.resyncs,
            reconnects: self.reconnects + other.reconnects,
            fallbacks: self.fallbacks + other.fallbacks,
            parse_errors: self.parse_errors + other.parse_errors,
            rest_snaps: self.rest_snaps + other.rest_snaps,
        }
    }
}

/// One venue's inputs to the stats emitter.
#[derive(Debug)]
pub struct EmitterEntry {
    /// Venue this entry reports for.
    pub venue: Venue,
    /// Counters incremented by the connection task.
    pub stats: Arc<VenueStats>,
    /// Sink gauges (segment count, current segment bytes).
    pub gauges: Arc<SinkGauges>,
}

/// One serialized stats line (the JSONL record shape).
#[derive(Debug, serde::Serialize)]
struct StatsLine<'a> {
    ts_wall_ns: u64,
    venue: &'a str,
    #[serde(flatten)]
    counters: StatsSnapshot,
    rss_mb: u64,
    rss_max_mb: u64,
    segments: u64,
    current_segment_bytes: u64,
    uptime_s: u64,
}

/// Render one tick's worth of stats lines: one per venue plus a `"total"`
/// line, each newline-terminated. Pure over its inputs (aside from the
/// atomic loads) so tests can validate the exact JSON shape.
pub fn emit_lines(
    entries: &[EmitterEntry],
    ts_wall_ns: u64,
    rss_mb: u64,
    rss_max_mb: u64,
    uptime_s: u64,
) -> String {
    let mut out = String::new();
    let mut total = StatsSnapshot::default();
    let mut total_segments = 0u64;
    let mut total_seg_bytes = 0u64;
    for e in entries {
        let snap = e.stats.snapshot();
        total = total.add(&snap);
        let segments = e.gauges.segments.load(Ordering::Relaxed);
        let seg_bytes = e.gauges.current_bytes.load(Ordering::Relaxed);
        total_segments += segments;
        total_seg_bytes += seg_bytes;
        let line = StatsLine {
            ts_wall_ns,
            venue: e.venue.name(),
            counters: snap,
            rss_mb,
            rss_max_mb,
            segments,
            current_segment_bytes: seg_bytes,
            uptime_s,
        };
        out.push_str(&serde_json::to_string(&line).expect("stats line serializes"));
        out.push('\n');
    }
    let line = StatsLine {
        ts_wall_ns,
        venue: "total",
        counters: total,
        rss_mb,
        rss_max_mb,
        segments: total_segments,
        current_segment_bytes: total_seg_bytes,
        uptime_s,
    };
    out.push_str(&serde_json::to_string(&line).expect("stats line serializes"));
    out.push('\n');
    out
}

/// Resident set size of `pid` in KiB via `ps -o rss= -p <pid>` (portable on
/// macOS/Linux; returns `None` if `ps` fails or output doesn't parse).
pub fn rss_kb(pid: u32) -> Option<u64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// Append `text` to `path`, creating parent dirs and the file as needed;
/// open-append-close so nothing is ever buffered across ticks.
pub fn append_to_file(path: &Path, text: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)?;
    f.write_all(text.as_bytes())
}

/// Stats emitter task: every `period` (60 s in production) append one JSON
/// line per venue plus a `"total"` line to `path`, and log a one-line human
/// summary. Exits when `shutdown` flips to true, after one final emission
/// so even runs shorter than `period` leave stats lines behind.
pub async fn run_stats_emitter(
    entries: Vec<EmitterEntry>,
    path: PathBuf,
    period: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let start = std::time::Instant::now();
    let pid = std::process::id();
    let mut rss_max_mb = 0u64;
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // the first tick fires immediately; skip it
    loop {
        let last = tokio::select! {
            _ = tick.tick() => false,
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    true // emit one final batch below, then exit
                } else {
                    continue;
                }
            }
        };
        let rss_mb = rss_kb(pid).unwrap_or(0) / 1024;
        rss_max_mb = rss_max_mb.max(rss_mb);
        let uptime_s = start.elapsed().as_secs();
        let lines = emit_lines(&entries, clock::wall_ns(), rss_mb, rss_max_mb, uptime_s);
        if let Err(e) = append_to_file(&path, &lines) {
            tracing::warn!(path = %path.display(), error = %e, "failed to append stats");
        }
        let total: StatsSnapshot = entries
            .iter()
            .map(|e| e.stats.snapshot())
            .fold(StatsSnapshot::default(), |acc, s| acc.add(&s));
        tracing::info!(
            msgs = total.msgs,
            events = total.events,
            gaps = total.gaps,
            reconnects = total.reconnects,
            fallbacks = total.fallbacks,
            parse_errors = total.parse_errors,
            rest_snaps = total.rest_snaps,
            rss_mb,
            uptime_s,
            "soak stats"
        );
        if last {
            return;
        }
    }
}
