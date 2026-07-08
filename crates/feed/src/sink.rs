//! Capture sinks: size/age-rotated raw-capture segments.
//!
//! [`RotatingRawLog`] wraps [`flashbook_proto::rawlog::RawLogWriter`] with
//! rotation: when the current segment exceeds `max_bytes` or `max_age` it is
//! finished (flush + fsync) and a fresh segment is opened. Closed segments
//! are optionally handed to the `zstd` CLI for background compression
//! (`zstd -q -3 --rm <path>`), so the hot path never pays for compression.
//!
//! Segment paths are `<dir>/<venue>/<venue>-<start_wall_ns>.fbraw`; segments
//! are immutable once closed (rotation or shutdown always opens a new file).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use flashbook_proto::rawlog::RawLogWriter;
use flashbook_proto::{Venue, clock};

/// Default segment size cap: 256 MiB.
pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Default segment age cap: 900 s (15 min).
pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(900);

/// Shared, atomically-updated sink gauges for the stats emitter (the sink
/// itself is owned by the venue task; the emitter only reads these).
#[derive(Debug, Default)]
pub struct SinkGauges {
    /// Segments opened so far (including the current one).
    pub segments: AtomicU64,
    /// Bytes written to the current segment (header + records).
    pub current_bytes: AtomicU64,
}

/// Pure rotation predicate: rotate when the current segment exceeds either
/// the byte or the age budget.
#[inline]
pub fn should_rotate(bytes: u64, max_bytes: u64, age: Duration, max_age: Duration) -> bool {
    bytes >= max_bytes || age >= max_age
}

/// Build the conventional segment-header meta JSON from the venue's
/// subscribed `(venue_symbol, instrument_id)` pairs.
pub fn meta_json<'a>(symbols: impl IntoIterator<Item = (&'a str, u32)>) -> Vec<u8> {
    let list: Vec<serde_json::Value> = symbols
        .into_iter()
        .map(|(sym, id)| serde_json::json!({ "sym": sym, "id": id }))
        .collect();
    serde_json::to_vec(&serde_json::json!({ "symbols": list })).expect("meta serializes")
}

/// A size/age-rotating raw-capture segment writer for one venue.
pub struct RotatingRawLog {
    dir: PathBuf,
    venue: Venue,
    meta: Vec<u8>,
    max_bytes: u64,
    max_age: Duration,
    compress: bool,
    zstd: Option<PathBuf>,
    writer: Option<RawLogWriter>,
    current_path: PathBuf,
    opened_at: Instant,
    gauges: Arc<SinkGauges>,
}

impl std::fmt::Debug for RotatingRawLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RotatingRawLog")
            .field("dir", &self.dir)
            .field("venue", &self.venue)
            .field("max_bytes", &self.max_bytes)
            .field("max_age", &self.max_age)
            .field("compress", &self.compress)
            .field("current_path", &self.current_path)
            .finish_non_exhaustive()
    }
}

impl RotatingRawLog {
    /// Create the sink under `<root>/<venue name>/`, opening the first
    /// segment immediately. `meta` is written verbatim into every segment
    /// header (see [`meta_json`]). If `compress` is set but no `zstd`
    /// binary is found on `PATH`, compression is disabled with a warning.
    pub fn create(
        root: &Path,
        venue: Venue,
        meta: Vec<u8>,
        max_bytes: u64,
        max_age: Duration,
        compress: bool,
    ) -> io::Result<Self> {
        let dir = root.join(venue.name());
        std::fs::create_dir_all(&dir)?;
        let zstd = if compress { find_zstd() } else { None };
        let compress = if compress && zstd.is_none() {
            tracing::warn!(
                venue = venue.name(),
                "zstd not found on PATH; segments will stay uncompressed"
            );
            false
        } else {
            compress
        };
        let mut sink = Self {
            dir,
            venue,
            meta,
            max_bytes,
            max_age,
            compress,
            zstd,
            writer: None,
            current_path: PathBuf::new(),
            opened_at: Instant::now(),
            gauges: Arc::new(SinkGauges::default()),
        };
        sink.open_segment()?;
        Ok(sink)
    }

    fn open_segment(&mut self) -> io::Result<()> {
        let mono = clock::mono_ns();
        let mut wall = clock::wall_ns();
        // Bump the timestamp on the (vanishingly rare) same-ns collision:
        // `RawLogWriter::create` refuses to overwrite existing files.
        loop {
            let name = format!("{}-{}.fbraw", self.venue.name(), wall);
            let path = self.dir.join(name);
            match RawLogWriter::create(&path, self.venue as u8, wall, mono, &self.meta) {
                Ok(w) => {
                    self.gauges
                        .current_bytes
                        .store(w.bytes(), Ordering::Relaxed);
                    self.gauges.segments.fetch_add(1, Ordering::Relaxed);
                    self.writer = Some(w);
                    self.current_path = path;
                    self.opened_at = Instant::now();
                    return Ok(());
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => wall += 1,
                Err(e) => return Err(e),
            }
        }
    }

    fn writer(&mut self) -> &mut RawLogWriter {
        self.writer.as_mut().expect("writer present outside finish")
    }

    /// Append one record, rotating first if the current segment is over
    /// budget (so a segment never exceeds `max_bytes` by more than one
    /// record).
    pub fn append(
        &mut self,
        rk: u8,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        payload: &[u8],
    ) -> io::Result<()> {
        self.maybe_rotate()?;
        self.writer()
            .append(rk, recv_mono_ns, recv_wall_ns, payload)?;
        let bytes = self.writer().bytes();
        self.gauges.current_bytes.store(bytes, Ordering::Relaxed);
        Ok(())
    }

    /// Rotate if the size or age budget is exceeded (also called on a
    /// periodic tick so idle segments still rotate by age). Returns whether
    /// a rotation happened.
    pub fn maybe_rotate(&mut self) -> io::Result<bool> {
        let bytes = self.writer().bytes();
        if !should_rotate(
            bytes,
            self.max_bytes,
            self.opened_at.elapsed(),
            self.max_age,
        ) {
            return Ok(false);
        }
        let closed = self.writer.take().expect("writer present outside finish");
        closed.finish()?;
        let closed_path = std::mem::take(&mut self.current_path);
        if self.compress
            && let Some(zstd) = &self.zstd
        {
            spawn_compress(zstd, &closed_path);
        }
        self.open_segment()?;
        Ok(true)
    }

    /// Flush + fsync the current segment (durability tick).
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer().sync()
    }

    /// Flush, fsync and close the current segment. The final segment is
    /// left uncompressed (compressing at shutdown would race process exit).
    pub fn finish(mut self) -> io::Result<()> {
        let w = self.writer.take().expect("writer present outside finish");
        w.finish()
    }

    /// Segments opened so far (including the current one).
    pub fn segments_created(&self) -> u64 {
        self.gauges.segments.load(Ordering::Relaxed)
    }

    /// Bytes written to the current segment (header + records).
    pub fn current_bytes(&self) -> u64 {
        self.gauges.current_bytes.load(Ordering::Relaxed)
    }

    /// Path of the current (open) segment.
    pub fn current_path(&self) -> &Path {
        &self.current_path
    }

    /// Shared gauges handle for the stats emitter.
    pub fn gauges(&self) -> Arc<SinkGauges> {
        Arc::clone(&self.gauges)
    }
}

/// Locate a `zstd` executable on `PATH` (resolved once at sink creation).
fn find_zstd() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let cand = dir.join("zstd");
        if is_executable(&cand) {
            return Some(cand);
        }
    }
    None
}

#[cfg(unix)]
fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(p: &Path) -> bool {
    p.is_file()
}

/// Kick off background compression of a closed segment; never blocks the
/// hot path and never fails the sink (compression errors only warn).
fn spawn_compress(zstd: &Path, segment: &Path) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::warn!(segment = %segment.display(), "no tokio runtime; leaving segment uncompressed");
        return;
    };
    let mut cmd = tokio::process::Command::new(zstd);
    cmd.args(["-q", "-3", "--rm"]).arg(segment);
    let seg = segment.to_path_buf();
    match cmd.spawn() {
        Ok(child) => {
            handle.spawn(async move {
                match child.wait_with_output().await {
                    Ok(out) if out.status.success() => {
                        tracing::debug!(segment = %seg.display(), "compressed");
                    }
                    Ok(out) => {
                        tracing::warn!(segment = %seg.display(), status = %out.status, "zstd failed; segment left uncompressed");
                    }
                    Err(e) => {
                        tracing::warn!(segment = %seg.display(), error = %e, "zstd wait failed");
                    }
                }
            });
        }
        Err(e) => {
            tracing::warn!(segment = %seg.display(), error = %e, "failed to spawn zstd; segment left uncompressed");
        }
    }
}
