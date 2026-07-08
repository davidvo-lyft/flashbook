//! Segment discovery and deterministic multi-venue record merging.
//!
//! Capture writes per-venue segment files (`<venue>-<startwallns>.fbraw`,
//! possibly zstd-compressed to `.fbraw.zst` by the compactor). Replay
//! discovers them, reads them transparently, and merges the per-venue
//! streams into one deterministic order: ascending `recv_mono_ns`, ties
//! broken by (venue, within-file order). Two replays of the same segments
//! always yield the identical record sequence — the foundation of the
//! byte-identical book-state guarantee.

use std::io::Read;
use std::path::{Path, PathBuf};

use flashbook_proto::rawlog::{RawLogError, RawLogReader};

/// One discovered segment file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFile {
    /// Full path (plain `.fbraw` or compressed `.fbraw.zst`).
    pub path: PathBuf,
    /// Venue id from the file name directory layout.
    pub venue: u8,
    /// Segment start wall time (from the file name), used for ordering.
    pub start_wall_ns: u64,
    /// True if zstd-compressed.
    pub compressed: bool,
}

/// Errors from discovery/reading.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// IO problem walking directories or opening files.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// A segment failed header validation.
    #[error("segment {path}: {err}")]
    Segment {
        /// Offending file.
        path: String,
        /// Underlying error.
        err: RawLogError,
    },
}

/// Discover every segment under `root` (layout `<root>/<venue>/<file>`),
/// sorted by (venue, start_wall_ns). A `.fbraw` and its `.fbraw.zst` twin
/// (mid-compaction crash leftover) dedupe to the compressed one.
pub fn discover(root: &Path) -> Result<Vec<SegmentFile>, SourceError> {
    let mut found: Vec<SegmentFile> = Vec::new();
    let venues = match std::fs::read_dir(root) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    for venue_dir in venues {
        let venue_dir = venue_dir?;
        if !venue_dir.file_type()?.is_dir() {
            continue;
        }
        let venue = match venue_dir.file_name().to_string_lossy().as_ref() {
            "coinbase" => 1u8,
            "binance" => 2,
            "kraken" => 3,
            _ => continue,
        };
        for f in std::fs::read_dir(venue_dir.path())? {
            let f = f?;
            let name = f.file_name().to_string_lossy().into_owned();
            let (stem, compressed) = if let Some(s) = name.strip_suffix(".fbraw.zst") {
                (s.to_string(), true)
            } else if let Some(s) = name.strip_suffix(".fbraw") {
                (s.to_string(), false)
            } else {
                continue;
            };
            // stem: <venuename>-<startwallns>
            let Some(ts) = stem.rsplit('-').next().and_then(|t| t.parse::<u64>().ok()) else {
                continue;
            };
            found.push(SegmentFile {
                path: f.path(),
                venue,
                start_wall_ns: ts,
                compressed,
            });
        }
    }
    // Dedupe plain/compressed twins in favor of the compressed file (the
    // compactor removes the source only after success, so the .zst is
    // complete if present).
    found.sort_by(|a, b| {
        (a.venue, a.start_wall_ns, !a.compressed).cmp(&(b.venue, b.start_wall_ns, !b.compressed))
    });
    found.dedup_by(|later, earlier| {
        later.venue == earlier.venue && later.start_wall_ns == earlier.start_wall_ns
    });
    Ok(found)
}

/// Open a segment for reading, transparently decompressing `.zst`.
pub fn open_segment(seg: &SegmentFile) -> Result<RawLogReader<Box<dyn Read>>, SourceError> {
    let file = std::fs::File::open(&seg.path)?;
    let reader: Box<dyn Read> = if seg.compressed {
        Box::new(zstd::stream::read::Decoder::new(file).map_err(SourceError::Io)?)
    } else {
        Box::new(std::io::BufReader::with_capacity(1 << 20, file))
    };
    RawLogReader::from_reader(reader).map_err(|err| SourceError::Segment {
        path: seg.path.display().to_string(),
        err,
    })
}

/// One record with its origin, in merged order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedRecord {
    /// Venue id.
    pub venue: u8,
    /// Record kind (see [`flashbook_proto::rawlog::rkind`]).
    pub rkind: u8,
    /// Monotonic receive timestamp (merge key).
    pub recv_mono_ns: u64,
    /// Wall receive timestamp.
    pub recv_wall_ns: u64,
    /// Payload bytes (owned; the merge outlives reader buffers).
    pub payload: Vec<u8>,
}

/// Per-venue cursor over that venue's segments in start order.
struct VenueCursor {
    venue: u8,
    segments: std::vec::IntoIter<SegmentFile>,
    reader: Option<RawLogReader<Box<dyn Read>>>,
    /// Records yielded by this cursor so far (tie-break stability).
    yielded: u64,
    /// Torn tails encountered (counted, then reading continues with the
    /// next segment — a torn final record is expected after a crash).
    torn_tails: u64,
}

impl VenueCursor {
    fn next_record(&mut self) -> Result<Option<MergedRecord>, SourceError> {
        loop {
            if self.reader.is_none() {
                match self.segments.next() {
                    Some(seg) => self.reader = Some(open_segment(&seg)?),
                    None => return Ok(None),
                }
            }
            let r = self.reader.as_mut().expect("just set");
            match r.read_next() {
                Ok(Some(rec)) => {
                    self.yielded += 1;
                    return Ok(Some(MergedRecord {
                        venue: self.venue,
                        rkind: rec.rkind,
                        recv_mono_ns: rec.recv_mono_ns,
                        recv_wall_ns: rec.recv_wall_ns,
                        payload: rec.payload.to_vec(),
                    }));
                }
                Ok(None) => {
                    self.reader = None; // clean EOF -> next segment
                }
                Err(RawLogError::TornTail { .. }) => {
                    self.torn_tails += 1;
                    self.reader = None; // salvage ends this segment
                }
                Err(e) => {
                    return Err(SourceError::Segment {
                        path: String::new(),
                        err: e,
                    });
                }
            }
        }
    }
}

/// Deterministic k-way merge over all venues' records.
pub struct MergedStream {
    cursors: Vec<VenueCursor>,
    /// Lookahead: the next record per cursor.
    heads: Vec<Option<MergedRecord>>,
}

/// Counters from a completed merge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeStats {
    /// Records yielded.
    pub records: u64,
    /// Torn segment tails skipped.
    pub torn_tails: u64,
}

impl MergedStream {
    /// Build over all segments under `root`.
    pub fn new(root: &Path) -> Result<MergedStream, SourceError> {
        let all = discover(root)?;
        let mut by_venue: std::collections::BTreeMap<u8, Vec<SegmentFile>> = Default::default();
        for s in all {
            by_venue.entry(s.venue).or_default().push(s);
        }
        let mut cursors = Vec::new();
        for (venue, segs) in by_venue {
            cursors.push(VenueCursor {
                venue,
                segments: segs.into_iter(),
                reader: None,
                yielded: 0,
                torn_tails: 0,
            });
        }
        let mut heads = Vec::with_capacity(cursors.len());
        for c in &mut cursors {
            heads.push(c.next_record()?);
        }
        Ok(MergedStream { cursors, heads })
    }

    /// Merge stats so far (final after the stream returns `None`).
    pub fn stats(&self) -> MergeStats {
        MergeStats {
            records: self.cursors.iter().map(|c| c.yielded).sum::<u64>()
                - self.heads.iter().flatten().count() as u64,
            torn_tails: self.cursors.iter().map(|c| c.torn_tails).sum(),
        }
    }

    /// Next record in deterministic merged order.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<MergedRecord>, SourceError> {
        // pick the head with the smallest (recv_mono_ns, venue)
        let mut best: Option<usize> = None;
        for (i, h) in self.heads.iter().enumerate() {
            if let Some(rec) = h {
                let better = match best {
                    None => true,
                    Some(b) => {
                        let cur = self.heads[b].as_ref().expect("best head present");
                        (rec.recv_mono_ns, rec.venue) < (cur.recv_mono_ns, cur.venue)
                    }
                };
                if better {
                    best = Some(i);
                }
            }
        }
        let Some(i) = best else {
            return Ok(None);
        };
        let out = self.heads[i].take().expect("chosen head present");
        self.heads[i] = self.cursors[i].next_record()?;
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flashbook_proto::rawlog::{RawLogWriter, rkind};

    fn write_segment(dir: &Path, venue: &str, venue_id: u8, start: u64, ts: &[u64]) -> PathBuf {
        let vdir = dir.join(venue);
        std::fs::create_dir_all(&vdir).unwrap();
        let path = vdir.join(format!("{venue}-{start}.fbraw"));
        let mut w = RawLogWriter::create(&path, venue_id, start, start, b"{}").unwrap();
        for &t in ts {
            let payload = format!("{{\"t\":{t}}}");
            w.append(rkind::WS_TEXT, t, t + 1, payload.as_bytes())
                .unwrap();
        }
        w.finish().unwrap();
        path
    }

    #[test]
    fn discovers_sorted_and_dedupes_zst_twins() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), "kraken", 3, 200, &[1]);
        let p1 = write_segment(dir.path(), "kraken", 3, 100, &[1]);
        write_segment(dir.path(), "coinbase", 1, 150, &[1]);
        // compress the kraken-100 segment but ALSO leave the original
        // (simulates a mid-compaction crash)
        let raw = std::fs::read(&p1).unwrap();
        let z = zstd::bulk::compress(&raw, 3).unwrap();
        std::fs::write(p1.with_extension("fbraw.zst"), &z).unwrap();

        let found = discover(dir.path()).unwrap();
        assert_eq!(found.len(), 3);
        assert_eq!(found[0].venue, 1);
        assert_eq!(found[1].venue, 3);
        assert_eq!(found[1].start_wall_ns, 100);
        assert!(found[1].compressed, "compressed twin wins");
        assert_eq!(found[2].venue, 3);
        assert_eq!(found[2].start_wall_ns, 200);
    }

    #[test]
    fn missing_root_is_empty() {
        assert!(
            discover(Path::new("/nonexistent/flashbook-test"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn merge_is_deterministic_and_ordered() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), "coinbase", 1, 100, &[10, 30, 50]);
        write_segment(dir.path(), "binance", 2, 100, &[20, 30, 60]);
        write_segment(dir.path(), "kraken", 3, 100, &[5, 30]);
        write_segment(dir.path(), "kraken", 3, 200, &[70]);

        let collect = || {
            let mut s = MergedStream::new(dir.path()).unwrap();
            let mut out = Vec::new();
            while let Some(r) = s.next().unwrap() {
                out.push((r.recv_mono_ns, r.venue));
            }
            (out, s.stats())
        };
        let (a, stats) = collect();
        let (b, _) = collect();
        assert_eq!(a, b, "two merges must be identical");
        assert_eq!(stats.records, 9);
        assert_eq!(stats.torn_tails, 0);
        // ascending mono; ties (t=30) broken by venue id
        assert_eq!(
            a,
            vec![
                (5, 3),
                (10, 1),
                (20, 2),
                (30, 1),
                (30, 2),
                (30, 3),
                (50, 1),
                (60, 2),
                (70, 3)
            ]
        );
    }

    #[test]
    fn zst_segments_read_transparently() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_segment(dir.path(), "binance", 2, 100, &[1, 2, 3]);
        let raw = std::fs::read(&p).unwrap();
        // stream-compress like the zstd CLI would
        let z = zstd::stream::encode_all(&raw[..], 3).unwrap();
        std::fs::write(p.with_extension("fbraw.zst"), z).unwrap();
        std::fs::remove_file(&p).unwrap();

        let mut s = MergedStream::new(dir.path()).unwrap();
        let mut n = 0;
        while let Some(r) = s.next().unwrap() {
            assert_eq!(r.venue, 2);
            assert!(r.payload.starts_with(b"{\"t\":"));
            n += 1;
        }
        assert_eq!(n, 3);
    }

    #[test]
    fn torn_tail_is_skipped_and_counted() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_segment(dir.path(), "kraken", 3, 100, &[1, 2, 3]);
        write_segment(dir.path(), "kraken", 3, 200, &[9]);
        // tear the first segment's last record
        let len = std::fs::metadata(&p).unwrap().len();
        let f = std::fs::File::options().write(true).open(&p).unwrap();
        f.set_len(len - 2).unwrap();
        drop(f);

        let mut s = MergedStream::new(dir.path()).unwrap();
        let mut got = Vec::new();
        while let Some(r) = s.next().unwrap() {
            got.push(r.recv_mono_ns);
        }
        assert_eq!(
            got,
            vec![1, 2, 9],
            "torn record dropped, next segment continues"
        );
        assert_eq!(s.stats().torn_tails, 1);
    }
}
