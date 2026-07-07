//! Append-only raw-capture segment files (D-001): CRC32-framed records of
//! raw venue payload bytes plus receive timestamps, with torn-write
//! detection on read. This is the capture ground truth; everything else is
//! derivable from it.
//!
//! Layout (all integers little-endian):
//!
//! ```text
//! header:  magic "FBRAW001" | ver u8 | venue u8 | rsvd u16 |
//!          start_wall_ns u64 | start_mono_ns u64 | meta_len u32 | meta bytes
//! record:  len u32 | rkind u8 | recv_mono_ns u64 | recv_wall_ns u64 |
//!          payload bytes | crc32 u32
//! ```
//!
//! `len` counts everything after the `len` field (rkind..crc32 inclusive).
//! `crc32` (IEEE, via `crc32fast`) covers rkind..payload. A record that fails
//! length or CRC checks is a torn tail: the reader reports the valid prefix
//! length so callers can truncate and continue.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Segment file magic.
pub const MAGIC: &[u8; 8] = b"FBRAW001";
/// Format version written by this code.
pub const VERSION: u8 = 1;
/// Hard sanity cap on a single record's payload (a venue message is KBs;
/// anything huge means corruption).
pub const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

const RECORD_OVERHEAD: usize = 1 + 8 + 8 + 4; // rkind + mono + wall + crc

/// Record kinds.
pub mod rkind {
    /// A WebSocket text frame payload, verbatim.
    pub const WS_TEXT: u8 = 1;
    /// A WebSocket binary frame payload, verbatim.
    pub const WS_BINARY: u8 = 2;
    /// A REST book-snapshot response body (JSON), verbatim.
    pub const REST_SNAPSHOT: u8 = 3;
    /// Operational note (JSON): connects, disconnects, gaps, subscribe acks.
    pub const NOTE: u8 = 4;
}

/// Errors from segment reading.
#[derive(Debug, thiserror::Error)]
pub enum RawLogError {
    /// Underlying IO error.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// File does not start with a valid header.
    #[error("bad header: {0}")]
    BadHeader(&'static str),
    /// Torn or corrupt tail; `valid_bytes` is the offset of the last
    /// fully-valid record end (safe truncation point).
    #[error("torn tail after {valid_bytes} valid bytes: {reason}")]
    TornTail {
        /// Safe truncation offset.
        valid_bytes: u64,
        /// Why the tail was rejected.
        reason: &'static str,
    },
}

/// Writes one segment file.
pub struct RawLogWriter {
    w: BufWriter<File>,
    bytes: u64,
    records: u64,
}

impl RawLogWriter {
    /// Create a new segment at `path` (fails if it exists — segments are
    /// immutable once created; a restart always opens a new segment).
    pub fn create(
        path: &Path,
        venue: u8,
        start_wall_ns: u64,
        start_mono_ns: u64,
        meta: &[u8],
    ) -> io::Result<Self> {
        let file = File::options().write(true).create_new(true).open(path)?;
        let mut w = BufWriter::with_capacity(1 << 20, file);
        w.write_all(MAGIC)?;
        w.write_all(&[VERSION, venue])?;
        w.write_all(&0u16.to_le_bytes())?;
        w.write_all(&start_wall_ns.to_le_bytes())?;
        w.write_all(&start_mono_ns.to_le_bytes())?;
        let meta_len = u32::try_from(meta.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        w.write_all(&meta_len.to_le_bytes())?;
        w.write_all(meta)?;
        let bytes = 8 + 2 + 2 + 8 + 8 + 4 + meta.len() as u64;
        Ok(Self {
            w,
            bytes,
            records: 0,
        })
    }

    /// Append one record.
    pub fn append(
        &mut self,
        rk: u8,
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        payload: &[u8],
    ) -> io::Result<()> {
        if payload.len() > MAX_PAYLOAD {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let len = u32::try_from(RECORD_OVERHEAD + payload.len())
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        let mono = recv_mono_ns.to_le_bytes();
        let wall = recv_wall_ns.to_le_bytes();
        let mut h = crc32fast::Hasher::new();
        h.update(&[rk]);
        h.update(&mono);
        h.update(&wall);
        h.update(payload);
        let crc = h.finalize();
        self.w.write_all(&len.to_le_bytes())?;
        self.w.write_all(&[rk])?;
        self.w.write_all(&mono)?;
        self.w.write_all(&wall)?;
        self.w.write_all(payload)?;
        self.w.write_all(&crc.to_le_bytes())?;
        self.bytes += 4 + u64::from(len);
        self.records += 1;
        Ok(())
    }

    /// Flush buffered data to the OS.
    pub fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }

    /// Flush and fsync (durability point; call periodically, not per record).
    pub fn sync(&mut self) -> io::Result<()> {
        self.w.flush()?;
        self.w.get_ref().sync_data()
    }

    /// Bytes written so far (header + records).
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Records written so far.
    pub fn records(&self) -> u64 {
        self.records
    }

    /// Flush, fsync and close.
    pub fn finish(mut self) -> io::Result<()> {
        self.sync()
    }
}

/// One decoded record, borrowing the reader's buffer.
#[derive(Debug, PartialEq, Eq)]
pub struct RawRecord<'a> {
    /// Record kind (see [`rkind`]).
    pub rkind: u8,
    /// Monotonic receive timestamp.
    pub recv_mono_ns: u64,
    /// Wall receive timestamp.
    pub recv_wall_ns: u64,
    /// Raw payload bytes.
    pub payload: &'a [u8],
}

/// Segment header fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Format version.
    pub version: u8,
    /// Venue id (see [`crate::event::Venue`]).
    pub venue: u8,
    /// Wall clock at segment creation.
    pub start_wall_ns: u64,
    /// Monotonic clock at segment creation.
    pub start_mono_ns: u64,
    /// Opaque metadata bytes (by convention JSON).
    pub meta: Vec<u8>,
}

/// Reads one segment file sequentially with torn-tail detection.
pub struct RawLogReader<R: Read> {
    r: R,
    /// Segment header.
    pub header: SegmentHeader,
    buf: Vec<u8>,
    offset: u64,
}

impl RawLogReader<BufReader<File>> {
    /// Open a plain (uncompressed) segment file.
    pub fn open(path: &Path) -> Result<Self, RawLogError> {
        let f = File::open(path)?;
        Self::from_reader(BufReader::with_capacity(1 << 20, f))
    }
}

impl<R: Read> RawLogReader<R> {
    /// Read the header from any byte stream (works for zstd-decoded streams).
    pub fn from_reader(mut r: R) -> Result<Self, RawLogError> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)
            .map_err(|_| RawLogError::BadHeader("short magic"))?;
        if &magic != MAGIC {
            return Err(RawLogError::BadHeader("bad magic"));
        }
        let mut fixed = [0u8; 2 + 2 + 8 + 8 + 4];
        r.read_exact(&mut fixed)
            .map_err(|_| RawLogError::BadHeader("short header"))?;
        let version = fixed[0];
        if version != VERSION {
            return Err(RawLogError::BadHeader("unsupported version"));
        }
        let venue = fixed[1];
        let start_wall_ns = u64::from_le_bytes(fixed[4..12].try_into().unwrap());
        let start_mono_ns = u64::from_le_bytes(fixed[12..20].try_into().unwrap());
        let meta_len = u32::from_le_bytes(fixed[20..24].try_into().unwrap()) as usize;
        if meta_len > MAX_PAYLOAD {
            return Err(RawLogError::BadHeader("meta too large"));
        }
        let mut meta = vec![0u8; meta_len];
        r.read_exact(&mut meta)
            .map_err(|_| RawLogError::BadHeader("short meta"))?;
        let offset = (8 + fixed.len() + meta_len) as u64;
        Ok(Self {
            r,
            header: SegmentHeader {
                version,
                venue,
                start_wall_ns,
                start_mono_ns,
                meta,
            },
            buf: Vec::with_capacity(64 * 1024),
            offset,
        })
    }

    /// Byte offset of the end of the last successfully-read record.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Read the next record. `Ok(None)` at a clean EOF; `Err(TornTail)` if
    /// the file ends mid-record or fails CRC.
    pub fn read_next(&mut self) -> Result<Option<RawRecord<'_>>, RawLogError> {
        let mut len4 = [0u8; 4];
        match read_exact_or_eof(&mut self.r, &mut len4) {
            ReadOutcome::Eof => return Ok(None),
            ReadOutcome::Partial => {
                return Err(RawLogError::TornTail {
                    valid_bytes: self.offset,
                    reason: "partial length prefix",
                });
            }
            ReadOutcome::Full => {}
        }
        let len = u32::from_le_bytes(len4) as usize;
        if !(RECORD_OVERHEAD..=RECORD_OVERHEAD + MAX_PAYLOAD).contains(&len) {
            return Err(RawLogError::TornTail {
                valid_bytes: self.offset,
                reason: "implausible record length",
            });
        }
        self.buf.resize(len, 0);
        if self.r.read_exact(&mut self.buf).is_err() {
            return Err(RawLogError::TornTail {
                valid_bytes: self.offset,
                reason: "record truncated",
            });
        }
        let (body, crc_bytes) = self.buf.split_at(len - 4);
        let want = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        let got = crc32fast::hash(body);
        if want != got {
            return Err(RawLogError::TornTail {
                valid_bytes: self.offset,
                reason: "crc mismatch",
            });
        }
        let rk = body[0];
        let recv_mono_ns = u64::from_le_bytes(body[1..9].try_into().unwrap());
        let recv_wall_ns = u64::from_le_bytes(body[9..17].try_into().unwrap());
        self.offset += 4 + len as u64;
        Ok(Some(RawRecord {
            rkind: rk,
            recv_mono_ns,
            recv_wall_ns,
            payload: &self.buf[17..len - 4],
        }))
    }
}

enum ReadOutcome {
    Full,
    Partial,
    Eof,
}

fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> ReadOutcome {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                return if filled == 0 {
                    ReadOutcome::Eof
                } else {
                    ReadOutcome::Partial
                };
            }
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return ReadOutcome::Partial,
        }
    }
    ReadOutcome::Full
}

/// Truncate a segment file at `valid_bytes` (recovery after a torn write).
pub fn truncate_to_valid(path: &Path, valid_bytes: u64) -> io::Result<()> {
    let f = File::options().write(true).open(path)?;
    f.set_len(valid_bytes)?;
    f.sync_data()
}

/// Scan a segment and return (records, payload_bytes, valid_bytes, torn).
pub fn scan(path: &Path) -> Result<ScanReport, RawLogError> {
    let mut rd = RawLogReader::open(path)?;
    let mut records = 0u64;
    let mut payload_bytes = 0u64;
    let torn = loop {
        match rd.read_next() {
            Ok(Some(rec)) => {
                records += 1;
                payload_bytes += rec.payload.len() as u64;
            }
            Ok(None) => break None,
            Err(RawLogError::TornTail {
                valid_bytes,
                reason,
            }) => {
                break Some((valid_bytes, reason));
            }
            Err(e) => return Err(e),
        }
    };
    Ok(ScanReport {
        records,
        payload_bytes,
        valid_bytes: rd.offset(),
        torn,
    })
}

/// Result of [`scan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanReport {
    /// Valid records found.
    pub records: u64,
    /// Sum of payload lengths over valid records.
    pub payload_bytes: u64,
    /// Offset of last valid record end.
    pub valid_bytes: u64,
    /// `Some((valid_bytes, reason))` if the tail was torn/corrupt.
    pub torn: Option<(u64, &'static str)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn write_sample(path: &Path, n: u64) {
        let mut w =
            RawLogWriter::create(path, 3, 1000, 2000, br#"{"symbols":["BTC/USD"]}"#).unwrap();
        for i in 0..n {
            let payload = format!("{{\"seq\":{i},\"data\":\"xxxxxxxxxx\"}}");
            w.append(rkind::WS_TEXT, i * 10, i * 10 + 1, payload.as_bytes())
                .unwrap();
        }
        w.finish().unwrap();
    }

    #[test]
    fn roundtrip_header_and_records() {
        let d = tmp();
        let p = d.path().join("a.fbraw");
        write_sample(&p, 5);
        let mut rd = RawLogReader::open(&p).unwrap();
        assert_eq!(rd.header.venue, 3);
        assert_eq!(rd.header.start_wall_ns, 1000);
        assert_eq!(rd.header.meta, br#"{"symbols":["BTC/USD"]}"#);
        let mut n = 0u64;
        while let Some(rec) = rd.read_next().unwrap() {
            assert_eq!(rec.rkind, rkind::WS_TEXT);
            assert_eq!(rec.recv_mono_ns, n * 10);
            assert!(rec.payload.starts_with(b"{\"seq\":"));
            n += 1;
        }
        assert_eq!(n, 5);
    }

    #[test]
    fn create_refuses_existing_file() {
        let d = tmp();
        let p = d.path().join("a.fbraw");
        write_sample(&p, 1);
        assert!(RawLogWriter::create(&p, 3, 0, 0, b"").is_err());
    }

    #[test]
    fn detects_torn_tail_and_recovers() {
        let d = tmp();
        let p = d.path().join("a.fbraw");
        write_sample(&p, 10);
        let full = scan(&p).unwrap();
        assert_eq!(full.records, 10);
        assert!(full.torn.is_none());

        // chop mid-record: cut 3 bytes off the end
        let len = std::fs::metadata(&p).unwrap().len();
        let f = File::options().write(true).open(&p).unwrap();
        f.set_len(len - 3).unwrap();
        drop(f);

        let torn = scan(&p).unwrap();
        assert_eq!(torn.records, 9);
        let (valid, _reason) = torn.torn.unwrap();
        assert!(valid < len - 3);

        // truncate to valid prefix -> clean read of 9 records
        truncate_to_valid(&p, valid).unwrap();
        let clean = scan(&p).unwrap();
        assert_eq!(clean.records, 9);
        assert!(clean.torn.is_none());
    }

    #[test]
    fn detects_crc_corruption() {
        let d = tmp();
        let p = d.path().join("a.fbraw");
        write_sample(&p, 3);
        // flip one byte inside the last record's payload
        let mut bytes = std::fs::read(&p).unwrap();
        let n = bytes.len();
        bytes[n - 6] ^= 0xFF;
        std::fs::write(&p, &bytes).unwrap();
        let rep = scan(&p).unwrap();
        assert_eq!(rep.records, 2);
        assert!(rep.torn.is_some());
    }

    #[test]
    fn rejects_bad_header() {
        let d = tmp();
        let p = d.path().join("bad.fbraw");
        std::fs::write(&p, b"NOTMAGIC").unwrap();
        assert!(matches!(
            RawLogReader::open(&p),
            Err(RawLogError::BadHeader(_))
        ));
        let p2 = d.path().join("short.fbraw");
        std::fs::write(&p2, b"FB").unwrap();
        assert!(matches!(
            RawLogReader::open(&p2),
            Err(RawLogError::BadHeader(_))
        ));
    }

    #[test]
    fn empty_segment_is_valid() {
        let d = tmp();
        let p = d.path().join("empty.fbraw");
        let w = RawLogWriter::create(&p, 1, 5, 6, b"{}").unwrap();
        w.finish().unwrap();
        let rep = scan(&p).unwrap();
        assert_eq!(rep.records, 0);
        assert!(rep.torn.is_none());
    }
}
