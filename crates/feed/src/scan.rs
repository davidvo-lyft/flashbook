//! Zero-allocation scanning primitives for the fast-path codecs.
//!
//! Venue payloads are compact machine-generated JSON with known shapes; the
//! fast path doesn't build a DOM, it walks bytes. Anything surprising makes
//! the codec return `CodecError::Structure` and the caller retries via the
//! serde_json slow path — so these primitives may be strict.

use memchr::memmem;

/// Byte cursor over one payload.
#[derive(Debug, Clone)]
pub struct Cursor<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cursor<'a> {
    /// New cursor at offset 0.
    #[inline]
    pub fn new(b: &'a [u8]) -> Self {
        Self { b, i: 0 }
    }

    /// Current offset.
    #[inline]
    pub fn pos(&self) -> usize {
        self.i
    }

    /// Jump to an absolute offset (clamped to len).
    #[inline]
    pub fn set_pos(&mut self, i: usize) {
        self.i = i.min(self.b.len());
    }

    /// Unread remainder.
    #[inline]
    pub fn rest(&self) -> &'a [u8] {
        &self.b[self.i..]
    }

    /// True when fully consumed.
    #[inline]
    pub fn done(&self) -> bool {
        self.i >= self.b.len()
    }

    /// Next byte without consuming.
    #[inline]
    pub fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    /// Consume one byte if it equals `c`.
    #[inline]
    pub fn eat(&mut self, c: u8) -> bool {
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    /// Skip JSON insignificant whitespace (rare in venue payloads).
    #[inline]
    pub fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }

    /// Advance to just past the next occurrence of `needle` using a
    /// precomputed searcher. Returns `None` (cursor unchanged) if absent.
    #[inline]
    pub fn skip_past_finder(&mut self, f: &memmem::Finder<'_>) -> Option<()> {
        let at = f.find(self.rest())?;
        self.i += at + f.needle().len();
        Some(())
    }

    /// Convenience form of [`Cursor::skip_past_finder`] for tests/cold paths.
    pub fn skip_past(&mut self, needle: &[u8]) -> Option<()> {
        let at = memmem::find(self.rest(), needle)?;
        self.i += at + needle.len();
        Some(())
    }

    /// Read a JSON string assuming the cursor is at the opening quote.
    /// Returns the raw inner bytes. Fails on escapes (`\`) — venue symbol /
    /// number-in-string tokens never contain them; anything else goes to the
    /// slow path.
    #[inline]
    pub fn read_string(&mut self) -> Option<&'a [u8]> {
        if !self.eat(b'"') {
            return None;
        }
        let start = self.i;
        let rel = memchr::memchr(b'"', &self.b[start..])?;
        let inner = &self.b[start..start + rel];
        if memchr::memchr(b'\\', inner).is_some() {
            return None;
        }
        self.i = start + rel + 1;
        Some(inner)
    }

    /// Read a JSON number token span: `[-+0-9.eE]+` (at least one byte).
    /// Returns the raw token; exactness is enforced by
    /// [`flashbook_proto::parse_fixed`] downstream.
    #[inline]
    pub fn read_number(&mut self) -> Option<&'a [u8]> {
        let start = self.i;
        while let Some(c) = self.peek() {
            match c {
                b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E' => self.i += 1,
                _ => break,
            }
        }
        if self.i == start {
            None
        } else {
            Some(&self.b[start..self.i])
        }
    }

    /// Read an unsigned decimal integer (no sign, no dot). Fails on overflow.
    #[inline]
    pub fn read_u64(&mut self) -> Option<u64> {
        let mut v: u64 = 0;
        let start = self.i;
        while let Some(c @ b'0'..=b'9') = self.peek() {
            v = v.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
            self.i += 1;
        }
        if self.i == start { None } else { Some(v) }
    }
}

/// Parse a strict RFC3339 UTC timestamp (`YYYY-MM-DDTHH:MM:SS[.f{1..9}]Z`)
/// into ns since the UNIX epoch. Rejects offsets other than `Z`, dates
/// before 1970, and any trailing bytes.
pub fn parse_rfc3339_ns(b: &[u8]) -> Option<u64> {
    #[inline]
    fn d(b: &[u8], i: usize) -> Option<u64> {
        let c = *b.get(i)?;
        c.is_ascii_digit().then(|| u64::from(c - b'0'))
    }
    #[inline]
    fn d2(b: &[u8], i: usize) -> Option<u64> {
        Some(d(b, i)? * 10 + d(b, i + 1)?)
    }
    if b.len() < 20 {
        return None;
    }
    let year = d(b, 0)? * 1000 + d(b, 1)? * 100 + d(b, 2)? * 10 + d(b, 3)?;
    if b[4] != b'-' || b[7] != b'-' || (b[10] != b'T' && b[10] != b't') {
        return None;
    }
    let month = d2(b, 5)?;
    let day = d2(b, 8)?;
    if b[13] != b':' || b[16] != b':' {
        return None;
    }
    let hour = d2(b, 11)?;
    let min = d2(b, 14)?;
    let sec = d2(b, 17)?;
    if !(1..=12).contains(&month) || hour > 23 || min > 59 || sec > 59 {
        return None;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    const DIM: [u64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let dim = if month == 2 && leap {
        29
    } else {
        DIM[month as usize - 1]
    };
    if day == 0 || day > dim {
        return None;
    }

    // Howard Hinnant's days_from_civil (year >= 1970 enforced below).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y / 400;
    let yoe = y - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe;
    if days < 719_468 {
        return None; // before 1970-01-01
    }
    let epoch_days = days - 719_468;

    let mut i = 19;
    let mut frac: u64 = 0;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let ndigits = i - start;
        if ndigits == 0 || ndigits > 9 {
            return None;
        }
        for &c in &b[start..i] {
            frac = frac * 10 + u64::from(c - b'0');
        }
        frac *= 10u64.pow(9 - u32::try_from(ndigits).ok()?);
    }
    if b.get(i) != Some(&b'Z') && b.get(i) != Some(&b'z') {
        return None;
    }
    if i + 1 != b.len() {
        return None;
    }

    let secs = epoch_days * 86_400 + hour * 3_600 + min * 60 + sec;
    secs.checked_mul(1_000_000_000)?.checked_add(frac)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_goldens() {
        // golden values computed independently (python datetime, UTC)
        for (s, want) in [
            ("2026-07-07T22:58:17.164892Z", 1_783_465_097_164_892_000),
            ("1970-01-01T00:00:00Z", 0),
            ("2000-02-29T12:00:00Z", 951_825_600_000_000_000),
            ("2026-01-01T00:00:00Z", 1_767_225_600_000_000_000),
            ("2038-01-19T03:14:08Z", 2_147_483_648_000_000_000),
            ("1999-12-31T23:59:59.999999999Z", 946_684_799_999_999_999),
        ] {
            assert_eq!(parse_rfc3339_ns(s.as_bytes()), Some(want), "{s}");
        }
    }

    #[test]
    fn rfc3339_fraction_scaling() {
        assert_eq!(
            parse_rfc3339_ns(b"1970-01-01T00:00:00.5Z"),
            Some(500_000_000)
        );
        assert_eq!(
            parse_rfc3339_ns(b"1970-01-01T00:00:00.123Z"),
            Some(123_000_000)
        );
        assert_eq!(parse_rfc3339_ns(b"1970-01-01T00:00:00.000000001Z"), Some(1));
    }

    #[test]
    fn rfc3339_rejects() {
        for bad in [
            "",
            "2026-07-07",
            "2026-07-07T22:58:17",             // no Z
            "2026-07-07T22:58:17+00:00",       // offset form not used by venues
            "2026-07-07 22:58:17Z",            // space separator
            "2026-13-07T22:58:17Z",            // month 13
            "2026-00-07T22:58:17Z",            // month 0
            "2026-07-32T22:58:17Z",            // day 32
            "2026-07-00T22:58:17Z",            // day 0
            "2001-02-29T00:00:00Z",            // not a leap year
            "2026-07-07T24:00:00Z",            // hour 24
            "2026-07-07T22:60:00Z",            // minute 60
            "2026-07-07T22:58:60Z",            // second 60 (venues don't emit leap seconds)
            "2026-07-07T22:58:17.Z",           // empty fraction
            "2026-07-07T22:58:17.1234567890Z", // 10-digit fraction
            "2026-07-07T22:58:17Zx",           // trailing junk
            "1969-12-31T23:59:59Z",            // pre-epoch
            "2026-07-07T22:58:1aZ",
        ] {
            assert_eq!(parse_rfc3339_ns(bad.as_bytes()), None, "{bad}");
        }
    }

    #[test]
    fn rfc3339_leap_year_boundaries() {
        assert!(parse_rfc3339_ns(b"2000-02-29T00:00:00Z").is_some()); // 400-rule leap
        assert!(parse_rfc3339_ns(b"1900-02-29T00:00:00Z").is_none()); // pre-epoch anyway
        assert!(parse_rfc3339_ns(b"2100-02-29T00:00:00Z").is_none()); // 100-rule non-leap
        assert!(parse_rfc3339_ns(b"2024-02-29T00:00:00Z").is_some());
        assert!(parse_rfc3339_ns(b"2026-02-29T00:00:00Z").is_none());
    }

    #[test]
    fn cursor_strings_and_numbers() {
        let mut c = Cursor::new(br#"{"price":"63589.64","qty":1.5e-3,"id":42}"#);
        c.skip_past(b"\"price\":").unwrap();
        assert_eq!(c.read_string().unwrap(), b"63589.64");
        c.skip_past(b"\"qty\":").unwrap();
        assert_eq!(c.read_number().unwrap(), b"1.5e-3");
        c.skip_past(b"\"id\":").unwrap();
        assert_eq!(c.read_u64().unwrap(), 42);
        assert!(c.eat(b'}'));
        assert!(c.done());
    }

    #[test]
    fn cursor_rejects_escapes_and_missing() {
        let mut c = Cursor::new(br#""has\"escape""#);
        assert!(c.read_string().is_none());
        let mut c = Cursor::new(b"noquote");
        assert!(c.read_string().is_none());
        let mut c = Cursor::new(b"\"unterminated");
        assert!(c.read_string().is_none());
        let mut c = Cursor::new(b"x123");
        assert!(c.read_number().is_none());
        assert!(c.read_u64().is_none());
        let mut c = Cursor::new(b"99999999999999999999999");
        assert!(c.read_u64().is_none()); // overflow
    }

    #[test]
    fn cursor_finder_navigation() {
        let f = memmem::Finder::new(b"\"s\":");
        let mut c = Cursor::new(br#"{"a":1,"s":"BTCUSDT","x":2}"#);
        c.skip_past_finder(&f).unwrap();
        assert_eq!(c.read_string().unwrap(), b"BTCUSDT");
        assert!(c.skip_past_finder(&f).is_none());
        let before = c.pos();
        assert_eq!(c.pos(), before);
    }
}
