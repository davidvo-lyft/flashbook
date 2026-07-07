//! Receive timestamps: process-monotonic ns (ordering, latency math) and
//! UNIX wall ns (cross-process alignment). Both are captured at socket read.

use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static ANCHOR: OnceLock<Instant> = OnceLock::new();

/// Initialize the monotonic anchor; call once at process start so early
/// events don't pay the `OnceLock` init.
pub fn init() {
    let _ = mono_ns();
}

/// Nanoseconds since the process clock anchor (monotonic, never goes back).
/// Only comparable within one process run.
#[inline]
pub fn mono_ns() -> u64 {
    let anchor = *ANCHOR.get_or_init(Instant::now);
    u64::try_from(anchor.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Nanoseconds since UNIX epoch (wall clock; can step under NTP).
#[inline]
pub fn wall_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mono_is_monotonic() {
        init();
        let mut prev = mono_ns();
        for _ in 0..1000 {
            let now = mono_ns();
            assert!(now >= prev);
            prev = now;
        }
    }

    #[test]
    fn wall_is_sane() {
        // between 2020-01-01 and 2100-01-01
        let w = wall_ns();
        assert!(w > 1_577_836_800_000_000_000);
        assert!(w < 4_102_444_800_000_000_000);
    }
}
