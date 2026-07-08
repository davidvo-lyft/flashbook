//! Unit/integration tests for the capture plumbing: rotating raw sinks,
//! stats emission, backoff, REST envelopes, and the parse-fallback policy
//! (driven through a MockCodec — no network anywhere in this file).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use flashbook_feed::codec::{CodecError, CodecStats, Signal, VenueCodec};
use flashbook_feed::conn::{
    self, EnvelopeError, HEALTHY_SESSION_MIN, backoff_ceiling, idle_deadline, next_backoff,
    parse_rest_envelope, process_payload, rest_envelope, retry_after_delay, session_healthy,
    should_log_parse_error, stagger_offset,
};
use flashbook_feed::sink::{RotatingRawLog, SinkGauges, meta_json, should_rotate};
use flashbook_feed::stats::{EmitterEntry, VenueStats, emit_lines, run_stats_emitter};
use flashbook_proto::rawlog;
use flashbook_proto::{Event, EventKind, Venue};
use proptest::prelude::*;

// ---------------------------------------------------------------- MockCodec

/// Scriptable codec: behavior keyed off the payload prefix.
///
/// - `fast...` -> fast path emits one event
/// - `gap...` -> fast path emits a Gap event + NeedResync signal
/// - `slow...` -> fast path Structure error, slow path emits two events
/// - `bad...` -> fast path Structure error, slow path appends a partial
///   event and THEN errors (exercises buffer restore)
/// - `unknown...` -> fast path UnknownInstrument (no slow retry expected)
struct MockCodec {
    stats: CodecStats,
    fast_calls: u64,
    slow_calls: u64,
}

impl MockCodec {
    fn new() -> Self {
        Self {
            stats: CodecStats::default(),
            fast_calls: 0,
            slow_calls: 0,
        }
    }

    fn event(kind: EventKind, mono: u64, wall: u64) -> Event {
        Event {
            recv_mono_ns: mono,
            recv_wall_ns: wall,
            instrument: 11,
            kind: kind as u8,
            venue: Venue::Kraken as u8,
            ..Event::ZERO
        }
    }
}

impl VenueCodec for MockCodec {
    fn venue(&self) -> Venue {
        Venue::Kraken
    }

    fn ws_url(&self) -> String {
        "wss://mock.invalid/ws".into()
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
        self.fast_calls += 1;
        if payload.starts_with(b"fast") {
            out.push(Self::event(EventKind::Trade, recv_mono_ns, recv_wall_ns));
            Ok(Signal::None)
        } else if payload.starts_with(b"gap") {
            out.push(Self::event(EventKind::Gap, recv_mono_ns, recv_wall_ns));
            Ok(Signal::NeedResync { instrument: 11 })
        } else if payload.starts_with(b"unknown") {
            Err(CodecError::UnknownInstrument)
        } else {
            Err(CodecError::Structure("mock fast"))
        }
    }

    fn parse_slow(
        &mut self,
        payload: &[u8],
        recv_mono_ns: u64,
        recv_wall_ns: u64,
        out: &mut Vec<Event>,
    ) -> Result<Signal, CodecError> {
        self.slow_calls += 1;
        if payload.starts_with(b"slow") {
            out.push(Self::event(EventKind::BidSet, recv_mono_ns, recv_wall_ns));
            out.push(Self::event(EventKind::AskSet, recv_mono_ns, recv_wall_ns));
            Ok(Signal::None)
        } else {
            // Emit partial garbage before failing: process_payload must
            // restore the buffer so none of it leaks out.
            out.push(Self::event(EventKind::Clear, recv_mono_ns, recv_wall_ns));
            Err(CodecError::Structure("mock slow"))
        }
    }

    fn stats(&self) -> &CodecStats {
        &self.stats
    }
}

// -------------------------------------------------------------------- sinks

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "fbraw"))
        .collect();
    files.sort();
    files
}

#[test]
fn rotation_by_size_produces_clean_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let meta = meta_json([("BTC/USD", 11), ("ETH/USD", 12)]);
    let mut sink = RotatingRawLog::create(
        tmp.path(),
        Venue::Kraken,
        meta.clone(),
        300, // tiny byte budget -> rotate every few records
        Duration::from_secs(3600),
        false,
    )
    .unwrap();

    let payload = [b'x'; 40];
    for i in 0..50u64 {
        sink.append(rawlog::rkind::WS_TEXT, i, i + 1, &payload)
            .unwrap();
    }
    let segments = sink.segments_created();
    assert!(segments > 5, "expected many rotations, got {segments}");
    assert!(sink.current_bytes() > 0);
    sink.finish().unwrap();

    let files = segment_files(&tmp.path().join("kraken"));
    assert_eq!(files.len() as u64, segments);
    let mut total_records = 0;
    for f in &files {
        let name = f.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.starts_with("kraken-"), "bad segment name {name}");
        let rep = rawlog::scan(f).unwrap();
        assert!(rep.torn.is_none(), "torn segment {name}");
        total_records += rep.records;
        let rd = rawlog::RawLogReader::open(f).unwrap();
        assert_eq!(rd.header.venue, Venue::Kraken as u8);
        assert_eq!(rd.header.meta, meta);
    }
    assert_eq!(total_records, 50);
}

#[test]
fn rotation_by_age() {
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = RotatingRawLog::create(
        tmp.path(),
        Venue::Coinbase,
        meta_json([("BTC-USD", 1)]),
        u64::MAX,
        Duration::from_millis(150),
        false,
    )
    .unwrap();
    sink.append(rawlog::rkind::WS_TEXT, 1, 2, b"first").unwrap();
    assert!(
        !sink.maybe_rotate().unwrap(),
        "should not rotate while young"
    );
    std::thread::sleep(Duration::from_millis(200));
    assert!(sink.maybe_rotate().unwrap(), "age budget exceeded");
    assert_eq!(sink.segments_created(), 2);
    sink.append(rawlog::rkind::NOTE, 3, 4, b"second").unwrap();
    sink.sync().unwrap();
    sink.finish().unwrap();

    let files = segment_files(&tmp.path().join("coinbase"));
    assert_eq!(files.len(), 2);
    let reps: Vec<_> = files.iter().map(|f| rawlog::scan(f).unwrap()).collect();
    assert!(reps.iter().all(|r| r.torn.is_none()));
    assert_eq!(reps.iter().map(|r| r.records).sum::<u64>(), 2);
}

#[test]
fn tick_arithmetic_helpers() {
    // Rotation predicate.
    assert!(!should_rotate(
        10,
        100,
        Duration::ZERO,
        Duration::from_secs(900)
    ));
    assert!(should_rotate(
        100,
        100,
        Duration::ZERO,
        Duration::from_secs(900)
    ));
    assert!(should_rotate(
        0,
        100,
        Duration::from_secs(900),
        Duration::from_secs(900)
    ));
    assert!(!should_rotate(
        99,
        100,
        Duration::from_secs(899),
        Duration::from_secs(900)
    ));

    // Idle deadline.
    let now = tokio::time::Instant::now();
    assert_eq!(
        idle_deadline(now, Duration::from_secs(60)),
        now + Duration::from_secs(60)
    );

    // Staggering spreads n targets evenly over one period.
    let p = Duration::from_secs(900);
    assert_eq!(stagger_offset(0, 3, p), Duration::from_secs(300));
    assert_eq!(stagger_offset(1, 3, p), Duration::from_secs(600));
    assert_eq!(stagger_offset(2, 3, p), p);
}

// -------------------------------------------------------------------- stats

fn entry(venue: Venue, msgs: u64, events: u64, segments: u64, seg_bytes: u64) -> EmitterEntry {
    let stats = Arc::new(VenueStats::default());
    stats.msgs.store(msgs, Ordering::Relaxed);
    stats.events.store(events, Ordering::Relaxed);
    stats.gaps.store(1, Ordering::Relaxed);
    let gauges = Arc::new(SinkGauges::default());
    gauges.segments.store(segments, Ordering::Relaxed);
    gauges.current_bytes.store(seg_bytes, Ordering::Relaxed);
    EmitterEntry {
        venue,
        stats,
        gauges,
    }
}

#[test]
fn stats_lines_have_documented_json_shape() {
    let entries = vec![
        entry(Venue::Coinbase, 10, 100, 2, 4096),
        entry(Venue::Binance, 5, 50, 1, 512),
    ];
    let out = emit_lines(&entries, 1_700_000_000_000_000_000, 42, 64, 3600);
    let lines: Vec<&str> = out.trim_end().split('\n').collect();
    assert_eq!(lines.len(), 3); // coinbase, binance, total

    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        for field in [
            "ts_wall_ns",
            "venue",
            "msgs",
            "bytes",
            "events",
            "gaps",
            "resyncs",
            "reconnects",
            "fallbacks",
            "parse_errors",
            "rest_snaps",
            "rss_mb",
            "rss_max_mb",
            "segments",
            "current_segment_bytes",
            "uptime_s",
        ] {
            assert!(v.get(field).is_some(), "missing field {field} in {line}");
        }
        assert_eq!(v["ts_wall_ns"].as_u64(), Some(1_700_000_000_000_000_000));
        assert_eq!(v["rss_mb"].as_u64(), Some(42));
        assert_eq!(v["rss_max_mb"].as_u64(), Some(64));
        assert_eq!(v["uptime_s"].as_u64(), Some(3600));
    }

    let coinbase: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(coinbase["venue"], "coinbase");
    assert_eq!(coinbase["msgs"].as_u64(), Some(10));
    let total: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
    assert_eq!(total["venue"], "total");
    assert_eq!(total["msgs"].as_u64(), Some(15));
    assert_eq!(total["events"].as_u64(), Some(150));
    assert_eq!(total["gaps"].as_u64(), Some(2));
    assert_eq!(total["segments"].as_u64(), Some(3));
    assert_eq!(total["current_segment_bytes"].as_u64(), Some(4608));
}

#[tokio::test]
async fn stats_emitter_appends_jsonl_ticks() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("soak").join("stats.jsonl");
    let entries = vec![entry(Venue::Kraken, 7, 70, 1, 100)];
    let (tx, rx) = tokio::sync::watch::channel(false);
    let h = tokio::spawn(run_stats_emitter(
        entries,
        path.clone(),
        Duration::from_millis(20),
        rx,
    ));
    tokio::time::sleep(Duration::from_millis(90)).await;
    tx.send(true).unwrap();
    h.await.unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = contents.trim_end().split('\n').collect();
    assert!(
        lines.len() >= 2,
        "expected >=1 tick (2 lines each), got {}",
        lines.len()
    );
    assert_eq!(lines.len() % 2, 0, "each tick appends venue + total lines");
    for line in lines {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(v["venue"] == "kraken" || v["venue"] == "total");
        assert_eq!(v["msgs"].as_u64(), Some(7));
    }
}

#[tokio::test]
async fn stats_emitter_flushes_final_batch_on_shutdown() {
    // A run far shorter than the emit period must still leave one full
    // batch behind (the final flush on shutdown).
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("stats.jsonl");
    let entries = vec![entry(Venue::Kraken, 3, 30, 1, 10)];
    let (tx, rx) = tokio::sync::watch::channel(false);
    let h = tokio::spawn(run_stats_emitter(
        entries,
        path.clone(),
        Duration::from_secs(3600), // would never tick during the test
        rx,
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.send(true).unwrap();
    h.await.unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = contents.trim_end().split('\n').collect();
    assert_eq!(lines.len(), 2, "final flush = one venue line + one total");
    for line in lines {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(v["venue"] == "kraken" || v["venue"] == "total");
        assert_eq!(v["msgs"].as_u64(), Some(3));
    }
}

// ------------------------------------------------------------------ backoff

#[test]
fn backoff_ceiling_is_exponential_and_capped() {
    let floor = Duration::from_secs(1);
    let cap = Duration::from_secs(60);
    assert_eq!(backoff_ceiling(0, floor, cap), Duration::from_secs(1));
    assert_eq!(backoff_ceiling(1, floor, cap), Duration::from_secs(2));
    assert_eq!(backoff_ceiling(2, floor, cap), Duration::from_secs(4));
    assert_eq!(backoff_ceiling(5, floor, cap), Duration::from_secs(32));
    assert_eq!(backoff_ceiling(6, floor, cap), cap); // 64 clamped
    assert_eq!(backoff_ceiling(31, floor, cap), cap);
    assert_eq!(backoff_ceiling(u32::MAX, floor, cap), cap); // no overflow
}

#[test]
fn backoff_jitter_stays_in_bounds_and_spreads() {
    let floor = Duration::from_secs(1);
    let cap = Duration::from_secs(60);

    // attempt 0 jitters too: uniform in [floor, 2*floor], so retries never
    // hammer a venue at exactly 1/floor in lockstep.
    let samples0: Vec<Duration> = (0..1000).map(|_| next_backoff(0, floor, cap)).collect();
    for s in &samples0 {
        assert!(*s >= floor, "attempt 0 below floor: {s:?}");
        assert!(*s <= floor * 2, "attempt 0 above 2*floor: {s:?}");
    }
    let mid = floor + Duration::from_millis(500);
    assert!(
        samples0.iter().any(|s| *s > mid),
        "attempt 0 never jittered up"
    );
    assert!(
        samples0.iter().any(|s| *s < mid),
        "attempt 0 never jittered down"
    );

    // Degenerate cap == floor: the cap still wins (no jitter possible).
    for _ in 0..100 {
        assert_eq!(next_backoff(0, floor, floor), floor);
    }

    // attempt 6 (capped): full jitter over [1s, 60s].
    let samples: Vec<Duration> = (0..1000).map(|_| next_backoff(6, floor, cap)).collect();
    for s in &samples {
        assert!(*s >= floor, "below floor: {s:?}");
        assert!(*s <= cap, "above cap: {s:?}");
    }
    let max = samples.iter().max().unwrap();
    let min = samples.iter().min().unwrap();
    assert!(
        *max > Duration::from_secs(45),
        "jitter never reached upper range: {max:?}"
    );
    assert!(
        *min < Duration::from_secs(20),
        "jitter never reached lower range: {min:?}"
    );

    // Growth: per-attempt ceilings dominate earlier ones.
    for attempt in 0..6u32 {
        let c0 = backoff_ceiling(attempt, floor, cap);
        let c1 = backoff_ceiling(attempt + 1, floor, cap);
        assert_eq!(c1, (c0 * 2).min(cap));
    }
}

#[test]
fn session_health_rule_gates_backoff_reset() {
    // Healthy = lasted >= HEALTHY_SESSION_MIN AND parsed >= 1 frame.
    assert!(session_healthy(HEALTHY_SESSION_MIN, 1));
    assert!(session_healthy(Duration::from_secs(7200), 1_000_000));
    // Accept-then-drop venue: session too short, never resets backoff.
    assert!(!session_healthy(
        HEALTHY_SESSION_MIN - Duration::from_millis(1),
        1
    ));
    assert!(!session_healthy(Duration::from_millis(50), 10_000));
    // Long-lived but silent/unparseable session is not healthy either.
    assert!(!session_healthy(Duration::from_secs(7200), 0));
    assert!(!session_healthy(Duration::ZERO, 0));
}

#[test]
fn retry_after_parsing_and_clamping() {
    // Delta-seconds form, honored as-is inside the clamp range.
    assert_eq!(retry_after_delay(Some("300")), Duration::from_secs(300));
    assert_eq!(retry_after_delay(Some(" 42 ")), Duration::from_secs(42));
    assert_eq!(retry_after_delay(Some("1")), Duration::from_secs(1));
    assert_eq!(retry_after_delay(Some("3600")), Duration::from_secs(3600));
    // Clamped to [1 s, 3600 s].
    assert_eq!(retry_after_delay(Some("0")), Duration::from_secs(1));
    assert_eq!(retry_after_delay(Some("999999")), Duration::from_secs(3600));
    // Absent or unparseable (HTTP-date form, garbage) -> 120 s default.
    assert_eq!(retry_after_delay(None), Duration::from_secs(120));
    assert_eq!(
        retry_after_delay(Some("Wed, 21 Oct 2026 07:28:00 GMT")),
        Duration::from_secs(120)
    );
    assert_eq!(retry_after_delay(Some("")), Duration::from_secs(120));
    assert_eq!(retry_after_delay(Some("-5")), Duration::from_secs(120));
    assert_eq!(retry_after_delay(Some("12.5")), Duration::from_secs(120));
}

#[test]
fn parse_error_warn_rate_limit_schedule() {
    // Full detail for the first 5 errors of a session...
    for n in 1..=5u64 {
        assert!(should_log_parse_error(n), "error #{n} must log");
    }
    // ...then only every 1000th.
    assert!(!should_log_parse_error(6));
    assert!(!should_log_parse_error(999));
    assert!(should_log_parse_error(1000));
    assert!(!should_log_parse_error(1001));
    assert!(should_log_parse_error(2000));
    assert!(!should_log_parse_error(2001));
}

// ---------------------------------------------------------- REST envelopes

#[test]
fn rest_envelope_roundtrip_preserves_body_verbatim() {
    let cases: &[&[u8]] = &[
        br#"{"bids":[["1.5","2"]],"asks":[]}"#,
        b"",
        b"   {\"spaced\": true}  ",
        "{\"unicode\":\"\u{20ac}\u{1f680} caf\u{e9}\"}".as_bytes(),
        br#"{"nested":{"body":",\"body\":"}}"#, // body containing the marker
        &[0xff, 0xfe, 0x00, 0x01],              // non-UTF8 garbage body
    ];
    for (i, body) in cases.iter().enumerate() {
        let env = rest_envelope(6, "BTCUSDT", "https://api.binance.com/api/v3/depth", body);
        let (instr, got) = parse_rest_envelope(&env).unwrap();
        assert_eq!(instr, 6, "case {i}");
        assert_eq!(got, *body, "case {i}: body not byte-identical");
    }

    // Hostile symbol/url strings must be escaped, not break the frame.
    let env = rest_envelope(3, "we\"ird\\sym", "https://x/?a=\",\"body\":", b"{\"k\":1}");
    let (instr, got) = parse_rest_envelope(&env).unwrap();
    assert_eq!(instr, 3);
    assert_eq!(got, b"{\"k\":1}");
}

#[test]
fn rest_envelope_rejects_malformed() {
    assert_eq!(parse_rest_envelope(b""), Err(EnvelopeError::BadPrefix));
    assert_eq!(
        parse_rest_envelope(b"garbage"),
        Err(EnvelopeError::BadPrefix)
    );
    assert_eq!(
        parse_rest_envelope(b"{\"instrument\":abc,\"body\":{}}"),
        Err(EnvelopeError::BadInstrument)
    );
    assert_eq!(
        parse_rest_envelope(b"{\"instrument\":5,\"venue_symbol\":\"X\"}"),
        Err(EnvelopeError::BadBody)
    );
    // Truncated mid-body (no closing brace at all).
    assert_eq!(
        parse_rest_envelope(b"{\"instrument\":5,\"body\":{\"x\":1"),
        Err(EnvelopeError::BadBody)
    );
    // Instrument id larger than u32.
    assert_eq!(
        parse_rest_envelope(b"{\"instrument\":99999999999,\"body\":{}}"),
        Err(EnvelopeError::BadInstrument)
    );
}

// --------------------------------------------------- parse-fallback policy

#[test]
fn fast_path_success_and_resync_signal() {
    let mut codec = MockCodec::new();
    let stats = VenueStats::default();
    let mut errs = 0u64;
    let mut out = Vec::new();

    let sig = process_payload(
        &mut codec,
        b"fast frame",
        10,
        20,
        &stats,
        &mut errs,
        &mut out,
    );
    assert_eq!(sig, Some(Signal::None));
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].recv_mono_ns, 10);
    assert_eq!(out[0].recv_wall_ns, 20);
    assert_eq!(codec.slow_calls, 0, "fast success must not touch slow path");
    assert_eq!(stats.fallbacks.load(Ordering::Relaxed), 0);
    assert_eq!(stats.parse_errors.load(Ordering::Relaxed), 0);
    assert_eq!(errs, 0, "success must not bump the session error counter");

    out.clear();
    let sig = process_payload(
        &mut codec,
        b"gap frame",
        11,
        21,
        &stats,
        &mut errs,
        &mut out,
    );
    assert_eq!(sig, Some(Signal::NeedResync { instrument: 11 }));
    conn::account_events(&stats, &out);
    assert_eq!(stats.events.load(Ordering::Relaxed), 1);
    assert_eq!(stats.gaps.load(Ordering::Relaxed), 1);
}

#[test]
fn structure_error_falls_back_to_slow() {
    let mut codec = MockCodec::new();
    let stats = VenueStats::default();
    let mut errs = 0u64;
    let mut out = vec![MockCodec::event(EventKind::Heartbeat, 1, 1)]; // pre-existing

    let sig = process_payload(
        &mut codec,
        b"slow frame",
        30,
        40,
        &stats,
        &mut errs,
        &mut out,
    );
    assert_eq!(sig, Some(Signal::None));
    assert_eq!(codec.fast_calls, 1);
    assert_eq!(codec.slow_calls, 1);
    assert_eq!(stats.fallbacks.load(Ordering::Relaxed), 1);
    assert_eq!(stats.parse_errors.load(Ordering::Relaxed), 0);
    assert_eq!(errs, 0, "fallback success is not a parse error");
    // Pre-existing event untouched, two new ones appended by the slow path.
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].kind, EventKind::Heartbeat as u8);
    assert_eq!(out[1].kind, EventKind::BidSet as u8);
    assert_eq!(out[2].kind, EventKind::AskSet as u8);
}

#[test]
fn both_paths_failing_counts_error_and_restores_buffer() {
    let mut codec = MockCodec::new();
    let stats = VenueStats::default();
    let mut errs = 0u64;
    let mut out = vec![MockCodec::event(EventKind::Heartbeat, 1, 1)];

    // MockCodec's slow path appends a partial event before erroring; the
    // policy must roll the buffer back to exactly its prior contents.
    let sig = process_payload(
        &mut codec,
        b"bad frame",
        50,
        60,
        &stats,
        &mut errs,
        &mut out,
    );
    assert_eq!(sig, None);
    assert_eq!(codec.fast_calls, 1);
    assert_eq!(codec.slow_calls, 1);
    assert_eq!(stats.fallbacks.load(Ordering::Relaxed), 0);
    assert_eq!(stats.parse_errors.load(Ordering::Relaxed), 1);
    assert_eq!(errs, 1, "session error counter tracks parse errors");
    assert_eq!(
        out.len(),
        1,
        "no partial garbage events for a failed message"
    );
    assert_eq!(out[0].kind, EventKind::Heartbeat as u8);

    // The counter keeps climbing across failures (rate-limit input).
    for _ in 0..4 {
        let _ = process_payload(
            &mut codec,
            b"bad frame",
            51,
            61,
            &stats,
            &mut errs,
            &mut out,
        );
    }
    assert_eq!(errs, 5);
    assert_eq!(stats.parse_errors.load(Ordering::Relaxed), 5);
}

#[test]
fn non_structure_error_skips_slow_path() {
    let mut codec = MockCodec::new();
    let stats = VenueStats::default();
    let mut errs = 0u64;
    let mut out = Vec::new();

    let sig = process_payload(
        &mut codec,
        b"unknown symbol",
        70,
        80,
        &stats,
        &mut errs,
        &mut out,
    );
    assert_eq!(sig, None);
    assert_eq!(codec.fast_calls, 1);
    assert_eq!(
        codec.slow_calls, 0,
        "UnknownInstrument must not retry via slow"
    );
    assert_eq!(stats.fallbacks.load(Ordering::Relaxed), 0);
    assert_eq!(stats.parse_errors.load(Ordering::Relaxed), 1);
    assert_eq!(errs, 1);
    assert!(out.is_empty());
}

// ----------------------------------------------------------------- proptest

proptest! {
    #[test]
    fn parse_rest_envelope_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..300)) {
        let _ = parse_rest_envelope(&bytes);
    }

    #[test]
    fn rest_envelope_roundtrips_any_inputs(
        instr in 1u32..=u32::MAX,
        sym in any::<String>(),
        url in any::<String>(),
        body in proptest::collection::vec(any::<u8>(), 0..200),
    ) {
        let env = rest_envelope(instr, &sym, &url, &body);
        let (got_instr, got_body) = parse_rest_envelope(&env).unwrap();
        prop_assert_eq!(got_instr, instr);
        prop_assert_eq!(got_body, &body[..]);
    }
}
