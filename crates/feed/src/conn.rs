//! Connection layer: one long-lived task per venue owning WS connect,
//! reconnect with jittered exponential backoff, idle watchdog, REST snapshot
//! orchestration, raw capture, and the fast/slow parse-fallback policy.
//!
//! # Backpressure policy
//!
//! Frames are processed **inline** in the read loop: raw append to the
//! rotating segment (buffered write) followed by parse. There are NO
//! unbounded queues anywhere in the capture path; the only buffering is the
//! kernel socket buffer, which bounds memory by construction. At soak rates
//! (hundreds of messages per second) inline processing costs microseconds
//! per frame, so the socket buffer never meaningfully fills.
//!
//! # Raw capture is ground truth
//!
//! Every received text frame is appended to the raw log **before** parsing:
//! a codec bug can never lose data, and every parse decision is replayable
//! from the segments after the fact.

use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use flashbook_proto::rawlog::rkind;
use flashbook_proto::{Event, EventKind, Venue, clock};

use crate::codec::{CodecError, Signal, VenueCodec};
use crate::sink::RotatingRawLog;
use crate::stats::VenueStats;

/// One REST snapshot target (a single instrument's book endpoint).
#[derive(Debug, Clone)]
pub struct RestTarget {
    /// Instrument id passed to `parse_rest_snapshot` (REST bodies don't
    /// self-identify).
    pub instrument: u32,
    /// Venue-native symbol, recorded in the envelope for humans/replay.
    pub venue_symbol: String,
    /// Full URL to GET.
    pub url: String,
}

/// A venue's REST snapshot plan (empty for venues that resync in-band).
#[derive(Debug, Clone, Default)]
pub struct RestPlan {
    /// Snapshot endpoints, one per subscribed instrument.
    pub targets: Vec<RestTarget>,
    /// Fetch every target after each (re)connect (Binance depth-sync).
    pub on_connect: bool,
    /// Periodic per-target refresh interval (staggered), if any.
    pub refresh_every: Option<Duration>,
    /// Minimum spacing between consecutive REST requests (rate-limit
    /// budget; e.g. 1.5 s for Binance `depth?limit=1000` at weight 50).
    pub min_spacing: Duration,
}

impl RestPlan {
    /// A plan with no REST activity at all (Kraken).
    pub fn none() -> Self {
        Self::default()
    }

    /// True if this plan never issues a request.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

/// Per-venue connection policy.
#[derive(Debug, Clone)]
pub struct VenueConfig {
    /// Force a reconnect if no frame arrives within this window (all
    /// subscribed venues heartbeat every ~1 s, so 60 s means "dead").
    pub idle_timeout: Duration,
    /// Reconnect backoff floor.
    pub backoff_floor: Duration,
    /// Reconnect backoff cap.
    pub backoff_cap: Duration,
    /// REST snapshot plan.
    pub rest: RestPlan,
}

impl Default for VenueConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(60),
            backoff_floor: Duration::from_secs(1),
            backoff_cap: Duration::from_secs(60),
            rest: RestPlan::none(),
        }
    }
}

/// WebSocket handshake budget: a TCP/TLS/upgrade handshake that hasn't
/// completed within this window is declared failed (backoff + retry) so a
/// stalled connect can never wedge a venue task while the process looks
/// alive.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// Minimum established-session duration for the reconnect backoff counter
/// to reset (see [`session_healthy`]).
pub const HEALTHY_SESSION_MIN: Duration = Duration::from_secs(30);

/// True if a finished session earned a backoff reset: it lasted at least
/// [`HEALTHY_SESSION_MIN`] AND parsed at least one frame. An accept-then-
/// drop venue (handshake succeeds, session dies instantly) therefore keeps
/// escalating backoff instead of being hammered at the floor rate forever.
pub fn session_healthy(duration: Duration, frames_parsed: u64) -> bool {
    duration >= HEALTHY_SESSION_MIN && frames_parsed >= 1
}

/// Deterministic backoff ceiling for `attempt` (0-based):
/// `min(cap, floor * 2^attempt)`.
pub fn backoff_ceiling(attempt: u32, floor: Duration, cap: Duration) -> Duration {
    let mult = 1u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX);
    floor.saturating_mul(mult).min(cap)
}

/// Exponential backoff with full jitter: uniform in
/// `[floor, max(backoff_ceiling(attempt), 2*floor)]`, capped at `cap`.
/// Always at least `floor`, never more than `cap`. Even attempt 0 jitters
/// (uniform in `[floor, 2*floor]`) so instances never retry in lockstep at
/// exactly the floor rate.
pub fn next_backoff(attempt: u32, floor: Duration, cap: Duration) -> Duration {
    use rand::Rng as _;
    let ceil = backoff_ceiling(attempt, floor, cap)
        .max(floor.saturating_mul(2))
        .min(cap);
    if ceil <= floor {
        return floor;
    }
    let span = (ceil - floor).as_nanos() as u64;
    let jitter = rand::rng().random_range(0..=span);
    floor + Duration::from_nanos(jitter)
}

/// Parse an HTTP `Retry-After` header value (delta-seconds form) into a
/// venue-wide REST pause. Absent or unparseable (e.g. HTTP-date form)
/// values default to 120 s; the result is always clamped to
/// `[1 s, 3600 s]`. Used on Binance 429/418 responses, where honoring the
/// header is the documented way to avoid extending an IP ban.
pub fn retry_after_delay(header: Option<&str>) -> Duration {
    const DEFAULT: Duration = Duration::from_secs(120);
    const MIN: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(3600);
    header
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map_or(DEFAULT, Duration::from_secs)
        .clamp(MIN, MAX)
}

/// Rate-limit predicate for unparseable-frame warns, keyed by the 1-based
/// count of parse errors this session: full detail for the first 5 after
/// each connect, then only every 1000th (a venue-wide format change would
/// otherwise emit gigabytes of identical warns per day).
pub fn should_log_parse_error(nth: u64) -> bool {
    nth <= 5 || nth.is_multiple_of(1000)
}

/// Idle-watchdog arithmetic: the instant at which a connection that last
/// produced a frame at `last_frame` must be declared dead.
#[inline]
pub fn idle_deadline(last_frame: Instant, idle_timeout: Duration) -> Instant {
    last_frame + idle_timeout
}

/// Staggered initial deadline for periodic REST target `i` of `n`: spreads
/// the n refreshes evenly across one `period` so they never bunch up.
#[inline]
pub fn stagger_offset(i: usize, n: usize, period: Duration) -> Duration {
    debug_assert!(n > 0);
    period.mul_f64((i + 1) as f64 / n as f64)
}

/// Errors from [`parse_rest_envelope`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EnvelopeError {
    /// Input doesn't start with the expected `{"instrument":` prefix.
    #[error("bad envelope prefix")]
    BadPrefix,
    /// Instrument id missing or not a u32.
    #[error("bad instrument id")]
    BadInstrument,
    /// `"body":` marker or closing brace missing.
    #[error("bad envelope body")]
    BadBody,
}

/// Wrap a REST response body in the raw-log envelope. The body is inserted
/// **verbatim** (byte-for-byte) so replay re-parses exactly what the venue
/// sent; the other fields are JSON-escaped strings.
pub fn rest_envelope(instrument: u32, venue_symbol: &str, url: &str, body: &[u8]) -> Vec<u8> {
    let sym = serde_json::to_string(venue_symbol).expect("string serializes");
    let url = serde_json::to_string(url).expect("string serializes");
    let mut out = Vec::with_capacity(64 + sym.len() + url.len() + body.len());
    out.extend_from_slice(b"{\"instrument\":");
    out.extend_from_slice(instrument.to_string().as_bytes());
    out.extend_from_slice(b",\"venue_symbol\":");
    out.extend_from_slice(sym.as_bytes());
    out.extend_from_slice(b",\"url\":");
    out.extend_from_slice(url.as_bytes());
    out.extend_from_slice(b",\"body\":");
    out.extend_from_slice(body);
    out.push(b'}');
    out
}

/// Inverse of [`rest_envelope`]: extract `(instrument, verbatim body)` from
/// an envelope. The body slice is exactly the bytes passed to
/// [`rest_envelope`] (no re-serialization, no trimming).
///
/// Framing note: the envelope's final `}` is positional (last byte), so a
/// truncated envelope is only detectable when the surviving last byte isn't
/// `}`. That is fine in practice: envelopes live inside CRC-framed rawlog
/// records, which already reject torn payloads.
pub fn parse_rest_envelope(env: &[u8]) -> Result<(u32, &[u8]), EnvelopeError> {
    const PREFIX: &[u8] = b"{\"instrument\":";
    const BODY: &[u8] = b",\"body\":";
    let rest = env.strip_prefix(PREFIX).ok_or(EnvelopeError::BadPrefix)?;
    let digits = rest.iter().take_while(|b| b.is_ascii_digit()).count();
    if digits == 0 {
        return Err(EnvelopeError::BadInstrument);
    }
    let instrument: u32 = std::str::from_utf8(&rest[..digits])
        .expect("ascii digits")
        .parse()
        .map_err(|_| EnvelopeError::BadInstrument)?;
    // The raw byte sequence `,"body":` contains unescaped quotes, so it can
    // never occur inside the (JSON-escaped) venue_symbol/url strings; the
    // first occurrence is therefore the real field boundary.
    let start = memchr::memmem::find(rest, BODY).ok_or(EnvelopeError::BadBody)? + BODY.len();
    if rest.last() != Some(&b'}') || start >= rest.len() {
        return Err(EnvelopeError::BadBody);
    }
    Ok((instrument, &rest[start..rest.len() - 1]))
}

/// Apply the parse-fallback policy for one frame payload: try the fast
/// path; on a `Structure` error retry the identical payload via
/// `parse_slow` (counting a fallback); if that also fails (or the fast path
/// failed with a non-structural error) count a parse error and log a
/// payload snippet. Guarantees `out` gains no events from a failed message
/// (buffer length is restored on every error path).
///
/// `session_parse_errors` is a per-connection counter (reset on connect);
/// warn logging is rate-limited through it via [`should_log_parse_error`]
/// so a venue format change cannot flood the log.
///
/// Returns the effective [`Signal`], or `None` if the message could not be
/// parsed at all.
pub fn process_payload<C: VenueCodec + ?Sized>(
    codec: &mut C,
    payload: &[u8],
    recv_mono_ns: u64,
    recv_wall_ns: u64,
    stats: &VenueStats,
    session_parse_errors: &mut u64,
    out: &mut Vec<Event>,
) -> Option<Signal> {
    use std::sync::atomic::Ordering;
    let base = out.len();
    let err = match codec.parse(payload, recv_mono_ns, recv_wall_ns, out) {
        Ok(sig) => return Some(sig),
        Err(e) => e,
    };
    out.truncate(base);
    let err = if matches!(err, CodecError::Structure(_)) {
        match codec.parse_slow(payload, recv_mono_ns, recv_wall_ns, out) {
            Ok(sig) => {
                stats.fallbacks.fetch_add(1, Ordering::Relaxed);
                return Some(sig);
            }
            Err(e) => e,
        }
    } else {
        err
    };
    out.truncate(base);
    stats.parse_errors.fetch_add(1, Ordering::Relaxed);
    *session_parse_errors += 1;
    if should_log_parse_error(*session_parse_errors) {
        let snippet = String::from_utf8_lossy(&payload[..payload.len().min(200)]);
        tracing::warn!(
            error = %err,
            payload = %snippet,
            session_parse_errors = *session_parse_errors,
            "unparseable frame"
        );
    }
    None
}

/// Count a freshly-parsed batch into `stats`: `events` by batch size,
/// `gaps` by emitted [`EventKind::Gap`] markers (called by the read loop
/// after every successful parse).
pub fn account_events(stats: &VenueStats, out: &[Event]) {
    use std::sync::atomic::Ordering;
    stats.events.fetch_add(out.len() as u64, Ordering::Relaxed);
    let gaps = out
        .iter()
        .filter(|e| e.kind == EventKind::Gap as u8)
        .count() as u64;
    if gaps > 0 {
        stats.gaps.fetch_add(gaps, Ordering::Relaxed);
    }
}

/// Per-connection REST scheduler state: one optional deadline and one
/// failure-backoff attempt counter per target, serialized globally by
/// `min_spacing`.
struct RestSched {
    due: Vec<Option<Instant>>,
    last_fetch: Option<Instant>,
    attempts: Vec<u32>,
}

impl RestSched {
    fn new(plan: &RestPlan, now: Instant) -> Self {
        let due = (0..plan.targets.len())
            .map(|i| {
                if plan.on_connect {
                    Some(now)
                } else {
                    plan.refresh_every
                        .map(|p| now + stagger_offset(i, plan.targets.len(), p))
                }
            })
            .collect();
        Self {
            due,
            last_fetch: None,
            attempts: vec![0; plan.targets.len()],
        }
    }

    /// Next (target index, fire-at instant): the earliest due target,
    /// pushed out to honor `min_spacing` after the previous fetch.
    fn next(&self, min_spacing: Duration) -> Option<(usize, Instant)> {
        let (i, at) = self
            .due
            .iter()
            .enumerate()
            .filter_map(|(i, d)| d.map(|at| (i, at)))
            .min_by_key(|&(_, at)| at)?;
        let at = match self.last_fetch {
            Some(last) => at.max(last + min_spacing),
            None => at,
        };
        Some((i, at))
    }
}

/// Write an operational NOTE record to the raw log (best-effort JSON).
fn note(sink: &mut RotatingRawLog, json: &str) -> std::io::Result<()> {
    sink.append(
        rkind::NOTE,
        clock::mono_ns(),
        clock::wall_ns(),
        json.as_bytes(),
    )
}

/// Outcome of one established-connection session.
enum SessionEnd {
    /// Shutdown was requested; exit `run_venue` cleanly.
    Shutdown,
    /// Connection died (close/error/idle/resync-by-reconnect); reconnect.
    Reconnect,
}

/// Own one venue's capture lifecycle until shutdown: connect, subscribe,
/// read/parse/record frames, fetch REST snapshots, reconnect forever with
/// jittered backoff. Returns `Ok(())` only on requested shutdown; an `Err`
/// means something unrecoverable (e.g. the raw-log directory vanished) and
/// the process should exit so the crash is honestly counted.
pub async fn run_venue<C, F>(
    venue: Venue,
    cfg: VenueConfig,
    mut codec_factory: F,
    mut sink: RotatingRawLog,
    stats: std::sync::Arc<VenueStats>,
    http: reqwest::Client,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    C: VenueCodec,
    F: FnMut() -> C,
{
    let mut attempt: u32 = 0;
    let mut out: Vec<Event> = Vec::with_capacity(4096);
    loop {
        if *shutdown.borrow() {
            break;
        }
        let mut codec = codec_factory();
        let url = codec.ws_url();
        // Bound the handshake (a stalled TCP/TLS connect would otherwise
        // wedge this task forever with the process looking alive) and race
        // it against shutdown so ^C never waits on a dead venue.
        let connected = tokio::select! {
            r = tokio::time::timeout(
                CONNECT_TIMEOUT,
                tokio_tungstenite::connect_async(url.as_str()),
            ) => r,
            _ = shutdown.changed() => continue,
        };
        let ws = match connected {
            Ok(Ok((ws, resp))) => {
                tracing::info!(venue = venue.name(), url = %url, status = %resp.status(), "connected");
                Some(ws)
            }
            Ok(Err(e)) => {
                tracing::warn!(venue = venue.name(), url = %url, error = %e, "connect failed");
                None
            }
            Err(_) => {
                tracing::warn!(venue = venue.name(), url = %url, timeout_s = CONNECT_TIMEOUT.as_secs(), "connect timed out");
                None
            }
        };
        let Some(ws) = ws else {
            // A timed-out handshake is a failed attempt like any other.
            let delay = next_backoff(attempt, cfg.backoff_floor, cfg.backoff_cap);
            attempt = attempt.saturating_add(1);
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = shutdown.changed() => {}
            }
            continue;
        };
        note(
            &mut sink,
            &format!("{{\"event\":\"connect\",\"url\":{url:?}}}"),
        )?;
        let session_start = Instant::now();
        let mut frames_parsed = 0u64;
        match run_session(
            venue,
            &cfg,
            &mut codec,
            ws,
            &mut sink,
            &stats,
            &http,
            &mut shutdown,
            &mut frames_parsed,
            &mut out,
        )
        .await?
        {
            SessionEnd::Shutdown => break,
            SessionEnd::Reconnect => {
                // Reset the backoff counter only once a session has proven
                // healthy; an accept-then-drop venue keeps escalating.
                let healthy = session_healthy(session_start.elapsed(), frames_parsed);
                if healthy {
                    attempt = 0;
                }
                let delay = next_backoff(attempt, cfg.backoff_floor, cfg.backoff_cap);
                if !healthy {
                    attempt = attempt.saturating_add(1);
                }
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    _ = shutdown.changed() => {}
                }
            }
        }
    }
    sink.finish()?;
    tracing::info!(venue = venue.name(), "venue task shut down cleanly");
    Ok(())
}

/// One established-connection session: subscribe, then read frames until
/// the connection dies or shutdown is requested. `Err` is unrecoverable
/// (raw-log IO failure). `frames_parsed` counts successfully parsed frames
/// so the caller can apply the [`session_healthy`] backoff-reset rule.
#[allow(clippy::too_many_arguments)]
async fn run_session<C: VenueCodec>(
    venue: Venue,
    cfg: &VenueConfig,
    codec: &mut C,
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    sink: &mut RotatingRawLog,
    stats: &VenueStats,
    http: &reqwest::Client,
    shutdown: &mut watch::Receiver<bool>,
    frames_parsed: &mut u64,
    out: &mut Vec<Event>,
) -> anyhow::Result<SessionEnd> {
    use std::sync::atomic::Ordering;

    let mut session_parse_errors = 0u64;
    let (mut tx, mut rx) = ws.split();
    for sub in codec.subscribe_messages() {
        if let Err(e) = tx.send(Message::Text(sub.into())).await {
            tracing::warn!(venue = venue.name(), error = %e, "subscribe send failed");
            disconnect_note(sink, stats, "subscribe send failed")?;
            return Ok(SessionEnd::Reconnect);
        }
    }

    let mut rest = RestSched::new(&cfg.rest, Instant::now());
    let mut last_frame = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let rest_next = rest.next(cfg.rest.min_spacing);
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    note(sink, "{\"event\":\"disconnect\",\"reason\":\"shutdown\"}")?;
                    return Ok(SessionEnd::Shutdown);
                }
            }
            () = tokio::time::sleep_until(idle_deadline(last_frame, cfg.idle_timeout)) => {
                tracing::warn!(venue = venue.name(), timeout_s = cfg.idle_timeout.as_secs(), "idle timeout; forcing reconnect");
                disconnect_note(sink, stats, "idle timeout")?;
                return Ok(SessionEnd::Reconnect);
            }
            _ = tick.tick() => {
                sink.sync()?;
                sink.maybe_rotate()?;
            }
            () = async move {
                // Guarded by `if`, so unwrap-by-shape is safe here.
                let (_, at) = rest_next.expect("guarded by rest_next.is_some()");
                tokio::time::sleep_until(at).await;
            }, if rest_next.is_some() => {
                let (idx, _) = rest_next.expect("guarded by rest_next.is_some()");
                fetch_rest(venue, cfg, codec, sink, stats, http, &mut rest, idx, out).await?;
            }
            msg = rx.next() => {
                let reason = match msg {
                    Some(Ok(Message::Text(t))) => {
                        last_frame = Instant::now();
                        let (mono, wall) = (clock::mono_ns(), clock::wall_ns());
                        let payload = t.as_bytes();
                        // Raw capture FIRST: ground truth even if parsing fails.
                        sink.append(rkind::WS_TEXT, mono, wall, payload)?;
                        stats.msgs.fetch_add(1, Ordering::Relaxed);
                        stats.bytes.fetch_add(payload.len() as u64, Ordering::Relaxed);
                        out.clear();
                        let sig = process_payload(
                            codec, payload, mono, wall, stats, &mut session_parse_errors, out,
                        );
                        if sig.is_some() {
                            *frames_parsed += 1;
                        }
                        account_events(stats, out);
                        match sig {
                            Some(Signal::NeedResync { instrument }) => {
                                stats.resyncs.fetch_add(1, Ordering::Relaxed);
                                match cfg.rest.targets.iter().position(|t| t.instrument == instrument) {
                                    Some(idx) => rest.due[idx] = Some(Instant::now()),
                                    None => {
                                        // No REST plan (Kraken): resync by reconnecting,
                                        // which re-delivers a full snapshot on subscribe.
                                        tracing::warn!(venue = venue.name(), instrument, "resync without REST plan; reconnecting");
                                        disconnect_note(sink, stats, "resync")?;
                                        return Ok(SessionEnd::Reconnect);
                                    }
                                }
                            }
                            Some(Signal::Reconnect) => {
                                // Venue advised this connection is going away
                                // (e.g. Binance serverShutdown): reconnect
                                // proactively instead of waiting for the close.
                                tracing::warn!(venue = venue.name(), "venue advised reconnect");
                                stats.reconnects.fetch_add(1, Ordering::Relaxed);
                                note(sink, "{\"event\":\"reconnect\",\"reason\":\"venue-advisory\"}")?;
                                return Ok(SessionEnd::Reconnect);
                            }
                            _ => {}
                        }
                        continue;
                    }
                    Some(Ok(Message::Binary(b))) => {
                        last_frame = Instant::now();
                        sink.append(rkind::WS_BINARY, clock::mono_ns(), clock::wall_ns(), &b)?;
                        stats.msgs.fetch_add(1, Ordering::Relaxed);
                        stats.bytes.fetch_add(b.len() as u64, Ordering::Relaxed);
                        continue;
                    }
                    Some(Ok(Message::Ping(p))) => {
                        last_frame = Instant::now();
                        if let Err(e) = tx.send(Message::Pong(p)).await {
                            tracing::warn!(venue = venue.name(), error = %e, "pong send failed");
                            "pong send failed"
                        } else {
                            continue;
                        }
                    }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {
                        last_frame = Instant::now();
                        continue;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        tracing::warn!(venue = venue.name(), frame = ?frame, "server closed connection");
                        "server close"
                    }
                    Some(Err(e)) => {
                        tracing::warn!(venue = venue.name(), error = %e, "websocket error");
                        "websocket error"
                    }
                    None => {
                        tracing::warn!(venue = venue.name(), "websocket stream ended");
                        "stream end"
                    }
                };
                disconnect_note(sink, stats, reason)?;
                return Ok(SessionEnd::Reconnect);
            }
        }
    }
}

/// Record a disconnect NOTE and count the reconnect.
fn disconnect_note(
    sink: &mut RotatingRawLog,
    stats: &VenueStats,
    reason: &str,
) -> std::io::Result<()> {
    use std::sync::atomic::Ordering;
    stats.reconnects.fetch_add(1, Ordering::Relaxed);
    note(
        sink,
        &format!("{{\"event\":\"disconnect\",\"reason\":{reason:?}}}"),
    )
}

/// Fetch one REST snapshot target: GET, envelope, raw-log append, parse.
/// HTTP failures reschedule the target with backoff and never tear down the
/// WS session; only raw-log IO errors propagate.
#[allow(clippy::too_many_arguments)]
async fn fetch_rest<C: VenueCodec>(
    venue: Venue,
    cfg: &VenueConfig,
    codec: &mut C,
    sink: &mut RotatingRawLog,
    stats: &VenueStats,
    http: &reqwest::Client,
    rest: &mut RestSched,
    idx: usize,
    out: &mut Vec<Event>,
) -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;
    let target = &cfg.rest.targets[idx];
    rest.last_fetch = Some(Instant::now());
    let body = match http.get(&target.url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(venue = venue.name(), url = %target.url, error = %e, "rest body read failed");
                reschedule_rest_failure(rest, idx);
                return Ok(());
            }
        },
        Ok(resp) => {
            let status = resp.status();
            if matches!(status.as_u16(), 429 | 418) {
                // Documented rate-limit / IP-ban responses (Binance):
                // honor Retry-After for the WHOLE venue, otherwise every
                // retry extends the ban.
                let delay = retry_after_delay(
                    resp.headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok()),
                );
                tracing::warn!(
                    venue = venue.name(),
                    url = %target.url,
                    status = %status,
                    retry_after_s = delay.as_secs(),
                    "rest rate limited; pausing all REST targets"
                );
                reschedule_rest_rate_limited(rest, idx, delay);
            } else {
                tracing::warn!(venue = venue.name(), url = %target.url, status = %status, "rest request rejected");
                reschedule_rest_failure(rest, idx);
            }
            return Ok(());
        }
        Err(e) => {
            tracing::warn!(venue = venue.name(), url = %target.url, error = %e, "rest request failed");
            reschedule_rest_failure(rest, idx);
            return Ok(());
        }
    };
    let (mono, wall) = (clock::mono_ns(), clock::wall_ns());
    let env = rest_envelope(target.instrument, &target.venue_symbol, &target.url, &body);
    sink.append(rkind::REST_SNAPSHOT, mono, wall, &env)?;
    stats.rest_snaps.fetch_add(1, Ordering::Relaxed);
    out.clear();
    match codec.parse_rest_snapshot(target.instrument, &body, mono, wall, out) {
        Ok(Signal::NeedResync { .. }) => {
            // Snapshot fetched but unusable for sync (e.g. stale vs. the
            // buffered diff stream): refetch with backoff instead of
            // waiting a whole refresh period while diffs are dropped.
            account_events(stats, out);
            stats.resyncs.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(venue = venue.name(), url = %target.url, "rest snapshot needs resync; refetching with backoff");
            reschedule_rest_failure(rest, idx);
            return Ok(());
        }
        Ok(_) => {
            account_events(stats, out);
            rest.attempts[idx] = 0;
        }
        Err(e) => {
            // A 200-but-unparseable body (including a codec-rejected stale
            // snapshot) must retry with backoff, NOT wait for the periodic
            // refresh: the codec may be Unsynced and dropping every diff.
            stats.parse_errors.fetch_add(1, Ordering::Relaxed);
            let snippet = String::from_utf8_lossy(&body[..body.len().min(200)]);
            tracing::warn!(venue = venue.name(), url = %target.url, error = %e, body = %snippet, "rest snapshot parse failed");
            reschedule_rest_failure(rest, idx);
            return Ok(());
        }
    }
    rest.due[idx] = cfg.rest.refresh_every.map(|p| Instant::now() + p);
    Ok(())
}

/// Reschedule a failed REST fetch with per-target exponential backoff
/// (1 s floor, 60 s cap), leaving the WS session untouched.
fn reschedule_rest_failure(rest: &mut RestSched, idx: usize) {
    let delay = next_backoff(
        rest.attempts[idx],
        Duration::from_secs(1),
        Duration::from_secs(60),
    );
    rest.attempts[idx] = rest.attempts[idx].saturating_add(1);
    rest.due[idx] = Some(Instant::now() + delay);
}

/// Venue-wide REST pushback after an HTTP 429/418: every scheduled
/// target's due time is pushed out to at least `now + delay` (the limit is
/// per-IP, not per-endpoint), the offending target retries exactly then,
/// and a failure attempt is counted against it.
fn reschedule_rest_rate_limited(rest: &mut RestSched, idx: usize, delay: Duration) {
    let due = Instant::now() + delay;
    rest.attempts[idx] = rest.attempts[idx].saturating_add(1);
    rest.due[idx] = Some(due);
    for d in &mut rest.due {
        if let Some(at) = d
            && *at < due
        {
            *at = due;
        }
    }
}
