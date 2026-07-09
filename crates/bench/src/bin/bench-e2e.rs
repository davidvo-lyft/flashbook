//! bench-e2e: end-to-end latency decomposition + cross-network fan-out
//! (BENCHMARKS.md 3e, plus the network half of 3d).
//!
//! Two subcommands:
//!
//! **`bench-e2e net`** — loopback TCP fan-out. A publisher thread binds
//! `127.0.0.1:0`; N subscriber threads connect (`TCP_NODELAY` on both
//! ends). The publisher paces seeded [`EventGen`] events at a sustained
//! target rate on an absolute schedule (event `i` goes out at
//! `start + i/rate`), stamping `recv_mono_ns` with [`clock::mono_ns`]
//! immediately before the first subscriber write. Wire format is the raw
//! 64-byte [`Event`] record (bytemuck layout), length-implicit fixed
//! frames, one `write(2)` per event per subscriber (no batching).
//! Subscribers `read_exact` 64-byte frames, stamp at read completion, and
//! sample `now - recv_mono_ns`. If the target rate cannot be sustained the
//! ACHIEVED rate is reported and `sustained=false` — never the target.
//!
//! LIMITATIONS (also embedded in the result notes): loopback is not a NIC.
//! This measures kernel network-stack + syscall + scheduler-handoff cost on
//! one host — no serialization delay, no propagation, no NIC interrupts.
//! Treat it as a floor for cross-machine fan-out, not a substitute.
//!
//! **`bench-e2e live`** — LIVE venue pipeline decomposition, designed to
//! run alongside the capture soak (default `--symbols BTC` only, so the
//! extra load is trivial; these are distinct WS connections, not the
//! soak's). Per WS text frame:
//!
//! - `t0` = [`clock::mono_ns`] at frame receipt (the moment tungstenite
//!   yields the complete message),
//! - parse via the real venue codec (fast path, `parse_slow` fallback on
//!   `Structure` errors) with `recv_mono_ns = t0` → `t1`,
//! - publish every emitted event to a [`flashbook_bus`] ring → `t2`
//!   (stamped once per frame after publishing its events),
//! - a subscriber thread `try_next()`-polls the ring and stamps `t3` at
//!   dequeue, matching events to their frame via `recv_mono_ns == t0`.
//!
//! Samples (ns): `parse = t1-t0`, `publish = t2-t1`, `deliver = t3-t2`
//! (per event, using the frame's `t2`), `total_added = t3-t0`.
//! "Exchange→subscriber added latency" = `total_added`: it starts at socket
//! read, so it contains zero internet time by construction.
//!
//! VENUE PATH (context, NOT added by flashbook): per frame carrying a venue
//! timestamp, `venue_path = recv_wall - venue_ts`. It includes venue-side
//! batching (Coinbase `level2_batch` ~50 ms, Binance `depth@100ms` cadence)
//! plus WAN transit plus venue↔host wall-clock offset. Every 5 s a WS Ping
//! carrying an 8-byte `mono_ns` payload measures RTT (`rtt = now - payload`
//! on Pong), published per venue so readers can bound venue-internal
//! batching ≈ `venue_path - rtt/2` (approximation: symmetric path, instant
//! pong turnaround).
//!
//! TRANSPORT NOTE: live mode connects with `tokio-tungstenite`
//! (`connect_async`, rustls with native roots) — the exact same
//! WebSocket/TLS stack as the production capture path in
//! `flashbook_feed::conn`. TLS is terminated in-process and `t0` is stamped
//! the instant tungstenite yields a frame, so the receive path matches
//! production capture: no child process, no extra pipe hop.
//!
//! Usage:
//!   bench-e2e net  [--rate N] [--subs N] [--secs N] [--quick]
//!                  [--results-dir DIR] [--overwrite]
//!   bench-e2e live [--secs N] [--symbols BTC] [--quick]
//!                  [--results-dir DIR] [--overwrite]
//!
//! `--quick` defaults: net rate 50k, 10 s; live 60 s. Writes
//! `e2e_net.json` / `e2e_live.json` + `e2e_rtt.json` via
//! [`flashbook_bench::results`]. Exit codes: 0 ok, 2 usage/IO.

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use flashbook_bench::loadgen::EventGen;
use flashbook_bench::{Percentiles, ResultFile, write_result};
use flashbook_bus::Recv;
use flashbook_feed::binance::BinanceCodec;
use flashbook_feed::coinbase::CoinbaseCodec;
use flashbook_feed::kraken::KrakenCodec;
use flashbook_feed::{CodecError, SymbolTable, VenueCodec};
use flashbook_proto::event::EVENT_SIZE;
use flashbook_proto::{Event, Registry, Venue, clock};
use futures_util::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// Bus ring capacity for the live pipeline (same as the fan-out bench).
const RING_CAPACITY: usize = 65_536;
/// Latency-sample cap per net-mode subscriber (stride-sampled).
const NET_SAMPLE_CAP: u64 = 2_000_000;
/// Net-mode events skipped from sampling at the start (connection warmup).
const NET_WARMUP_EVENTS: u64 = 1_000;
/// EventGen seed for net mode (same stream every run).
const NET_SEED: u64 = 0xE2E0_57A6;
/// Achieved/target ratio below which a net run is marked not sustained.
const SUSTAINED_MIN_RATIO: f64 = 0.99;
/// Live mode: WS connect + handshake (and each subscribe send) budget
/// before a venue is declared down. Message size stays bounded by
/// tungstenite's default 64 MiB cap (Coinbase snapshots run to a few MB).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// Live mode: interval between RTT pings.
const PING_INTERVAL: Duration = Duration::from_secs(5);

const USAGE: &str = "usage: bench-e2e <net|live> [options]
  net  [--rate N] [--subs N] [--secs N] [--quick] [--results-dir DIR] [--overwrite]
       quick defaults: rate 50000, secs 10 (full: rate 200000, secs 30); subs default 4
  live [--secs N] [--symbols BTC] [--quick] [--results-dir DIR] [--overwrite]
       quick default: secs 60 (full: 300)";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Loopback fan-out configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NetCfg {
    /// Target sustained publish rate, events/s.
    rate: u64,
    /// Subscriber (TCP consumer) count.
    subs: usize,
    /// Paced duration, seconds.
    secs: u64,
    /// Smoke mode; marks the result non-official.
    quick: bool,
    /// Where `e2e_net.json` is written.
    results_dir: PathBuf,
    /// Allow clobbering an existing result file.
    overwrite: bool,
}

/// Live venue decomposition configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveCfg {
    /// Measurement window, seconds.
    secs: u64,
    /// Canonical base symbol (default `BTC`; keep it BTC during the soak).
    symbols: String,
    /// Smoke mode; marks the result non-official.
    quick: bool,
    /// Where `e2e_live.json` / `e2e_rtt.json` are written.
    results_dir: PathBuf,
    /// Allow clobbering existing result files.
    overwrite: bool,
}

/// Parse outcome: run a subcommand, or print usage and exit 0 (the
/// `bench/run-all.sh` probe runs `--help` and skips bins that fail it).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Cli {
    /// `bench-e2e net ...`
    Net(NetCfg),
    /// `bench-e2e live ...`
    Live(LiveCfg),
    /// `--help`/`-h`.
    Help,
}

/// Parse one numeric flag value.
fn parse_num(flag: &str, v: Option<String>) -> Result<u64, String> {
    v.ok_or_else(|| format!("{flag} needs a value"))?
        .parse()
        .map_err(|_| format!("{flag} needs an unsigned integer"))
}

/// Parse CLI arguments (everything after argv[0]). Pure.
fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Cli, String> {
    let sub = args.next().ok_or(USAGE)?;
    match sub.as_str() {
        "--help" | "-h" => return Ok(Cli::Help),
        "net" | "live" => {}
        other => return Err(format!("unknown subcommand: {other}\n{USAGE}")),
    }
    let (mut rate, mut secs, mut subs) = (None, None, None);
    let mut symbols = "BTC".to_string();
    let mut quick = false;
    let mut results_dir = PathBuf::from("bench/results");
    let mut overwrite = false;
    while let Some(a) = args.next() {
        match (sub.as_str(), a.as_str()) {
            (_, "--quick") => quick = true,
            (_, "--overwrite") => overwrite = true,
            (_, "--results-dir") => {
                results_dir = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or("--results-dir needs a path")?;
            }
            (_, "--secs") => secs = Some(parse_num("--secs", args.next())?),
            ("net", "--rate") => rate = Some(parse_num("--rate", args.next())?),
            ("net", "--subs") => subs = Some(parse_num("--subs", args.next())?),
            ("live", "--symbols") => {
                symbols = args.next().ok_or("--symbols needs a value")?.to_uppercase();
            }
            (_, "--help" | "-h") => return Ok(Cli::Help),
            (_, other) => return Err(format!("unknown arg: {other}\n{USAGE}")),
        }
    }
    if sub == "net" {
        Ok(Cli::Net(NetCfg {
            rate: rate.unwrap_or(if quick { 50_000 } else { 200_000 }),
            subs: usize::try_from(subs.unwrap_or(4)).map_err(|_| "--subs too large")?,
            secs: secs.unwrap_or(if quick { 10 } else { 30 }),
            quick,
            results_dir,
            overwrite,
        }))
    } else {
        Ok(Cli::Live(LiveCfg {
            secs: secs.unwrap_or(if quick { 60 } else { 300 }),
            symbols,
            quick,
            results_dir,
            overwrite,
        }))
    }
}

// ---------------------------------------------------------------------------
// Shared pure helpers
// ---------------------------------------------------------------------------

/// Events per second from a count and elapsed nanoseconds.
fn rate_per_sec(count: u64, elapsed_ns: u64) -> f64 {
    if elapsed_ns == 0 {
        return 0.0;
    }
    count as f64 * 1e9 / elapsed_ns as f64
}

/// Deterministic sampling stride so `total` events yield at most `cap`
/// samples: every `stride`-th delivered event is kept.
fn sample_stride(total: u64, cap: u64) -> u64 {
    if cap == 0 {
        return total.max(1);
    }
    total.div_ceil(cap).max(1)
}

/// Absolute pacing schedule: nanoseconds after start at which event `i`
/// must be sent to hold `rate` events/s.
fn send_deadline_ns(i: u64, rate: u64) -> u64 {
    debug_assert!(rate > 0);
    u64::try_from(u128::from(i) * 1_000_000_000u128 / u128::from(rate)).unwrap_or(u64::MAX)
}

/// Sustained-rate verdict: achieved within [`SUSTAINED_MIN_RATIO`] of target.
fn is_sustained(target: f64, achieved: f64) -> bool {
    target > 0.0 && achieved >= target * SUSTAINED_MIN_RATIO
}

/// Percentiles of `samples` as a JSON value (`null` when empty).
fn pctl_json(samples: &[u64]) -> serde_json::Value {
    match Percentiles::from_samples(samples) {
        Some(p) => serde_json::to_value(p).expect("percentiles serialize"),
        None => serde_json::Value::Null,
    }
}

// ---------------------------------------------------------------------------
// net: loopback TCP fan-out
// ---------------------------------------------------------------------------

/// One net-mode subscriber's outcome.
struct NetSub {
    /// Events read before EOF.
    delivered: u64,
    /// Stride-sampled `read-completion - recv_mono_ns` latencies, ns.
    samples: Vec<u64>,
}

/// Subscriber loop: read exact 64-byte frames until EOF, stamping at read
/// completion; every `stride`-th event after warmup is sampled.
fn net_subscriber(mut sock: TcpStream, stride: u64) -> NetSub {
    let mut buf = [0u8; EVENT_SIZE];
    let mut delivered = 0u64;
    let mut samples = Vec::new();
    while sock.read_exact(&mut buf).is_ok() {
        let now = clock::mono_ns();
        delivered += 1;
        let idx = delivered - 1;
        if idx >= NET_WARMUP_EVENTS && (idx - NET_WARMUP_EVENTS).is_multiple_of(stride) {
            let ev = Event::iter_unaligned(&buf).next().expect("one event");
            samples.push(now.saturating_sub(ev.recv_mono_ns));
        }
    }
    NetSub { delivered, samples }
}

/// Run the loopback fan-out benchmark and write `e2e_net.json`.
fn run_net(cfg: &NetCfg) -> anyhow::Result<String> {
    anyhow::ensure!(
        cfg.rate > 0 && cfg.secs > 0 && cfg.subs > 0,
        "rate/secs/subs must be > 0"
    );
    let total = cfg.rate * cfg.secs;
    let stride = sample_stride(total.saturating_sub(NET_WARMUP_EVENTS), NET_SAMPLE_CAP);
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    eprintln!(
        "net: {} events at {}/s to {} subscriber(s) on {addr} (stride {stride})",
        total, cfg.rate, cfg.subs
    );

    // Subscribers connect; the publisher accepts and enables TCP_NODELAY on
    // both ends of every connection.
    let handles: Vec<_> = (0..cfg.subs)
        .map(|_| {
            std::thread::spawn(move || -> anyhow::Result<NetSub> {
                let sock = TcpStream::connect(addr)?;
                sock.set_nodelay(true)?;
                Ok(net_subscriber(sock, stride))
            })
        })
        .collect();
    let mut streams = Vec::with_capacity(cfg.subs);
    for _ in 0..cfg.subs {
        let (s, _) = listener.accept()?;
        s.set_nodelay(true)?;
        streams.push(s);
    }

    // Paced publish on an absolute schedule; one stamp per event, shared by
    // all subscriber writes (later subscribers see the fan-out cost).
    let mut evgen = EventGen::new(NET_SEED);
    let start = clock::mono_ns();
    for i in 0..total {
        let deadline = start + send_deadline_ns(i, cfg.rate);
        loop {
            let now = clock::mono_ns();
            if now >= deadline {
                break;
            }
            let ahead = deadline - now;
            if ahead > 300_000 {
                std::thread::sleep(Duration::from_nanos(ahead - 200_000));
            } else {
                std::hint::spin_loop();
            }
        }
        let mut ev = evgen.next_event();
        ev.recv_mono_ns = clock::mono_ns();
        let bytes = Event::slice_as_bytes(std::slice::from_ref(&ev));
        for s in &mut streams {
            s.write_all(bytes)?;
        }
    }
    let elapsed_ns = clock::mono_ns() - start;
    drop(streams); // EOF to subscribers
    drop(listener);

    let subs: Vec<NetSub> = handles
        .into_iter()
        .map(|h| h.join().expect("subscriber thread panicked"))
        .collect::<anyhow::Result<_>>()?;

    let achieved = rate_per_sec(total, elapsed_ns);
    let sustained = is_sustained(cfg.rate as f64, achieved);
    let sub_json: Vec<serde_json::Value> = subs
        .iter()
        .map(|s| {
            serde_json::json!({
                "delivered": s.delivered,
                "latency_ns": pctl_json(&s.samples),
            })
        })
        .collect();
    let merged: Vec<u64> = subs
        .iter()
        .flat_map(|s| s.samples.iter().copied())
        .collect();

    let notes = format!(
        "LIMITATIONS: loopback TCP is NOT a NIC. This measures kernel network-stack + syscall + \
         scheduler-handoff cost on one host: no wire serialization, no propagation, no NIC \
         interrupt/coalescing behavior. Treat as a floor for cross-machine fan-out latency. \
         Method: publisher paces on an absolute schedule (event i at start+i/rate), stamps \
         recv_mono_ns once immediately before the first subscriber write, then write(2)s the raw \
         64B Event (length-implicit framing, no batching) to each subscriber in turn — later \
         subscribers include fan-out serialization. Subscribers stamp after read_exact(64) \
         completes. All stamps share one process-monotonic clock; threads unpinned (macOS). \
         First {NET_WARMUP_EVENTS} events per subscriber excluded as warmup; stride sampling \
         (stride {stride}, cap {NET_SAMPLE_CAP}/sub). If the schedule slips, the ACHIEVED rate \
         is reported and sustained=false.{}",
        if cfg.quick {
            " QUICK smoke run: not official numbers."
        } else {
            ""
        }
    );
    let result = ResultFile::new(
        "e2e_net",
        serde_json::json!({
            "target_rate_per_sec": cfg.rate,
            "subs": cfg.subs,
            "secs": cfg.secs,
            "events": total,
            "quick": cfg.quick,
            "seed": NET_SEED,
        }),
        serde_json::json!({
            "achieved_rate_per_sec": achieved,
            "sustained": sustained,
            "elapsed_ns": elapsed_ns,
            "per_sub": sub_json,
            "merged_latency_ns": pctl_json(&merged),
        }),
        &notes,
    );
    let path = write_result(&cfg.results_dir, &result, cfg.overwrite)?;
    let p = Percentiles::from_samples(&merged);
    let summary = format!(
        "net: target={}/s achieved={achieved:.0}/s sustained={sustained} subs={} \
         p50={} p99={} p999={} max={} (ns, merged n={})",
        cfg.rate,
        cfg.subs,
        p.map_or(0, |p| p.p50),
        p.map_or(0, |p| p.p99),
        p.map_or(0, |p| p.p999),
        p.map_or(0, |p| p.max),
        p.map_or(0, |p| p.n),
    );
    println!("{summary}");
    println!("wrote {}", path.display());
    Ok(summary)
}

// ---------------------------------------------------------------------------
// live transport: tokio-tungstenite (rustls, native roots)
// ---------------------------------------------------------------------------

/// The live-mode connection type — identical to the production capture
/// path's connection in `flashbook_feed::conn` (tokio-tungstenite over
/// rustls with native roots).
type LiveWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Connect one venue WS and send its subscribe messages, every await
/// bounded by [`CONNECT_TIMEOUT`]. Errors come back as human-readable note
/// strings for the venue outcome.
async fn ws_connect(url: &str, subscribe: Vec<String>) -> Result<LiveWs, String> {
    let (mut ws, _resp) = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(url))
        .await
        .map_err(|_| format!("timed out after {}s", CONNECT_TIMEOUT.as_secs()))?
        .map_err(|e| e.to_string())?;
    for m in subscribe {
        tokio::time::timeout(CONNECT_TIMEOUT, ws.send(Message::Text(m.into())))
            .await
            .map_err(|_| "subscribe send timed out".to_string())?
            .map_err(|e| format!("subscribe send: {e}"))?;
    }
    Ok(ws)
}

/// What the live loop does with one received message.
#[derive(Debug, PartialEq, Eq)]
enum LiveMsg<'a> {
    /// A data frame to parse (text or binary payload).
    Data(&'a [u8]),
    /// A pong: candidate RTT echo for [`rtt_from_pong`].
    Pong(&'a [u8]),
    /// A server ping to answer with a payload-echoing pong (tungstenite
    /// also queues one itself; the explicit reply matches production).
    Ping(&'a [u8]),
    /// Close frame: the session is over.
    Close,
    /// Raw frames never surface from a polled stream; ignore.
    Ignore,
}

/// Classify one tungstenite message for the live loop. Pure; payloads are
/// borrowed so the hot data path stays copy-free.
fn classify(msg: &Message) -> LiveMsg<'_> {
    match msg {
        Message::Text(t) => LiveMsg::Data(t.as_bytes()),
        Message::Binary(b) => LiveMsg::Data(b.as_ref()),
        Message::Pong(p) => LiveMsg::Pong(p.as_ref()),
        Message::Ping(p) => LiveMsg::Ping(p.as_ref()),
        Message::Close(_) => LiveMsg::Close,
        Message::Frame(_) => LiveMsg::Ignore,
    }
}

/// Degradation note for a session that ended at `now` (mono ns), or `None`
/// when the run deadline had already been reached (a clean end).
fn early_close_note(name: &str, now: u64, start: u64, deadline: u64, why: &str) -> Option<String> {
    (now < deadline).then(|| {
        format!(
            "{name} closed early ({}s in): {why}",
            now.saturating_sub(start) / 1_000_000_000
        )
    })
}

/// RTT ping payload: the current [`clock::mono_ns`], little-endian.
fn ping_payload(mono_ns: u64) -> [u8; 8] {
    mono_ns.to_le_bytes()
}

/// RTT from a pong echo received at `now`: `Some(now - sent)` when
/// `payload` is one of our 8-byte little-endian mono-ns stamps (saturating
/// at 0), `None` otherwise (venue-originated pongs are ignored).
fn rtt_from_pong(payload: &[u8], now: u64) -> Option<u64> {
    let sent: [u8; 8] = payload.try_into().ok()?;
    Some(now.saturating_sub(u64::from_le_bytes(sent)))
}

/// Venue-path sample from receive wall time vs venue timestamp:
/// `(sample_ns, clamped)`, clamped to 0 when the venue clock reads ahead of
/// the local wall clock.
fn venue_path_sample(recv_wall: u64, venue_ts: u64) -> (u64, bool) {
    if recv_wall >= venue_ts {
        (recv_wall - venue_ts, false)
    } else {
        (0, true)
    }
}

// ---------------------------------------------------------------------------
// live: venue pipeline decomposition
// ---------------------------------------------------------------------------

/// Everything measured for one venue in live mode.
struct VenueOutcome {
    /// Stable venue name.
    name: &'static str,
    /// Whether the WS connection + handshake succeeded.
    connected: bool,
    /// Degradation / early-exit note ("" when clean).
    note: String,
    /// Text frames successfully parsed.
    frames: u64,
    /// Events emitted (== events published to the ring).
    events: u64,
    /// Frames that used the serde_json fallback.
    fallbacks: u64,
    /// Frames neither parse path accepted.
    parse_errors: u64,
    /// Resync/reconnect signals from the codec (no REST resync is wired
    /// here, so Binance depth stays unsynced by design — see notes).
    resync_signals: u64,
    /// Events lost by the ring subscriber (overwrite-oldest lag).
    lagged_lost: u64,
    /// Events whose frame `t2` stamp could not be matched (deliver sample
    /// dropped; `total_added` still recorded).
    unmatched: u64,
    /// `t1-t0` per parsed frame, ns.
    parse: Vec<u64>,
    /// `t2-t1` per event-emitting frame, ns.
    publish: Vec<u64>,
    /// `t3-t2` per delivered event, ns.
    deliver: Vec<u64>,
    /// `t3-t0` per delivered event, ns.
    total_added: Vec<u64>,
    /// `deliver` restricted to non-snapshot events (steady state).
    deliver_steady: Vec<u64>,
    /// `total_added` restricted to non-snapshot events (steady state).
    total_steady: Vec<u64>,
    /// `recv_wall - venue_ts` per venue-stamped frame, ns (clamped at 0).
    venue_path: Vec<u64>,
    /// venue_path samples clamped to 0 (venue clock ahead of local wall).
    venue_path_clamped: u64,
    /// Ping RTTs, ns.
    rtt: Vec<u64>,
    /// Pings sent.
    pings_sent: u64,
}

impl VenueOutcome {
    /// Empty outcome for `venue`.
    fn new(venue: Venue) -> Self {
        VenueOutcome {
            name: venue.name(),
            connected: false,
            note: String::new(),
            frames: 0,
            events: 0,
            fallbacks: 0,
            parse_errors: 0,
            resync_signals: 0,
            lagged_lost: 0,
            unmatched: 0,
            parse: Vec::new(),
            publish: Vec::new(),
            deliver: Vec::new(),
            total_added: Vec::new(),
            deliver_steady: Vec::new(),
            total_steady: Vec::new(),
            venue_path: Vec::new(),
            venue_path_clamped: 0,
            rtt: Vec::new(),
            pings_sent: 0,
        }
    }
}

/// The BTC-class instrument for `venue` from a registry:
/// `(venue_symbol, instrument_id)` for canonical `<base>-USD`.
fn venue_instrument(reg: &Registry, venue: Venue, base: &str) -> Option<(String, u32)> {
    let canonical = format!("{base}-USD");
    reg.for_venue(venue)
        .find(|m| m.canonical == canonical)
        .map(|m| (m.venue_symbol.clone(), m.id))
}

/// Construct the real production codec for `venue` over `table`.
fn make_codec(venue: Venue, table: SymbolTable) -> Box<dyn VenueCodec> {
    match venue {
        Venue::Coinbase => Box::new(CoinbaseCodec::new(table)),
        Venue::Binance => Box::new(BinanceCodec::new(table)),
        Venue::Kraken => Box::new(KrakenCodec::new(table)),
    }
}

/// Fast-path parse with the production fallback policy (retry `Structure`
/// errors via `parse_slow`). Returns whether the frame parsed; failed
/// frames leave `out` unchanged.
fn parse_with_fallback(
    codec: &mut dyn VenueCodec,
    payload: &[u8],
    t0_mono: u64,
    t0_wall: u64,
    out: &mut Vec<Event>,
    o: &mut VenueOutcome,
) -> bool {
    use flashbook_feed::Signal;
    let base = out.len();
    let sig = match codec.parse(payload, t0_mono, t0_wall, out) {
        Ok(sig) => Some(sig),
        Err(CodecError::Structure(_)) => {
            out.truncate(base);
            match codec.parse_slow(payload, t0_mono, t0_wall, out) {
                Ok(sig) => {
                    o.fallbacks += 1;
                    Some(sig)
                }
                Err(_) => None,
            }
        }
        Err(_) => None,
    };
    match sig {
        Some(Signal::NeedResync { .. } | Signal::Reconnect) => {
            o.resync_signals += 1;
            true
        }
        Some(_) => true,
        None => {
            out.truncate(base);
            o.parse_errors += 1;
            false
        }
    }
}

/// Ring-subscriber results, returned from the poll thread.
struct SubStats {
    /// `t3-t2` samples, ns.
    deliver: Vec<u64>,
    /// `t3-t0` samples, ns.
    total_added: Vec<u64>,
    /// `deliver` restricted to events without `FROM_SNAPSHOT` — the initial
    /// full-book snapshot is one huge frame whose sequential drain would
    /// otherwise dominate event-weighted percentiles on short windows.
    deliver_steady: Vec<u64>,
    /// `total_added` restricted to non-snapshot events.
    total_steady: Vec<u64>,
    /// Events lost to ring overwrite.
    lagged_lost: u64,
    /// Events with no matching frame stamp within the grace window.
    unmatched: u64,
}

/// Ring subscriber poll loop: stamps `t3` at dequeue, matches events to
/// frame stamps `(t0, t2)` arriving on `stamps`, and yields every 256 empty
/// polls (soak politeness — the wakeup cost is inside `deliver`).
fn ring_subscriber(
    mut cons: flashbook_bus::Consumer,
    stamps: &crossbeam_channel::Receiver<(u64, u64)>,
    stop: &AtomicBool,
) -> SubStats {
    let mut s = SubStats {
        deliver: Vec::new(),
        total_added: Vec::new(),
        deliver_steady: Vec::new(),
        total_steady: Vec::new(),
        lagged_lost: 0,
        unmatched: 0,
    };
    let mut t2_by_t0: HashMap<u64, u64> = HashMap::new();
    let mut empties = 0u32;
    loop {
        while let Ok((t0, t2)) = stamps.try_recv() {
            t2_by_t0.insert(t0, t2);
        }
        match cons.try_next() {
            Recv::Event(ev) => {
                let t3 = clock::mono_ns();
                let steady = ev.flags & flashbook_proto::event::flags::FROM_SNAPSHOT == 0;
                let total = t3.saturating_sub(ev.recv_mono_ns);
                s.total_added.push(total);
                if steady {
                    s.total_steady.push(total);
                }
                // The (t0, t2) stamp is sent AFTER the events it covers are
                // published, so a fast dequeue can beat it: grace-wait 1 ms.
                let mut t2 = t2_by_t0.get(&ev.recv_mono_ns).copied();
                let grace_until = t3 + 1_000_000;
                while t2.is_none() && clock::mono_ns() < grace_until {
                    while let Ok((a, b)) = stamps.try_recv() {
                        t2_by_t0.insert(a, b);
                    }
                    t2 = t2_by_t0.get(&ev.recv_mono_ns).copied();
                    std::hint::spin_loop();
                }
                match t2 {
                    Some(t2) => {
                        let d = t3.saturating_sub(t2);
                        s.deliver.push(d);
                        if steady {
                            s.deliver_steady.push(d);
                        }
                    }
                    None => s.unmatched += 1,
                }
                empties = 0;
            }
            Recv::Empty => {
                if stop.load(Ordering::Acquire) {
                    break; // producer is done and the ring is drained
                }
                empties += 1;
                if empties.is_multiple_of(256) {
                    std::thread::yield_now();
                } else {
                    std::hint::spin_loop();
                }
            }
            Recv::Lagged { lost } => s.lagged_lost += lost,
        }
    }
    s
}

/// Measure one venue for `secs` seconds over a [`LiveWs`] driven on a
/// current-thread tokio runtime. Never hangs: connect/subscribe awaits are
/// bounded by [`CONNECT_TIMEOUT`] and the read loop races the run deadline
/// and the ping interval in a `select!`. All failures degrade to a
/// disconnected (or partial) outcome with a note.
fn run_live_venue(venue: Venue, base: &str, secs: u64) -> VenueOutcome {
    let mut o = VenueOutcome::new(venue);
    let reg = Registry::builtin();
    let Some((sym, id)) = venue_instrument(&reg, venue, base) else {
        o.note = format!(
            "no {base}-USD instrument for {} in builtin registry",
            o.name
        );
        return o;
    };
    let mut codec = make_codec(venue, SymbolTable::new([(sym, id)]));
    let url = codec.ws_url();
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            o.note = format!("{} runtime build failed: {e}", o.name);
            eprintln!("live: {}", o.note);
            return o;
        }
    };
    let mut ws = match rt.block_on(ws_connect(&url, codec.subscribe_messages())) {
        Ok(ws) => ws,
        Err(e) => {
            o.note = format!("{} connect failed: {e}", o.name);
            eprintln!("live: {}", o.note);
            return o;
        }
    };
    o.connected = true;
    eprintln!("live: {} connected ({url})", o.name);

    let mut producer = flashbook_bus::ring(RING_CAPACITY);
    let consumer = producer.subscribe();
    let stop = Arc::new(AtomicBool::new(false));
    let (stamp_tx, stamp_rx) = crossbeam_channel::unbounded::<(u64, u64)>();
    let sub = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || ring_subscriber(consumer, &stamp_rx, &stop))
    };

    let start = clock::mono_ns();
    let run_deadline = start + secs.saturating_mul(1_000_000_000);
    rt.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        let mut ping =
            tokio::time::interval_at(tokio::time::Instant::now() + PING_INTERVAL, PING_INTERVAL);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut events_buf: Vec<Event> = Vec::with_capacity(4096);
        loop {
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => break,
                _ = ping.tick() => {
                    let sent = clock::mono_ns();
                    let frame = Message::Ping(ping_payload(sent).to_vec().into());
                    match tokio::time::timeout(PING_INTERVAL, ws.send(frame)).await {
                        Ok(Ok(())) => o.pings_sent += 1,
                        Ok(Err(e)) => {
                            if let Some(n) = early_close_note(
                                o.name, clock::mono_ns(), start, run_deadline,
                                &format!("ping send: {e}"),
                            ) {
                                o.note = n;
                            }
                            break;
                        }
                        Err(_) => {
                            if let Some(n) = early_close_note(
                                o.name, clock::mono_ns(), start, run_deadline,
                                "ping send timed out",
                            ) {
                                o.note = n;
                            }
                            break;
                        }
                    }
                }
                msg = ws.next() => {
                    // t0: stamped the moment tungstenite yields the frame.
                    let (t0, wall) = (clock::mono_ns(), clock::wall_ns());
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            if let Some(n) = early_close_note(
                                o.name, t0, start, run_deadline, &e.to_string(),
                            ) {
                                o.note = n;
                            }
                            break;
                        }
                        None => {
                            if let Some(n) = early_close_note(
                                o.name, t0, start, run_deadline, "stream ended",
                            ) {
                                o.note = n;
                            }
                            break;
                        }
                    };
                    match classify(&msg) {
                        LiveMsg::Data(payload) => {
                            events_buf.clear();
                            let parsed = parse_with_fallback(
                                codec.as_mut(), payload, t0, wall, &mut events_buf, &mut o,
                            );
                            let t1 = clock::mono_ns();
                            if parsed {
                                o.frames += 1;
                                o.parse.push(t1 - t0);
                            }
                            if !events_buf.is_empty() {
                                for ev in &events_buf {
                                    producer.publish(ev);
                                }
                                let t2 = clock::mono_ns();
                                o.publish.push(t2 - t1);
                                let _ = stamp_tx.send((t0, t2));
                                o.events += events_buf.len() as u64;
                                if let Some(vts) =
                                    events_buf.iter().map(|e| e.venue_ts_ns).find(|&v| v > 0)
                                {
                                    let (sample, clamped) = venue_path_sample(wall, vts);
                                    o.venue_path.push(sample);
                                    o.venue_path_clamped += u64::from(clamped);
                                }
                            }
                        }
                        LiveMsg::Pong(p) => {
                            if let Some(rtt) = rtt_from_pong(p, t0) {
                                o.rtt.push(rtt);
                            }
                        }
                        LiveMsg::Ping(p) => {
                            let pong = Message::Pong(p.to_vec().into());
                            let _ = tokio::time::timeout(PING_INTERVAL, ws.send(pong)).await;
                        }
                        LiveMsg::Close => {
                            if let Some(n) = early_close_note(
                                o.name, t0, start, run_deadline, "server close",
                            ) {
                                o.note = n;
                            }
                            break;
                        }
                        LiveMsg::Ignore => {}
                    }
                }
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), ws.close(None)).await;
    });
    drop(stamp_tx);
    stop.store(true, Ordering::Release);
    let stats = sub.join().expect("subscriber thread panicked");
    o.deliver = stats.deliver;
    o.total_added = stats.total_added;
    o.deliver_steady = stats.deliver_steady;
    o.total_steady = stats.total_steady;
    o.lagged_lost = stats.lagged_lost;
    o.unmatched = stats.unmatched;
    o
}

/// JSON block for one venue's live outcome.
fn venue_json(o: &VenueOutcome) -> serde_json::Value {
    serde_json::json!({
        "connected": o.connected,
        "note": o.note,
        "frames": o.frames,
        "events": o.events,
        "fallbacks": o.fallbacks,
        "parse_errors": o.parse_errors,
        "resync_signals": o.resync_signals,
        "lagged_lost": o.lagged_lost,
        "unmatched_deliver": o.unmatched,
        "stages": {
            "parse_ns": pctl_json(&o.parse),
            "publish_ns": pctl_json(&o.publish),
            "deliver_ns": pctl_json(&o.deliver),
            "total_added_ns": pctl_json(&o.total_added),
            "deliver_steady_ns": pctl_json(&o.deliver_steady),
            "total_added_steady_ns": pctl_json(&o.total_steady),
        },
        "venue_path_ns": pctl_json(&o.venue_path),
        "venue_path_clamped": o.venue_path_clamped,
    })
}

/// Run live mode across all venues; write `e2e_live.json` + `e2e_rtt.json`.
fn run_live(cfg: &LiveCfg) -> anyhow::Result<String> {
    anyhow::ensure!(cfg.secs > 0, "--secs must be > 0");
    eprintln!(
        "live: {}s window, {}-USD on {} venue(s); running alongside the soak",
        cfg.secs,
        cfg.symbols,
        Venue::ALL.len()
    );
    let handles: Vec<_> = Venue::ALL
        .into_iter()
        .map(|v| {
            let base = cfg.symbols.clone();
            let secs = cfg.secs;
            std::thread::spawn(move || run_live_venue(v, &base, secs))
        })
        .collect();
    let outcomes: Vec<VenueOutcome> = handles
        .into_iter()
        .map(|h| h.join().expect("venue thread panicked"))
        .collect();

    let mut agg_parse = Vec::new();
    let mut agg_publish = Vec::new();
    let mut agg_deliver = Vec::new();
    let mut agg_total = Vec::new();
    let mut agg_deliver_steady = Vec::new();
    let mut agg_total_steady = Vec::new();
    let mut venues = serde_json::Map::new();
    let mut rtt_venues = serde_json::Map::new();
    for o in &outcomes {
        agg_parse.extend_from_slice(&o.parse);
        agg_publish.extend_from_slice(&o.publish);
        agg_deliver.extend_from_slice(&o.deliver);
        agg_total.extend_from_slice(&o.total_added);
        agg_deliver_steady.extend_from_slice(&o.deliver_steady);
        agg_total_steady.extend_from_slice(&o.total_steady);
        venues.insert(o.name.to_string(), venue_json(o));
        rtt_venues.insert(
            o.name.to_string(),
            serde_json::json!({
                "connected": o.connected,
                "note": o.note,
                "pings_sent": o.pings_sent,
                "pongs_matched": o.rtt.len(),
                "rtt_ns": pctl_json(&o.rtt),
            }),
        );
    }
    let connected = outcomes.iter().filter(|o| o.connected).count();
    anyhow::ensure!(connected > 0, "no venue connected; nothing measured");

    let config = serde_json::json!({
        "secs": cfg.secs,
        "symbols": cfg.symbols,
        "quick": cfg.quick,
        "venues": Venue::ALL.iter().map(|v| v.name()).collect::<Vec<_>>(),
        "ring_capacity": RING_CAPACITY,
    });
    let quick_note = if cfg.quick {
        " QUICK smoke run: not official numbers."
    } else {
        ""
    };
    let live_notes = format!(
        "Decomposition of the LOCAL pipeline only, measured on live venue traffic ({}-USD, one \
         extra WS connection per venue, run alongside the capture soak). t0 = mono_ns when \
         tungstenite yields a complete WS text frame; parse = t1-t0 (production codec fast path incl. \
         serde_json fallback); publish = t2-t1 (bus ring publish of the frame's events, t2 \
         stamped once per frame after all its publishes); deliver = t3-t2 per event (subscriber \
         thread dequeue, matched to its frame via recv_mono_ns == t0); total_added = t3-t0. \
         'Exchange->subscriber added latency' = total_added: it starts at socket read and \
         contains zero internet time by construction. VENUE PATH is context, NOT added by \
         flashbook: venue_path = recv_wall - venue_ts per venue-stamped frame; it includes \
         venue-side batching (Coinbase level2_batch ~50 ms, Binance depth@100ms cadence) + WAN \
         transit + venue<->host wall-clock offset; bound venue-internal batching ~= venue_path - \
         rtt/2 using e2e_rtt.json (approximation: symmetric path). TRANSPORT: the same \
         WebSocket/TLS stack as the production capture path — tokio-tungstenite (connect_async) \
         with rustls (native roots), TLS terminated in-process; t0 sits after TLS decrypt + WS \
         frame assembly, matching production's receive stamp (no child process, no pipe hop). \
         LIMITATIONS: (1) The ring subscriber yields every 256 empty polls (soak \
         politeness); its wakeup cost is inside deliver. (2) No REST resync is wired, so Binance \
         depth events stay unsynced and are dropped by the codec; Binance samples are dominated \
         by trade frames (resync_signals counts the codec asking). (3) venue_path samples where \
         the venue clock is ahead of local wall are clamped to 0 and counted \
         (venue_path_clamped); a clamp count near n means the local-vs-venue wall-clock offset \
         exceeds the one-way path and venue_path is uninterpretable without an offset \
         correction — the RTT file is the trustworthy WAN bound in that case. (4) deliver \
         saturates at 0 for events dequeued before their frame's t2 stamp was taken (t2 is \
         per-frame, after ALL its publishes). (5) The initial full-book snapshot arrives as one \
         enormous frame; its sequential per-event drain dominates event-weighted deliver/ \
         total_added percentiles on short windows, so *_steady_ns (events without the \
         FROM_SNAPSHOT flag) is published alongside and is the steady-state number.{quick_note}",
        cfg.symbols
    );
    let live = ResultFile::new(
        "e2e_live",
        config.clone(),
        serde_json::json!({
            "venues": venues,
            "aggregate": {
                "parse_ns": pctl_json(&agg_parse),
                "publish_ns": pctl_json(&agg_publish),
                "deliver_ns": pctl_json(&agg_deliver),
                "total_added_ns": pctl_json(&agg_total),
                "deliver_steady_ns": pctl_json(&agg_deliver_steady),
                "total_added_steady_ns": pctl_json(&agg_total_steady),
            },
            "venues_connected": connected,
        }),
        &live_notes,
    );
    let rtt_notes = format!(
        "RTT method: every 5 s a WS Ping with an 8-byte little-endian mono_ns payload is sent; \
         on the Pong echo, rtt = mono_ns - payload. Subtraction method for readers: \
         venue-internal batching ~= venue_path (e2e_live.json) - rtt/2, an approximation that \
         assumes a symmetric WAN path and instant pong turnaround. Pings and pongs travel the \
         same in-process tungstenite+rustls transport as the data frames (the production \
         capture stack; no child process or pipe hops). n is small by design (one ping per \
         5 s); high percentiles saturate at the max accordingly.{quick_note}"
    );
    let rtt = ResultFile::new(
        "e2e_rtt",
        config,
        serde_json::json!({ "venues": rtt_venues }),
        &rtt_notes,
    );
    let live_path = write_result(&cfg.results_dir, &live, cfg.overwrite)?;
    let rtt_path = write_result(&cfg.results_dir, &rtt, cfg.overwrite)?;

    let mut lines = Vec::new();
    for o in &outcomes {
        let stage = |v: &[u64]| Percentiles::from_samples(v).map_or(0, |p| p.p50);
        lines.push(format!(
            "live[{}]: connected={} frames={} events={} parse_p50={}ns publish_p50={}ns \
             deliver_p50={}ns total_p50={}ns steady_total_p50={}ns venue_path_p50={:.1}ms \
             rtt_p50={:.1}ms{}",
            o.name,
            o.connected,
            o.frames,
            o.events,
            stage(&o.parse),
            stage(&o.publish),
            stage(&o.deliver),
            stage(&o.total_added),
            stage(&o.total_steady),
            stage(&o.venue_path) as f64 / 1e6,
            stage(&o.rtt) as f64 / 1e6,
            if o.note.is_empty() {
                String::new()
            } else {
                format!(" note={}", o.note)
            },
        ));
    }
    let agg = Percentiles::from_samples(&agg_total);
    let agg_s = Percentiles::from_samples(&agg_total_steady);
    lines.push(format!(
        "live[aggregate]: total_added p50={}ns p99={}ns p999={}ns max={}ns (n={}); \
         steady (non-snapshot) p50={}ns p99={}ns (n={})",
        agg.map_or(0, |p| p.p50),
        agg.map_or(0, |p| p.p99),
        agg.map_or(0, |p| p.p999),
        agg.map_or(0, |p| p.max),
        agg.map_or(0, |p| p.n),
        agg_s.map_or(0, |p| p.p50),
        agg_s.map_or(0, |p| p.p99),
        agg_s.map_or(0, |p| p.n),
    ));
    let summary = lines.join("\n");
    println!("{summary}");
    println!("wrote {} and {}", live_path.display(), rtt_path.display());
    Ok(summary)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> ExitCode {
    clock::init();
    let cli = match parse_args(std::env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let run = match cli {
        Cli::Help => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Cli::Net(cfg) => run_net(&cfg),
        Cli::Live(cfg) => run_live(&cfg),
    };
    match run {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bench-e2e: {e:#}");
            ExitCode::from(2)
        }
    }
}

// ---------------------------------------------------------------------------
// tests (pure helpers only — no sockets, no children, no network)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_net_quick_defaults() {
        let cli = parse_args(["net", "--quick"].map(String::from).into_iter()).unwrap();
        let Cli::Net(c) = cli else {
            panic!("expected net")
        };
        assert_eq!((c.rate, c.subs, c.secs, c.quick), (50_000, 4, 10, true));
        assert_eq!(c.results_dir, PathBuf::from("bench/results"));
        assert!(!c.overwrite);
    }

    #[test]
    fn parse_args_net_full_defaults_and_overrides() {
        let Cli::Net(c) = parse_args(["net"].map(String::from).into_iter()).unwrap() else {
            panic!("expected net")
        };
        assert_eq!((c.rate, c.subs, c.secs), (200_000, 4, 30));

        let args = [
            "net", "--rate", "1000", "--subs", "2", "--secs", "5", "--quick",
        ];
        let Cli::Net(c) = parse_args(args.map(String::from).into_iter()).unwrap() else {
            panic!("expected net")
        };
        // explicit values beat --quick defaults
        assert_eq!((c.rate, c.subs, c.secs, c.quick), (1000, 2, 5, true));
    }

    #[test]
    fn parse_args_live_defaults_help_and_errors() {
        let Cli::Live(c) = parse_args(["live", "--quick"].map(String::from).into_iter()).unwrap()
        else {
            panic!("expected live")
        };
        assert_eq!((c.secs, c.quick), (60, true));
        assert_eq!(c.symbols, "BTC");

        let args = ["live", "--secs", "300", "--symbols", "eth"];
        let Cli::Live(c) = parse_args(args.map(String::from).into_iter()).unwrap() else {
            panic!("expected live")
        };
        assert_eq!((c.secs, c.symbols.as_str()), (300, "ETH"));

        assert_eq!(
            parse_args(["--help"].map(String::from).into_iter()).unwrap(),
            Cli::Help
        );
        assert!(parse_args(["bogus"].map(String::from).into_iter()).is_err());
        assert!(parse_args(["net", "--rate"].map(String::from).into_iter()).is_err());
        assert!(parse_args(["net", "--symbols", "BTC"].map(String::from).into_iter()).is_err());
        assert!(parse_args(std::iter::empty()).is_err());
    }

    #[test]
    fn pacing_schedule_and_stride() {
        assert_eq!(send_deadline_ns(0, 1000), 0);
        assert_eq!(send_deadline_ns(1, 1000), 1_000_000);
        assert_eq!(send_deadline_ns(200_000, 200_000), 1_000_000_000);
        // no overflow at large i
        assert!(send_deadline_ns(u64::MAX / 2, 1) > 0);

        assert_eq!(sample_stride(100, 1000), 1);
        assert_eq!(sample_stride(2001, 1000), 3);
        assert_eq!(sample_stride(0, 1000), 1);
        assert_eq!(sample_stride(100, 0), 100);
    }

    #[test]
    fn sustained_verdict_and_rate() {
        assert!(is_sustained(200_000.0, 199_000.0));
        assert!(!is_sustained(200_000.0, 190_000.0));
        assert!(!is_sustained(0.0, 100.0));
        assert!((rate_per_sec(1_000, 1_000_000_000) - 1_000.0).abs() < 1e-9);
        assert_eq!(rate_per_sec(1_000, 0), 0.0);
    }

    #[test]
    fn live_message_classification() {
        assert_eq!(classify(&Message::Text("x".into())), LiveMsg::Data(b"x"));
        assert_eq!(
            classify(&Message::Binary(vec![1, 2].into())),
            LiveMsg::Data(&[1, 2])
        );
        assert_eq!(
            classify(&Message::Pong(vec![9].into())),
            LiveMsg::Pong(&[9])
        );
        assert_eq!(
            classify(&Message::Ping(vec![7].into())),
            LiveMsg::Ping(&[7])
        );
        assert_eq!(classify(&Message::Close(None)), LiveMsg::Close);
    }

    #[test]
    fn rtt_ping_pong_roundtrip() {
        let sent = 123_456_789u64;
        let p = ping_payload(sent);
        assert_eq!(rtt_from_pong(&p, sent + 1_000), Some(1_000));
        // saturates instead of underflowing if the echo beats the clock
        assert_eq!(rtt_from_pong(&p, 0), Some(0));
        // non-8-byte payloads (venue-originated pongs) are ignored
        assert_eq!(rtt_from_pong(b"", 5), None);
        assert_eq!(rtt_from_pong(b"123456789", 5), None);
    }

    #[test]
    fn venue_path_clamping() {
        assert_eq!(venue_path_sample(1_000, 400), (600, false));
        assert_eq!(venue_path_sample(400, 1_000), (0, true));
        assert_eq!(venue_path_sample(7, 7), (0, false));
    }

    #[test]
    fn early_close_note_formatting() {
        // before the deadline: a note with whole seconds elapsed + reason
        assert_eq!(
            early_close_note("kraken", 7_900_000_000, 500_000_000, 60_000_000_000, "eof"),
            Some("kraken closed early (7s in): eof".into())
        );
        // sub-second sessions read "0s in"
        assert_eq!(
            early_close_note("binance", 10, 5, 100, "x"),
            Some("binance closed early (0s in): x".into())
        );
        // at/after the deadline: clean end, no note
        assert_eq!(early_close_note("coinbase", 100, 0, 100, "x"), None);
        assert_eq!(early_close_note("coinbase", 200, 0, 100, "x"), None);
    }

    #[test]
    fn builtin_registry_btc_instruments() {
        let reg = Registry::builtin();
        assert_eq!(
            venue_instrument(&reg, Venue::Coinbase, "BTC"),
            Some(("BTC-USD".into(), 1))
        );
        assert_eq!(
            venue_instrument(&reg, Venue::Binance, "BTC"),
            Some(("BTCUSDT".into(), 6))
        );
        assert_eq!(
            venue_instrument(&reg, Venue::Kraken, "BTC"),
            Some(("BTC/USD".into(), 11))
        );
        assert_eq!(venue_instrument(&reg, Venue::Kraken, "NOPE"), None);
    }
}
