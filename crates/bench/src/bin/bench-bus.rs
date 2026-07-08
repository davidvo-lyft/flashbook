//! bench-bus: broadcast fan-out benchmark — three contenders on identical
//! synthetic workloads (Phase 4, in-process half of 3d):
//!
//! - **A. `flashbook_bus::ring`** — the hand-rolled seqlock broadcast ring
//!   (capacity 65536). Consumers spin-poll with `std::hint::spin_loop`;
//!   that burns a core per subscriber but IS the ring's natural usage mode
//!   and is published as such. Overwrite-oldest: a slow subscriber loses
//!   events and is told how many.
//! - **B. `crossbeam-channel` fan-out** — one `bounded(65536)` channel PER
//!   subscriber; the producer `send()`s a copy of each event to every
//!   channel (the idiomatic crossbeam broadcast shape — crossbeam has no
//!   native broadcast). `send` BLOCKS when a channel is full, so the
//!   backpressure semantics differ from the ring's overwrite-oldest: a slow
//!   subscriber stalls the producer instead of losing data. That difference
//!   is part of the published comparison, not noise to be normalized away.
//! - **C. `tokio::sync::broadcast::channel(65536)`** — subscribers are async
//!   tasks on a `multi_thread` runtime using blocking-free `recv().await`.
//!   Its lagging semantics (oldest values overwritten, receiver gets
//!   `RecvError::Lagged(n)`) match the ring's overwrite-oldest more closely
//!   than crossbeam's backpressure does.
//!
//! For each contender x subscriber count in {1, 2, 4, 8}:
//!
//! 1. **Throughput:** the producer publishes 5M events (500k with
//!    `--quick`) flat-out from a seeded [`EventGen`]; subscribers consume to
//!    exhaustion counting delivered messages (+ lost where the contender
//!    can lose). Reported: producer-side publish rate, per-subscriber
//!    effective delivery rate, and loss counts.
//! 2. **Latency at a sustained paced rate** (500k msg/s full, 100k msg/s
//!    `--quick`): the producer paces against the shared mono clock on an
//!    absolute schedule (message `i` goes out at `start + i/rate`; if a
//!    blocking send stalls the producer it catches up in a burst — published
//!    as-is) and stamps each event's `recv_mono_ns` with
//!    [`clock::mono_ns`] at publish. Each subscriber computes
//!    `mono_ns at dequeue - stamp` per delivered message. Samples are
//!    capped at 2M per subscriber via **stride sampling**: the stride is
//!    fixed up front as `ceil(paced_msgs / cap)` so every k-th delivered
//!    message is sampled deterministically (no reservoir, no bias toward
//!    either end of the run). Reported as [`Percentiles`] per
//!    contender/sub-count, merged across subscribers (per-subscriber sample
//!    counts published alongside).
//!
//! Methodology (also embedded in the result file's notes):
//! - all threads share ONE process-monotonic clock ([`clock::mono_ns`]), so
//!   cross-thread stamp arithmetic is sound;
//! - threads are unpinned (macOS has no public affinity API) — scheduler
//!   noise is in the numbers;
//! - consumers use each contender's natural receive mode: spin-poll for the
//!   ring (CPU burn), blocking `recv()` for crossbeam, `recv().await` for
//!   tokio.
//!
//! Usage: `bench-bus [--quick] [--results-dir DIR] [--overwrite]`
//!
//! Writes `<results-dir>/bus_fanout.json` via [`flashbook_bench::results`].
//! Exit codes: 0 ok, 2 usage/IO.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use flashbook_bench::loadgen::EventGen;
use flashbook_bench::{Percentiles, ResultFile, write_result};
use flashbook_bus::Recv;
use flashbook_proto::{Event, clock};

/// Ring / channel capacity for all three contenders.
const CAPACITY: usize = 65_536;
/// Subscriber counts measured for every contender.
const SUB_COUNTS: [usize; 4] = [1, 2, 4, 8];
/// Throughput-phase message count (full run).
const THROUGHPUT_MSGS_FULL: u64 = 5_000_000;
/// Throughput-phase message count under `--quick`.
const THROUGHPUT_MSGS_QUICK: u64 = 500_000;
/// Latency-phase sustained publish rate, msg/s (full run).
const PACED_RATE_FULL: u64 = 500_000;
/// Latency-phase sustained publish rate, msg/s, under `--quick`.
const PACED_RATE_QUICK: u64 = 100_000;
/// Latency-phase message count (full run): 5 s at the full paced rate.
const PACED_MSGS_FULL: u64 = 2_500_000;
/// Latency-phase message count under `--quick`: 2 s at the quick rate.
const PACED_MSGS_QUICK: u64 = 200_000;
/// Hard cap on latency samples kept per subscriber (stride-sampled).
const SAMPLE_CAP_PER_SUB: u64 = 2_000_000;
/// EventGen seed — same synthetic stream for every contender.
const SEED: u64 = 0x0B05_FA17;

const USAGE: &str = "usage: bench-bus [--quick] [--results-dir DIR] [--overwrite]";

/// Parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    /// Smaller message counts and paced rate; marks the result non-official.
    quick: bool,
    /// Where `bus_fanout.json` is written.
    results_dir: PathBuf,
    /// Allow clobbering an existing result file.
    overwrite: bool,
}

/// Parse CLI args (everything after argv[0]). Pure; returns a usage error
/// string on bad input.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Config, String> {
    let mut quick = false;
    let mut results_dir = PathBuf::from("bench/results");
    let mut overwrite = false;
    let mut args = args.peekable();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--quick" => quick = true,
            "--results-dir" => {
                results_dir = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or("--results-dir needs a path")?;
            }
            "--overwrite" => overwrite = true,
            "--help" | "-h" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown arg: {other}\n{USAGE}")),
        }
    }
    Ok(Config {
        quick,
        results_dir,
        overwrite,
    })
}

/// Workload sizes for a run: `(throughput_msgs, paced_rate, paced_msgs)`.
fn workload(quick: bool) -> (u64, u64, u64) {
    if quick {
        (THROUGHPUT_MSGS_QUICK, PACED_RATE_QUICK, PACED_MSGS_QUICK)
    } else {
        (THROUGHPUT_MSGS_FULL, PACED_RATE_FULL, PACED_MSGS_FULL)
    }
}

/// Deterministic sampling stride so `total_msgs` messages yield at most
/// `cap` samples: every `stride`-th delivered message is kept.
fn sample_stride(total_msgs: u64, cap: u64) -> u64 {
    if cap == 0 {
        return total_msgs.max(1);
    }
    total_msgs.div_ceil(cap).max(1)
}

/// Events (or messages) per second from a count and elapsed nanoseconds.
fn rate_per_sec(count: u64, elapsed_ns: u64) -> f64 {
    if elapsed_ns == 0 {
        return 0.0;
    }
    count as f64 * 1e9 / elapsed_ns as f64
}

/// Absolute-schedule pacing: the mono-ns deadline for message `index` at
/// `rate` msg/s starting from `start_ns`.
fn pace_target_ns(start_ns: u64, index: u64, rate: u64) -> u64 {
    let off = (u128::from(index) * 1_000_000_000u128) / u128::from(rate.max(1));
    start_ns.saturating_add(u64::try_from(off).unwrap_or(u64::MAX))
}

/// Spin until the shared mono clock reaches `target_ns` (sub-µs pacing;
/// `thread::sleep` is far too coarse at 2 µs intervals).
#[inline]
fn spin_until(target_ns: u64) {
    while clock::mono_ns() < target_ns {
        std::hint::spin_loop();
    }
}

/// One subscriber's outcome for a run.
#[derive(Debug, Clone, serde::Serialize)]
struct SubStats {
    /// Messages actually received.
    delivered: u64,
    /// Messages irrecoverably lost (ring lag / tokio `Lagged`; always 0 for
    /// crossbeam, whose `send` blocks instead).
    lost: u64,
    /// Nanoseconds from the shared start barrier to this subscriber
    /// finishing its drain.
    elapsed_ns: u64,
}

/// Throughput-phase outcome for one contender x subscriber count.
struct ThroughputRun {
    /// Producer wall time publishing all messages, ns.
    producer_elapsed_ns: u64,
    /// Per-subscriber delivery stats.
    subs: Vec<SubStats>,
}

/// Latency-phase outcome for one contender x subscriber count.
struct LatencyRun {
    /// Producer wall time for the whole paced schedule, ns.
    producer_elapsed_ns: u64,
    /// Per-subscriber delivery stats.
    subs: Vec<SubStats>,
    /// Per-subscriber stride-sampled `dequeue - publish` latencies, ns.
    per_sub_samples: Vec<Vec<u64>>,
}

// ---------------------------------------------------------------- contenders

/// A. seqlock ring, flat-out throughput. Subscribers spin-poll until they
/// have accounted (delivered + lost) for every published message.
fn run_ring_throughput(events: &[Event], n_subs: usize) -> ThroughputRun {
    let mut prod = flashbook_bus::ring(CAPACITY);
    let total = events.len() as u64;
    let barrier = Arc::new(Barrier::new(n_subs + 1));
    let mut handles = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        let mut cons = prod.subscribe();
        let b = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            b.wait();
            let t0 = clock::mono_ns();
            let (mut delivered, mut lost) = (0u64, 0u64);
            while delivered + lost < total {
                match cons.try_next() {
                    Recv::Event(_) => delivered += 1,
                    Recv::Lagged { lost: l } => lost += l,
                    Recv::Empty => std::hint::spin_loop(),
                }
            }
            SubStats {
                delivered,
                lost,
                elapsed_ns: clock::mono_ns().saturating_sub(t0),
            }
        }));
    }
    barrier.wait();
    let t0 = clock::mono_ns();
    for ev in events {
        prod.publish(ev);
    }
    let producer_elapsed_ns = clock::mono_ns().saturating_sub(t0);
    let subs = handles
        .into_iter()
        .map(|h| h.join().expect("ring subscriber thread"))
        .collect();
    ThroughputRun {
        producer_elapsed_ns,
        subs,
    }
}

/// A. seqlock ring, paced latency. The producer stamps `recv_mono_ns` at
/// publish; subscribers spin-poll and stride-sample dequeue latency.
fn run_ring_latency(events: &[Event], n_subs: usize, rate: u64, stride: u64) -> LatencyRun {
    let mut prod = flashbook_bus::ring(CAPACITY);
    let total = events.len() as u64;
    let barrier = Arc::new(Barrier::new(n_subs + 1));
    let mut handles = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        let mut cons = prod.subscribe();
        let b = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            b.wait();
            let t0 = clock::mono_ns();
            let (mut delivered, mut lost) = (0u64, 0u64);
            let mut samples = Vec::new();
            while delivered + lost < total {
                match cons.try_next() {
                    Recv::Event(e) => {
                        if delivered.is_multiple_of(stride)
                            && (samples.len() as u64) < SAMPLE_CAP_PER_SUB
                        {
                            samples.push(clock::mono_ns().saturating_sub(e.recv_mono_ns));
                        }
                        delivered += 1;
                    }
                    Recv::Lagged { lost: l } => lost += l,
                    Recv::Empty => std::hint::spin_loop(),
                }
            }
            (
                SubStats {
                    delivered,
                    lost,
                    elapsed_ns: clock::mono_ns().saturating_sub(t0),
                },
                samples,
            )
        }));
    }
    barrier.wait();
    let start = clock::mono_ns();
    for (i, ev) in events.iter().enumerate() {
        spin_until(pace_target_ns(start, i as u64, rate));
        let mut e = *ev;
        e.recv_mono_ns = clock::mono_ns();
        prod.publish(&e);
    }
    let producer_elapsed_ns = clock::mono_ns().saturating_sub(start);
    let (subs, per_sub_samples) = handles
        .into_iter()
        .map(|h| h.join().expect("ring subscriber thread"))
        .unzip();
    LatencyRun {
        producer_elapsed_ns,
        subs,
        per_sub_samples,
    }
}

/// B. crossbeam fan-out, flat-out throughput: one bounded channel per
/// subscriber, one blocking `send` per subscriber per message; subscribers
/// block on `recv()` until disconnect.
fn run_crossbeam_throughput(events: &[Event], n_subs: usize) -> ThroughputRun {
    let barrier = Arc::new(Barrier::new(n_subs + 1));
    let mut txs = Vec::with_capacity(n_subs);
    let mut handles = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        let (tx, rx) = crossbeam_channel::bounded::<Event>(CAPACITY);
        txs.push(tx);
        let b = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            b.wait();
            let t0 = clock::mono_ns();
            let mut delivered = 0u64;
            while rx.recv().is_ok() {
                delivered += 1;
            }
            SubStats {
                delivered,
                lost: 0,
                elapsed_ns: clock::mono_ns().saturating_sub(t0),
            }
        }));
    }
    barrier.wait();
    let t0 = clock::mono_ns();
    for ev in events {
        for tx in &txs {
            tx.send(*ev).expect("crossbeam receiver alive");
        }
    }
    drop(txs); // disconnect: receivers drain then exit
    let producer_elapsed_ns = clock::mono_ns().saturating_sub(t0);
    let subs = handles
        .into_iter()
        .map(|h| h.join().expect("crossbeam subscriber thread"))
        .collect();
    ThroughputRun {
        producer_elapsed_ns,
        subs,
    }
}

/// B. crossbeam fan-out, paced latency. One stamp per message (taken once,
/// before the per-subscriber send loop — the stamp is publish time, and the
/// serial fan-out cost is part of what is being measured).
fn run_crossbeam_latency(events: &[Event], n_subs: usize, rate: u64, stride: u64) -> LatencyRun {
    let barrier = Arc::new(Barrier::new(n_subs + 1));
    let mut txs = Vec::with_capacity(n_subs);
    let mut handles = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        let (tx, rx) = crossbeam_channel::bounded::<Event>(CAPACITY);
        txs.push(tx);
        let b = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            b.wait();
            let t0 = clock::mono_ns();
            let mut delivered = 0u64;
            let mut samples = Vec::new();
            while let Ok(e) = rx.recv() {
                if delivered.is_multiple_of(stride) && (samples.len() as u64) < SAMPLE_CAP_PER_SUB {
                    samples.push(clock::mono_ns().saturating_sub(e.recv_mono_ns));
                }
                delivered += 1;
            }
            (
                SubStats {
                    delivered,
                    lost: 0,
                    elapsed_ns: clock::mono_ns().saturating_sub(t0),
                },
                samples,
            )
        }));
    }
    barrier.wait();
    let start = clock::mono_ns();
    for (i, ev) in events.iter().enumerate() {
        spin_until(pace_target_ns(start, i as u64, rate));
        let mut e = *ev;
        e.recv_mono_ns = clock::mono_ns();
        for tx in &txs {
            tx.send(e).expect("crossbeam receiver alive");
        }
    }
    drop(txs);
    let producer_elapsed_ns = clock::mono_ns().saturating_sub(start);
    let (subs, per_sub_samples) = handles
        .into_iter()
        .map(|h| h.join().expect("crossbeam subscriber thread"))
        .unzip();
    LatencyRun {
        producer_elapsed_ns,
        subs,
        per_sub_samples,
    }
}

/// Tokio helper: spawn `n_subs` receiver tasks on a fresh multi-thread
/// runtime, run `produce` on the calling thread once all tasks are live
/// (the `Sender` handle is sync — no async needed on the producer side),
/// then join the tasks.
fn tokio_fanout<F>(n_subs: usize, stride: u64, produce: F) -> LatencyRun
where
    F: FnOnce(&tokio::sync::broadcast::Sender<Event>) -> u64,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let (tx, _) = tokio::sync::broadcast::channel::<Event>(CAPACITY);
    let ready = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        let mut rx = tx.subscribe();
        let ready = Arc::clone(&ready);
        tasks.push(rt.spawn(async move {
            ready.fetch_add(1, Ordering::SeqCst);
            let t0 = clock::mono_ns();
            let (mut delivered, mut lost) = (0u64, 0u64);
            let mut samples = Vec::new();
            loop {
                match rx.recv().await {
                    Ok(e) => {
                        if delivered.is_multiple_of(stride)
                            && (samples.len() as u64) < SAMPLE_CAP_PER_SUB
                        {
                            samples.push(clock::mono_ns().saturating_sub(e.recv_mono_ns));
                        }
                        delivered += 1;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => lost += n,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            (
                SubStats {
                    delivered,
                    lost,
                    elapsed_ns: clock::mono_ns().saturating_sub(t0),
                },
                samples,
            )
        }));
    }
    // Subscriptions exist from `tx.subscribe()` on, so no message can be
    // missed; the spin just ensures every task is scheduled before timing.
    while ready.load(Ordering::SeqCst) < n_subs {
        std::hint::spin_loop();
    }
    let producer_elapsed_ns = produce(&tx);
    drop(tx); // Closed: receivers drain the buffer then exit
    let (subs, per_sub_samples) = tasks
        .into_iter()
        .map(|t| rt.block_on(t).expect("tokio subscriber task"))
        .unzip();
    LatencyRun {
        producer_elapsed_ns,
        subs,
        per_sub_samples,
    }
}

/// C. tokio broadcast, flat-out throughput.
fn run_tokio_throughput(events: &[Event], n_subs: usize) -> ThroughputRun {
    let run = tokio_fanout(n_subs, u64::MAX, |tx| {
        let t0 = clock::mono_ns();
        for ev in events {
            // Err only if every receiver dropped, which cannot happen
            // before Closed; ignore rather than pay a panic branch.
            let _ = tx.send(*ev);
        }
        clock::mono_ns().saturating_sub(t0)
    });
    ThroughputRun {
        producer_elapsed_ns: run.producer_elapsed_ns,
        subs: run.subs,
    }
}

/// C. tokio broadcast, paced latency.
fn run_tokio_latency(events: &[Event], n_subs: usize, rate: u64, stride: u64) -> LatencyRun {
    tokio_fanout(n_subs, stride, |tx| {
        let start = clock::mono_ns();
        for (i, ev) in events.iter().enumerate() {
            spin_until(pace_target_ns(start, i as u64, rate));
            let mut e = *ev;
            e.recv_mono_ns = clock::mono_ns();
            let _ = tx.send(e);
        }
        clock::mono_ns().saturating_sub(start)
    })
}

// -------------------------------------------------------------------- driver

/// JSON for one throughput point on a contender's curve.
fn throughput_point(subs: usize, msgs: u64, run: &ThroughputRun) -> serde_json::Value {
    let sub_json: Vec<serde_json::Value> = run
        .subs
        .iter()
        .map(|s| {
            serde_json::json!({
                "delivered": s.delivered,
                "lost": s.lost,
                "elapsed_ns": s.elapsed_ns,
                "effective_delivery_msgs_per_s": rate_per_sec(s.delivered, s.elapsed_ns),
            })
        })
        .collect();
    serde_json::json!({
        "subscribers": subs,
        "msgs": msgs,
        "producer_elapsed_ns": run.producer_elapsed_ns,
        "producer_publish_msgs_per_s": rate_per_sec(msgs, run.producer_elapsed_ns),
        "per_subscriber": sub_json,
    })
}

/// JSON for one latency point on a contender's curve. Percentiles are over
/// the merged (all-subscriber) stride samples.
fn latency_point(
    subs: usize,
    rate: u64,
    msgs: u64,
    stride: u64,
    run: &LatencyRun,
) -> serde_json::Value {
    let merged: Vec<u64> = run.per_sub_samples.iter().flatten().copied().collect();
    let pct = Percentiles::from_samples(&merged);
    let sub_json: Vec<serde_json::Value> = run
        .subs
        .iter()
        .zip(&run.per_sub_samples)
        .map(|(s, samp)| {
            serde_json::json!({
                "delivered": s.delivered,
                "lost": s.lost,
                "elapsed_ns": s.elapsed_ns,
                "samples": samp.len(),
            })
        })
        .collect();
    serde_json::json!({
        "subscribers": subs,
        "paced_rate_msgs_per_s": rate,
        "msgs": msgs,
        "sample_stride": stride,
        "producer_elapsed_ns": run.producer_elapsed_ns,
        "achieved_publish_msgs_per_s": rate_per_sec(msgs, run.producer_elapsed_ns),
        "per_subscriber": sub_json,
        "latency_ns": serde_json::to_value(pct).expect("percentiles serialize"),
    })
}

/// Methodology notes embedded in the result file.
const NOTES: &str = "In-process fan-out comparison on identical seeded EventGen workloads. \
Semantics differ BY DESIGN and are published as-is: the seqlock ring and tokio broadcast \
overwrite the oldest events when a subscriber lags (subscriber is told the loss count); \
crossbeam-channel fan-out (one bounded(65536) channel per subscriber, producer send()s a \
copy to each) BLOCKS the producer when any channel is full — backpressure, never loss. \
Receive modes are each contender's natural usage: ring consumers spin-poll with \
std::hint::spin_loop (burns a core per subscriber), crossbeam uses blocking recv(), tokio \
uses recv().await on a multi_thread runtime. All threads stamp and diff ONE process-\
monotonic clock (flashbook_proto::clock::mono_ns), so cross-thread latency math is sound; \
threads are unpinned (macOS). Latency phase paces the producer on an absolute schedule \
(message i at start + i/rate, spin-waited); a blocked crossbeam send makes it catch up in \
a burst afterwards. Latency samples are per delivered message, stride-sampled \
deterministically (every ceil(msgs/2M)-th message, capped at 2M per subscriber) and \
merged across subscribers for the published percentiles (nearest-rank).";

fn main() -> ExitCode {
    let cfg = match parse_args(std::env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    clock::init();
    let (tp_msgs, paced_rate, paced_msgs) = workload(cfg.quick);
    let stride = sample_stride(paced_msgs, SAMPLE_CAP_PER_SUB);

    // One shared event stream; both phases slice from it so every
    // contender sees byte-identical payloads. Pre-generated so generator
    // cost is outside every timed region.
    let n_gen = usize::try_from(tp_msgs.max(paced_msgs)).expect("msg count fits usize");
    let mut evgen = EventGen::new(SEED);
    let mut events: Vec<Event> = Vec::new();
    evgen.fill(n_gen, &mut events);
    let tp_events = &events[..usize::try_from(tp_msgs).expect("fits usize")];
    let lat_events = &events[..usize::try_from(paced_msgs).expect("fits usize")];

    type TpFn = fn(&[Event], usize) -> ThroughputRun;
    type LatFn = fn(&[Event], usize, u64, u64) -> LatencyRun;
    let contenders: [(&str, TpFn, LatFn); 3] = [
        ("ring", run_ring_throughput, run_ring_latency),
        (
            "crossbeam_channel",
            run_crossbeam_throughput,
            run_crossbeam_latency,
        ),
        ("tokio_broadcast", run_tokio_throughput, run_tokio_latency),
    ];

    let mut metrics = serde_json::Map::new();
    for (name, tp_fn, lat_fn) in contenders {
        let mut tp_curve = Vec::new();
        let mut lat_curve = Vec::new();
        for subs in SUB_COUNTS {
            eprintln!("[{name}] subs={subs} throughput ({tp_msgs} msgs flat-out)...");
            let tp = tp_fn(tp_events, subs);
            let lost: u64 = tp.subs.iter().map(|s| s.lost).sum();
            eprintln!(
                "[{name}] subs={subs} publish {:.2} Mmsg/s, total lost {lost}",
                rate_per_sec(tp_msgs, tp.producer_elapsed_ns) / 1e6
            );
            tp_curve.push(throughput_point(subs, tp_msgs, &tp));

            eprintln!(
                "[{name}] subs={subs} latency ({paced_msgs} msgs paced at {paced_rate}/s)..."
            );
            let lat = lat_fn(lat_events, subs, paced_rate, stride);
            let merged: Vec<u64> = lat.per_sub_samples.iter().flatten().copied().collect();
            if let Some(p) = Percentiles::from_samples(&merged) {
                eprintln!(
                    "[{name}] subs={subs} latency p50={} p99={} p999={} max={} (n={})",
                    p.p50, p.p99, p.p999, p.max, p.n
                );
            }
            lat_curve.push(latency_point(subs, paced_rate, paced_msgs, stride, &lat));
        }
        metrics.insert(
            name.to_string(),
            serde_json::json!({ "throughput": tp_curve, "latency": lat_curve }),
        );
    }

    let config = serde_json::json!({
        "quick": cfg.quick,
        "capacity": CAPACITY,
        "subscriber_counts": SUB_COUNTS,
        "throughput_msgs": tp_msgs,
        "paced_rate_msgs_per_s": paced_rate,
        "paced_msgs": paced_msgs,
        "sample_cap_per_subscriber": SAMPLE_CAP_PER_SUB,
        "sample_stride": stride,
        "seed": SEED,
    });
    let result = ResultFile::new(
        "bus_fanout",
        config,
        serde_json::Value::Object(metrics),
        NOTES,
    );
    match write_result(&cfg.results_dir, &result, cfg.overwrite) {
        Ok(p) => {
            println!("wrote {}", p.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("write failed: {e}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_defaults_and_flags() {
        let c = parse_args(std::iter::empty()).unwrap();
        assert_eq!(
            c,
            Config {
                quick: false,
                results_dir: PathBuf::from("bench/results"),
                overwrite: false,
            }
        );
        let c = parse_args(
            ["--quick", "--results-dir", "/tmp/x", "--overwrite"]
                .iter()
                .map(ToString::to_string),
        )
        .unwrap();
        assert!(c.quick && c.overwrite);
        assert_eq!(c.results_dir, PathBuf::from("/tmp/x"));
    }

    #[test]
    fn parse_args_rejects_bad_input() {
        assert!(parse_args(["--nope"].iter().map(ToString::to_string)).is_err());
        // --results-dir with no value
        assert!(parse_args(["--results-dir"].iter().map(ToString::to_string)).is_err());
        // --help is a usage "error" (prints usage, exit 2)
        assert!(parse_args(["-h"].iter().map(ToString::to_string)).is_err());
    }

    #[test]
    fn workload_sizes() {
        assert_eq!(
            workload(false),
            (THROUGHPUT_MSGS_FULL, PACED_RATE_FULL, PACED_MSGS_FULL)
        );
        assert_eq!(
            workload(true),
            (THROUGHPUT_MSGS_QUICK, PACED_RATE_QUICK, PACED_MSGS_QUICK)
        );
    }

    #[test]
    fn sample_stride_caps_at_target() {
        assert_eq!(sample_stride(100, 1000), 1, "under cap: keep everything");
        assert_eq!(sample_stride(2_000_000, 2_000_000), 1);
        assert_eq!(sample_stride(2_000_001, 2_000_000), 2);
        assert_eq!(sample_stride(10_000_000, 2_000_000), 5);
        assert_eq!(sample_stride(0, 2_000_000), 1);
        assert_eq!(sample_stride(7, 0), 7, "cap 0 degrades to one sample");
        // stride actually enforces the cap: ceil(total/stride) <= cap
        for (total, cap) in [(5_000_001u64, 2_000_000u64), (3, 2), (999, 10)] {
            let s = sample_stride(total, cap);
            assert!(total.div_ceil(s) <= cap, "total={total} cap={cap} s={s}");
        }
    }

    #[test]
    fn rate_per_sec_math() {
        assert!((rate_per_sec(1_000_000, 1_000_000_000) - 1e6).abs() < 1e-9);
        assert!((rate_per_sec(500, 1_000_000) - 500_000.0).abs() < 1e-6);
        assert_eq!(rate_per_sec(42, 0), 0.0, "zero elapsed degrades to 0");
    }

    #[test]
    fn pace_target_schedule_is_absolute_and_monotone() {
        // 500k msg/s => 2000 ns between messages, from the start anchor
        assert_eq!(pace_target_ns(1000, 0, 500_000), 1000);
        assert_eq!(pace_target_ns(1000, 1, 500_000), 3000);
        assert_eq!(pace_target_ns(1000, 5, 500_000), 11_000);
        // 100k msg/s => 10_000 ns spacing
        assert_eq!(pace_target_ns(0, 3, 100_000), 30_000);
        // monotone in index, no overflow at large indices
        let mut prev = 0;
        for i in [0u64, 1, 10, 1_000_000, u64::MAX / 2] {
            let t = pace_target_ns(5, i, 250_000);
            assert!(t >= prev);
            prev = t;
        }
        // rate 0 degrades to rate 1 rather than dividing by zero
        assert_eq!(pace_target_ns(0, 2, 0), 2_000_000_000);
    }
}
