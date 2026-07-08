//! Soak capture binary (Phase 1): concurrently ingest Coinbase, Binance and
//! Kraken public WebSocket feeds, appending every raw frame to rotating
//! CRC-framed segments (the ground truth) and emitting per-minute JSONL
//! stats. Graceful shutdown on SIGINT/SIGTERM; any venue task exiting early
//! is treated as fatal (exit 1) so the watchdog restart is honestly counted.
//!
//! Args (hand-parsed, no clap):
//!   --data-dir <dir>     raw segment root          (default data/raw)
//!   --stats-file <path>  stats JSONL file          (default ops/soak/stats.jsonl)
//!   --pid-file <path>    pid file                  (default ops/soak/capture.pid)
//!   --venues a,b,c       subset of coinbase,binance,kraken (default all)
//!   --symbols A,B,...    base symbols              (default BTC,ETH,SOL,XRP,DOGE)

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use flashbook_feed::SymbolTable;
use flashbook_feed::binance::BinanceCodec;
use flashbook_feed::coinbase::CoinbaseCodec;
use flashbook_feed::conn::{RestPlan, RestTarget, VenueConfig, run_venue};
use flashbook_feed::kraken::KrakenCodec;
use flashbook_feed::sink::{DEFAULT_MAX_AGE, DEFAULT_MAX_BYTES, RotatingRawLog, meta_json};
use flashbook_feed::stats::{EmitterEntry, VenueStats, append_to_file, run_stats_emitter};
use flashbook_proto::instrument::InstrumentMeta;
use flashbook_proto::{Registry, Venue, clock};
use tokio::sync::{mpsc, watch};

struct Args {
    data_dir: PathBuf,
    stats_file: PathBuf,
    pid_file: PathBuf,
    venues: Vec<Venue>,
    symbols: Vec<String>,
}

fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        data_dir: PathBuf::from("data/raw"),
        stats_file: PathBuf::from("ops/soak/stats.jsonl"),
        pid_file: PathBuf::from("ops/soak/capture.pid"),
        venues: Venue::ALL.to_vec(),
        symbols: ["BTC", "ETH", "SOL", "XRP", "DOGE"]
            .iter()
            .map(ToString::to_string)
            .collect(),
    };
    let mut it = argv.iter();
    while let Some(a) = it.next() {
        let mut val = |name: &str| {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{name} requires a value"))
        };
        match a.as_str() {
            "--data-dir" => args.data_dir = PathBuf::from(val("--data-dir")?),
            "--stats-file" => args.stats_file = PathBuf::from(val("--stats-file")?),
            "--pid-file" => args.pid_file = PathBuf::from(val("--pid-file")?),
            "--venues" => {
                args.venues = val("--venues")?
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| match s.trim().to_ascii_lowercase().as_str() {
                        "coinbase" => Ok(Venue::Coinbase),
                        "binance" => Ok(Venue::Binance),
                        "kraken" => Ok(Venue::Kraken),
                        other => Err(format!("unknown venue: {other}")),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "--symbols" => {
                args.symbols = val("--symbols")?
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.trim().to_ascii_uppercase())
                    .collect();
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }
    if args.venues.is_empty() {
        return Err("--venues selected no venue".into());
    }
    if args.symbols.is_empty() {
        return Err("--symbols selected no symbol".into());
    }
    Ok(args)
}

/// REST snapshot plan for one venue over its subscribed instruments.
fn rest_plan(venue: Venue, metas: &[&InstrumentMeta]) -> RestPlan {
    let target = |m: &&InstrumentMeta, url: String| RestTarget {
        instrument: m.id,
        venue_symbol: m.venue_symbol.clone(),
        url,
    };
    match venue {
        // depth?limit=1000 costs 250 of the 6000/min weight budget; 1.5 s
        // spacing keeps even a reconnect storm far under it.
        Venue::Binance => RestPlan {
            targets: metas
                .iter()
                .map(|m| {
                    target(
                        m,
                        format!(
                            "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
                            m.venue_symbol
                        ),
                    )
                })
                .collect(),
            on_connect: true,
            refresh_every: Some(Duration::from_secs(30 * 60)),
            min_spacing: Duration::from_millis(1500),
        },
        // Public rate limit ~10 req/s; 15-min staggered polls are trivial.
        Venue::Coinbase => RestPlan {
            targets: metas
                .iter()
                .map(|m| {
                    target(
                        m,
                        format!(
                            "https://api.exchange.coinbase.com/products/{}/book?level=2",
                            m.venue_symbol
                        ),
                    )
                })
                .collect(),
            on_connect: false,
            refresh_every: Some(Duration::from_secs(15 * 60)),
            min_spacing: Duration::from_millis(500),
        },
        // Kraken resyncs in-band (snapshot on subscribe, CRC verification).
        Venue::Kraken => RestPlan::none(),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_args(&argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("capture: {e}");
            return ExitCode::from(2);
        }
    };

    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "capture exiting with failure");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    clock::init();
    let registry = Registry::builtin();

    // Pid file (start-soak.sh also writes it; both write the same pid).
    if let Some(parent) = args.pid_file.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut pf = std::fs::File::create(&args.pid_file)?;
    writeln!(pf, "{}", std::process::id())?;
    drop(pf);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // Venue tasks report early exits here (capacity: one slot per venue).
    let (fail_tx, mut fail_rx) = mpsc::channel::<Venue>(Venue::ALL.len());
    let http = reqwest::Client::builder()
        .user_agent("flashbook-capture/0.1")
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut entries = Vec::new();
    let mut handles = Vec::new();
    for &venue in &args.venues {
        let metas: Vec<&InstrumentMeta> = registry
            .for_venue(venue)
            .filter(|m| {
                args.symbols
                    .iter()
                    .any(|s| m.canonical.split('-').next() == Some(s.as_str()))
            })
            .collect();
        if metas.is_empty() {
            anyhow::bail!("no instruments match --symbols for venue {}", venue.name());
        }
        let table = SymbolTable::new(
            metas
                .iter()
                .map(|m| (m.venue_symbol.clone(), m.id))
                .collect::<Vec<_>>(),
        );
        let meta = meta_json(metas.iter().map(|m| (m.venue_symbol.as_str(), m.id)));
        let sink = RotatingRawLog::create(
            &args.data_dir,
            venue,
            meta,
            DEFAULT_MAX_BYTES,
            DEFAULT_MAX_AGE,
            true,
        )?;
        let stats = Arc::new(VenueStats::default());
        entries.push(EmitterEntry {
            venue,
            stats: Arc::clone(&stats),
            gauges: sink.gauges(),
        });
        let cfg = VenueConfig {
            rest: rest_plan(venue, &metas),
            ..VenueConfig::default()
        };
        let (http, rx) = (http.clone(), shutdown_rx.clone());
        let venue_fut = async move {
            match venue {
                Venue::Coinbase => {
                    let t = table;
                    run_venue(
                        venue,
                        cfg,
                        move || CoinbaseCodec::new(t.clone()),
                        sink,
                        stats,
                        http,
                        rx,
                    )
                    .await
                }
                Venue::Binance => {
                    let t = table;
                    run_venue(
                        venue,
                        cfg,
                        move || BinanceCodec::new(t.clone()),
                        sink,
                        stats,
                        http,
                        rx,
                    )
                    .await
                }
                Venue::Kraken => {
                    let t = table;
                    run_venue(
                        venue,
                        cfg,
                        move || KrakenCodec::new(t.clone()),
                        sink,
                        stats,
                        http,
                        rx,
                    )
                    .await
                }
            }
        };
        let fail_tx = fail_tx.clone();
        let watch_rx = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            let result = venue_fut.await;
            let during_shutdown = *watch_rx.borrow();
            if let Err(e) = &result {
                tracing::error!(venue = venue.name(), error = %e, "venue task failed");
                let _ = fail_tx.send(venue).await;
            } else if !during_shutdown {
                tracing::error!(venue = venue.name(), "venue task exited before shutdown");
                let _ = fail_tx.send(venue).await;
            }
            result
        }));
    }
    drop(fail_tx);

    let stats_handle = tokio::spawn(run_stats_emitter(
        entries,
        args.stats_file.clone(),
        Duration::from_secs(60),
        shutdown_rx.clone(),
    ));

    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let mut failed = false;
    tokio::select! {
        _ = sigint.recv() => tracing::info!("SIGINT: shutting down"),
        _ = sigterm.recv() => tracing::info!("SIGTERM: shutting down"),
        v = fail_rx.recv() => {
            if let Some(v) = v {
                tracing::error!(venue = v.name(), "venue task exit is fatal; shutting down");
            }
            failed = true;
        }
    }
    let _ = shutdown_tx.send(true);

    for h in handles {
        match tokio::time::timeout(Duration::from_secs(15), h).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(_))) => failed = true, // already logged by the wrapper
            Ok(Err(e)) => {
                tracing::error!(error = %e, "venue task panicked");
                failed = true;
            }
            Err(_) => {
                tracing::error!("venue task did not shut down within 15s");
                failed = true;
            }
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), stats_handle).await;

    let line = format!(
        "{{\"event\":\"shutdown\",\"ts_wall_ns\":{},\"clean\":{}}}\n",
        clock::wall_ns(),
        !failed
    );
    if let Err(e) = append_to_file(&args.stats_file, &line) {
        tracing::warn!(error = %e, "failed to append shutdown stats line");
    }
    let _ = std::fs::remove_file(&args.pid_file);

    if failed {
        anyhow::bail!("one or more venue tasks failed");
    }
    tracing::info!("capture shut down cleanly");
    Ok(())
}
