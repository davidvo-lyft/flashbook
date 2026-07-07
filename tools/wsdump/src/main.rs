//! Dev tool: connect to a venue WebSocket, send subscribe messages, dump raw
//! text frames one-per-line for N seconds. Used to collect real codec
//! fixtures before the feed handlers existed; kept for debugging.

use std::io::Write as _;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut url: Option<String> = None;
    let mut subs: Vec<String> = Vec::new();
    let mut secs: u64 = 30;
    let mut out: Option<String> = None;
    let mut max_frames: u64 = u64::MAX;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--url" => url = args.next(),
            "--sub" => subs.push(args.next().ok_or_else(|| anyhow::anyhow!("--sub value"))?),
            "--secs" => {
                secs = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--secs value"))?
                    .parse()?
            }
            "--max-frames" => {
                max_frames = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--max-frames value"))?
                    .parse()?;
            }
            "--out" => out = args.next(),
            other => anyhow::bail!("unknown arg: {other}"),
        }
    }
    let url = url.ok_or_else(|| anyhow::anyhow!("--url required"))?;

    let (ws, resp) = tokio_tungstenite::connect_async(url.as_str()).await?;
    eprintln!("connected {} (http {})", url, resp.status());
    let (mut tx, mut rx) = ws.split();
    for s in &subs {
        tx.send(Message::Text(s.clone().into())).await?;
    }

    let mut w: Box<dyn std::io::Write> = match &out {
        Some(p) => Box::new(std::io::BufWriter::new(std::fs::File::create(p)?)),
        None => Box::new(std::io::stdout().lock()),
    };

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    let mut n_text = 0u64;
    let mut n_other = 0u64;
    while n_text < max_frames {
        let msg = tokio::select! {
            () = tokio::time::sleep_until(deadline) => break,
            m = rx.next() => match m { Some(m) => m?, None => break },
        };
        match msg {
            Message::Text(t) => {
                n_text += 1;
                w.write_all(t.as_bytes())?;
                w.write_all(b"\n")?;
            }
            Message::Ping(p) => {
                n_other += 1;
                tx.send(Message::Pong(p)).await?;
            }
            Message::Close(c) => {
                eprintln!("close: {c:?}");
                break;
            }
            _ => n_other += 1,
        }
    }
    w.flush()?;
    eprintln!("text_frames={n_text} other_frames={n_other}");
    Ok(())
}
