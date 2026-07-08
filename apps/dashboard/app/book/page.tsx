"use client";

/**
 * Replayed book viewer: instrument selector, time scrubber over the sampled
 * window, top-10 depth ladder, and a bid/ask/mid strip chart with a marker
 * at the scrubber position.
 */

import { useEffect, useMemo, useState } from "react";
import DepthLadder from "@/components/DepthLadder";
import EmptyState from "@/components/EmptyState";
import LineChart from "@/components/LineChart";
import { loadBooks, type Books } from "@/lib/data";
import { fmtClock, fmtPrice, fmtStamp } from "@/lib/format";
import { toUnixSec } from "@/lib/contract.mjs";

export default function BookPage() {
  const [books, setBooks] = useState<Books | null | undefined>(undefined);
  const [sel, setSel] = useState(0);
  const [idx, setIdx] = useState(0);

  useEffect(() => {
    loadBooks().then((b) => {
      setBooks(b);
      // Start the scrubber mid-window so the first paint shows a real book.
      if (b && b.series[0]) setIdx(Math.floor(b.series[0].points.length / 2));
    });
  }, []);

  const series = books?.series[sel];
  const points = useMemo(() => series?.points ?? [], [series]);
  const clamped = Math.min(idx, Math.max(0, points.length - 1));
  const pt = points[clamped];

  const strip = useMemo(() => {
    if (points.length === 0) return [];
    const xy = (f: (p: (typeof points)[number]) => number) =>
      points.filter((p) => f(p) > 0).map((p) => ({ x: toUnixSec(p.t), y: f(p) }));
    return [
      { label: "ask", color: "var(--ask)" as string, points: xy((p) => p.ask) },
      { label: "mid", color: "#77839a", points: xy((p) => p.mid) },
      { label: "bid", color: "var(--bid)" as string, points: xy((p) => p.bid) },
    ];
  }, [points]);

  if (books === undefined) return <div className="loading">loading…</div>;
  if (books === null) {
    return (
      <>
        <h1>book</h1>
        <EmptyState file="books.json" hint="The exporter replays the capture and samples top-10 depth on a fixed step." />
      </>
    );
  }

  return (
    <>
      <h1>book</h1>
      <p className="subtitle">
        Order books rebuilt by deterministic replay of the capture, sampled every{" "}
        {books.window.stepS || "?"}s. Scrub through the window; the ladder shows top-10 depth at
        the selected instant.
      </p>

      <div className="controls">
        <label htmlFor="inst">instrument</label>
        <select
          id="inst"
          value={sel}
          onChange={(e) => {
            const i = Number(e.target.value);
            setSel(i);
            setIdx(Math.floor((books.series[i]?.points.length ?? 0) / 2));
          }}
        >
          {books.series.map((s, i) => (
            <option key={s.instrument} value={i}>
              {s.label}
            </option>
          ))}
        </select>
        <input
          type="range"
          min={0}
          max={Math.max(0, points.length - 1)}
          value={clamped}
          onChange={(e) => setIdx(Number(e.target.value))}
          aria-label="time scrubber"
        />
        <span className="scrub-time">{pt ? fmtStamp(toUnixSec(pt.t)) : "–"}</span>
      </div>

      {pt ? (
        <div className="book-grid">
          <div className="panel">
            <h2 style={{ marginTop: 0 }}>depth · top 10</h2>
            <DepthLadder bids={pt.bid10} asks={pt.ask10} />
            <p className="caption">
              bid {fmtPrice(pt.bid)} · mid {fmtPrice(pt.mid)} · ask {fmtPrice(pt.ask)}
            </p>
          </div>
          <div>
            <h2 style={{ marginTop: 0 }}>bid / ask / mid</h2>
            <LineChart
              series={strip}
              height={280}
              yFmt={(y) => y.toFixed(1)}
              xFmt={fmtClock}
              markerX={toUnixSec(pt.t)}
            />
          </div>
        </div>
      ) : (
        <div className="chart-empty">selected series has no points</div>
      )}
    </>
  );
}
