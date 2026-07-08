"use client";

/** Overview: headline corpus/soak stats + multi-venue BTC mid-price chart. */

import { useEffect, useState } from "react";
import EmptyState from "@/components/EmptyState";
import LineChart from "@/components/LineChart";
import StatCard from "@/components/StatCard";
import { loadBooks, loadMeta, type Books, type Meta } from "@/lib/data";
import { fmtBytes, fmtClock, fmtDuration, fmtInt, fmtStamp, seriesColor } from "@/lib/format";
import { toUnixSec } from "@/lib/contract.mjs";

export default function OverviewPage() {
  const [meta, setMeta] = useState<Meta | null | undefined>(undefined);
  const [books, setBooks] = useState<Books | null | undefined>(undefined);

  useEffect(() => {
    loadMeta().then(setMeta);
    loadBooks().then(setBooks);
  }, []);

  if (meta === undefined) return <div className="loading">loading…</div>;

  const btcSeries =
    books?.series
      .filter((s) => s.label.toUpperCase().includes("BTC"))
      .map((s, i) => ({
        label: s.label,
        color: seriesColor(i),
        points: s.points.filter((p) => p.mid > 0).map((p) => ({ x: toUnixSec(p.t), y: p.mid })),
      })) ?? [];

  return (
    <>
      <h1>overview</h1>
      <p className="subtitle">
        Evidence dashboard for the flashbook capture → store → replay pipeline. Every number below
        is computed from exported capture artifacts.
      </p>

      {meta === null ? (
        <EmptyState
          file="meta.json"
          hint="Clone-and-build works without exports; stats appear once the exporter has run."
        />
      ) : (
        <>
          <h2>capture corpus</h2>
          <div className="stat-grid">
            <StatCard label="events" value={fmtInt(meta.corpus.events)} note={`${fmtInt(meta.corpus.wsFrames)} ws frames`} />
            <StatCard
              label="checksums verified"
              value={fmtInt(meta.corpus.checksumsOk)}
              note={`${fmtInt(meta.corpus.checksumMismatches)} mismatches`}
              tone={meta.corpus.checksumMismatches === 0 ? "ok" : "bad"}
            />
            <StatCard
              label="parse errors / gaps"
              value={`${fmtInt(meta.corpus.parseErrors)} / ${fmtInt(meta.corpus.gaps)}`}
              tone={meta.corpus.parseErrors + meta.corpus.gaps === 0 ? "ok" : "warn"}
            />
            <StatCard
              label="store size"
              value={fmtBytes(meta.corpus.storeBytes)}
              note={`${meta.corpus.bytesPerEvent.toFixed(1)} B/event vs ${fmtBytes(meta.corpus.rawPayloadBytes)} raw`}
            />
            <StatCard label="span" value={fmtDuration(meta.corpus.spanS)} note={fmtStamp(toUnixSec(meta.corpus.firstWallNs))} />
          </div>

          <h2>live soak</h2>
          {meta.soak.present ? (
            <div className="stat-grid">
              <StatCard label="uptime" value={fmtDuration(meta.soak.uptimeS)} note={`${fmtInt(meta.soak.restarts)} restarts`} tone={meta.soak.restarts === 0 ? "ok" : "warn"} />
              <StatCard label="messages" value={fmtInt(meta.soak.msgs)} note={`${fmtInt(meta.soak.events)} events`} />
              <StatCard label="gaps" value={fmtInt(meta.soak.gaps)} tone={meta.soak.gaps === 0 ? "ok" : "warn"} note={`${fmtInt(meta.soak.reconnects)} reconnects, ${fmtInt(meta.soak.fallbacks)} fallbacks`} />
              <StatCard label="parse errors" value={fmtInt(meta.soak.parseErrors)} tone={meta.soak.parseErrors === 0 ? "ok" : "bad"} note={`${fmtInt(meta.soak.restSnaps)} REST snapshots`} />
              <StatCard label="rss max" value={`${fmtInt(meta.soak.rssMaxMb)} MB`} />
            </div>
          ) : (
            <p className="subtitle">soak stats not present in this export.</p>
          )}
        </>
      )}

      <h2>BTC mid price, all venues</h2>
      {books === undefined ? (
        <div className="loading">loading…</div>
      ) : books === null ? (
        <EmptyState file="books.json" />
      ) : btcSeries.length === 0 ? (
        <div className="chart-empty">no BTC series in books.json</div>
      ) : (
        <LineChart series={btcSeries} height={260} yFmt={(y) => y.toFixed(0)} xFmt={fmtClock} />
      )}

      <div className="note">
        <strong>What you are looking at:</strong> replayed capture data exported from the live soak
        — this deployment is a static snapshot, not a live feed. See{" "}
        <code>README — Vercel/live mode notes</code> for running against fresh exports.
      </div>
    </>
  );
}
