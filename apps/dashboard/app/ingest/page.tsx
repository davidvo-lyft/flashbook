"use client";

/**
 * Ingest health: per-minute msgs/events per venue, gap/reconnect/restart
 * counters (the zeros are the story), venue-path latency table, queue note,
 * and a segment-bytes gauge.
 */

import { useEffect, useState } from "react";
import BarGauge from "@/components/BarGauge";
import EmptyState from "@/components/EmptyState";
import LineChart from "@/components/LineChart";
import StatCard from "@/components/StatCard";
import { loadLag, loadMeta, type Lag, type Meta } from "@/lib/data";
import { fmtBytes, fmtClock, fmtInt, fmtMs, seriesColor } from "@/lib/format";
import { perVenueSeries } from "@/lib/contract.mjs";

interface VenueXY {
  venue: string;
  points: { x: number; y: number }[];
}

function venueChart(perMinute: Lag["perMinute"], field: "msgs" | "events") {
  const series = perVenueSeries(perMinute, field) as VenueXY[];
  return series.map((s, i) => ({ label: s.venue, color: seriesColor(i), points: s.points }));
}

export default function IngestPage() {
  const [lag, setLag] = useState<Lag | null | undefined>(undefined);
  const [meta, setMeta] = useState<Meta | null | undefined>(undefined);

  useEffect(() => {
    loadLag().then(setLag);
    loadMeta().then(setMeta);
  }, []);

  if (lag === undefined) return <div className="loading">loading…</div>;
  if (lag === null) {
    return (
      <>
        <h1>ingest</h1>
        <EmptyState file="lag.json" hint="Exported from ops/soak/stats.jsonl by the exporter." />
      </>
    );
  }

  const totalGaps = lag.perMinute.reduce((a, m) => a + m.gaps, 0);
  const totalReconnects = lag.perMinute.reduce((a, m) => a + m.reconnects, 0);
  const restarts = meta?.soak.present ? meta.soak.restarts : undefined;

  // Segment bytes is a gauge (current segment file size); show the latest
  // sample per venue rather than a sum.
  const segLatest = new Map<string, { t: number; bytes: number }>();
  for (const m of lag.perMinute) {
    const prev = segLatest.get(m.venue);
    if (!prev || m.tUnixS >= prev.t) segLatest.set(m.venue, { t: m.tUnixS, bytes: m.segmentBytes });
  }
  const gaugeRows = [...segLatest.entries()].map(([venue, { bytes }], i) => ({
    label: venue,
    value: bytes,
    color: seriesColor(i),
    text: fmtBytes(bytes),
  }));

  return (
    <>
      <h1>ingest</h1>
      <p className="subtitle">Per-minute feed health from the live soak, bucketed by venue.</p>

      <div className="stat-grid">
        <StatCard label="gaps (window)" value={fmtInt(totalGaps)} tone={totalGaps === 0 ? "ok" : "warn"} />
        <StatCard label="reconnects (window)" value={fmtInt(totalReconnects)} tone={totalReconnects === 0 ? "ok" : "warn"} />
        <StatCard
          label="restarts"
          value={restarts === undefined ? "–" : fmtInt(restarts)}
          tone={restarts === 0 ? "ok" : restarts === undefined ? undefined : "warn"}
          note={restarts === undefined ? "meta.json not loaded" : "watchdog-supervised"}
        />
      </div>

      <h2>messages / minute by venue</h2>
      <LineChart series={venueChart(lag.perMinute, "msgs")} height={220} area zeroBase yFmt={fmtInt} xFmt={fmtClock} />

      <h2>events / minute by venue</h2>
      <LineChart series={venueChart(lag.perMinute, "events")} height={220} area zeroBase yFmt={fmtInt} xFmt={fmtClock} />

      <h2>venue-path latency</h2>
      {lag.venuePathMs.length === 0 ? (
        <div className="chart-empty">no venue-path samples</div>
      ) : (
        <div className="panel">
          <table>
            <thead>
              <tr>
                <th>venue</th>
                <th className="num">p50</th>
                <th className="num">p90</th>
                <th className="num">p99</th>
                <th className="num">n</th>
              </tr>
            </thead>
            <tbody>
              {lag.venuePathMs.map((v) => (
                <tr key={v.venue}>
                  <td>{v.venue}</td>
                  <td className="num">{fmtMs(v.p50)}</td>
                  <td className="num">{fmtMs(v.p90)}</td>
                  <td className="num">{fmtMs(v.p99)}</td>
                  <td className="num">{fmtInt(v.n)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="caption">
            venue wall-clock → local receive: includes venue batching + WAN, not added by flashbook.
          </p>
        </div>
      )}

      {lag.queueNote && (
        <div className="note">
          <strong>queue:</strong> {lag.queueNote}
        </div>
      )}

      <h2>segment bytes (latest sample per venue)</h2>
      <div className="panel">
        <BarGauge rows={gaugeRows} />
        <p className="caption">current segment file size — the write-side gauge, not a cumulative total.</p>
      </div>
    </>
  );
}
