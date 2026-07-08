"use client";

/** Benchmarks: headline table grouped by section, or an honest pending state. */

import { useEffect, useState } from "react";
import EmptyState from "@/components/EmptyState";
import { loadBench, type Bench, type BenchRow } from "@/lib/data";
import { groupBenchRows } from "@/lib/contract.mjs";

interface Group {
  section: string;
  rows: BenchRow[];
}

export default function BenchPage() {
  const [bench, setBench] = useState<Bench | null | undefined>(undefined);

  useEffect(() => {
    loadBench().then(setBench);
  }, []);

  if (bench === undefined) return <div className="loading">loading…</div>;
  if (bench === null) {
    return (
      <>
        <h1>bench</h1>
        <EmptyState file="bench.json" />
      </>
    );
  }

  if (!bench.available) {
    return (
      <>
        <h1>bench</h1>
        <div className="empty">
          <div className="empty-title">official numbers pending</div>
          <p>
            Benchmarks are run on a quiet machine, not the capture box; smoke runs are not
            published. Methodology in <code>BENCHMARKS.md</code>.
          </p>
        </div>
      </>
    );
  }

  const groups = groupBenchRows(bench.rows) as Group[];

  return (
    <>
      <h1>bench</h1>
      <p className="subtitle">
        Headline results{bench.generatedFrom ? ` · generated from ${bench.generatedFrom}` : ""}. Full
        methodology and raw result files in <code>BENCHMARKS.md</code>.
      </p>
      {groups.length === 0 ? (
        <div className="chart-empty">bench.json has no rows</div>
      ) : (
        groups.map((g) => (
          <section key={g.section}>
            <h2>{g.section}</h2>
            <div className="panel">
              <table>
                <thead>
                  <tr>
                    <th>metric</th>
                    <th className="num">value</th>
                    <th>unit</th>
                    <th>source</th>
                  </tr>
                </thead>
                <tbody>
                  {g.rows.map((r, i) => (
                    <tr key={`${r.metric}-${i}`}>
                      <td>{r.metric}</td>
                      <td className="num">{r.value}</td>
                      <td>{r.unit}</td>
                      <td>{r.sourceFile}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ))
      )}
    </>
  );
}
