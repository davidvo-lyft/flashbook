#!/usr/bin/env node
/**
 * Data-contract check: validates lib/contract.mjs (the exact parsing code the
 * dashboard ships) against embedded fixture JSON matching the exporter
 * schemas, plus malformed/missing-data paths. Runs under plain `node` with
 * zero build steps; wired as `npm run check` and executed in CI before the
 * static build.
 */

import assert from "node:assert/strict";
import {
  parseMeta,
  parseBooks,
  parseLag,
  parseBench,
  groupBenchRows,
  perVenueSeries,
  toUnixSec,
} from "../lib/contract.mjs";

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`ok   ${name}`);
}

// ---- fixtures (schema-exact, mirroring the exporter contract) -------------

const metaFixture = {
  generated_unix_ms: 1751900000000,
  corpus: {
    events: 12345678,
    ws_frames: 2345678,
    span_s: 86400,
    first_wall_ns: 1751800000000000000,
    last_wall_ns: 1751886400000000000,
    raw_payload_bytes: 9876543210,
    store_bytes: 790123456,
    bytes_per_event: 64.0,
    checksums_ok: 51234,
    checksum_mismatches: 0,
    parse_errors: 0,
    gaps: 0,
  },
  soak: {
    present: true,
    uptime_s: 172800,
    msgs: 9876543,
    events: 12345678,
    gaps: 0,
    reconnects: 2,
    fallbacks: 1,
    parse_errors: 0,
    rest_snaps: 12,
    rss_max_mb: 145,
    restarts: 0,
    per_venue: [
      { venue: "kraken", msgs: 5000000, events: 7000000, gaps: 0, reconnects: 1, parse_errors: 0 },
      { venue: "coinbase", msgs: 4876543, events: 5345678, gaps: 0, reconnects: 1, parse_errors: 0 },
    ],
  },
  instruments: [
    { id: 0, venue: "kraken", venue_symbol: "XBT/USD", canonical: "BTC-USD" },
    { id: 1, venue: "coinbase", venue_symbol: "BTC-USD", canonical: "BTC-USD" },
  ],
};

const booksFixture = {
  window: { start_wall_ns: 1751800000000000000, end_wall_ns: 1751800600000000000, step_s: 60 },
  series: [
    {
      instrument: 0,
      label: "kraken BTC-USD",
      points: [
        {
          t: 1751800000,
          bid: 64999.5,
          ask: 65000.5,
          mid: 65000.0,
          bid10: [
            [64999.5, 0.5],
            [64999.0, 1.2],
          ],
          ask10: [
            [65000.5, 0.4],
            [65001.0, 2.0],
          ],
        },
        { t: 1751800060, bid: 65010.0, ask: 65011.0, mid: 65010.5, bid10: [[65010.0, 0.7]], ask10: [[65011.0, 0.3]] },
      ],
    },
  ],
};

const lagFixture = {
  per_minute: [
    { t_unix_s: 1751800000, venue: "kraken", msgs: 1200, events: 1800, gaps: 0, reconnects: 0, segment_bytes: 115200 },
    { t_unix_s: 1751800060, venue: "kraken", msgs: 1300, events: 1900, gaps: 0, reconnects: 0, segment_bytes: 121600 },
    { t_unix_s: 1751800000, venue: "coinbase", msgs: 900, events: 1100, gaps: 0, reconnects: 0, segment_bytes: 70400 },
  ],
  venue_path_ms: [{ venue: "kraken", p50: 42.1, p90: 88.7, p99: 210.4, n: 100000 }],
  queue_note: "bus queue depth stayed < 1 slot at p99; venue path dominates end-to-end.",
};

const benchFixture = {
  available: true,
  generated_from: "bench/results/2026-07-06",
  rows: [
    { section: "bus", metric: "publish p50", value: 42, unit: "ns", source_file: "bus_spsc.json" },
    { section: "bus", metric: "publish p99", value: 180, unit: "ns", source_file: "bus_spsc.json" },
    { section: "store", metric: "scan throughput", value: "812", unit: "M events/s", source_file: "scan.json" },
  ],
};

// ---- meta.json -------------------------------------------------------------

check("parseMeta: happy path normalizes corpus + soak + instruments", () => {
  const m = parseMeta(metaFixture);
  assert.equal(m.corpus.events, 12345678);
  assert.equal(m.corpus.checksumMismatches, 0);
  assert.equal(m.soak.present, true);
  assert.equal(m.soak.perVenue.length, 2);
  assert.equal(m.soak.perVenue[0].venue, "kraken");
  assert.equal(m.instruments[1].canonical, "BTC-USD");
});

check("parseMeta: missing sections coerce to zeroed defaults, not crashes", () => {
  const m = parseMeta({ generated_unix_ms: 1 });
  assert.equal(m.corpus.events, 0);
  assert.equal(m.soak.present, false);
  assert.deepEqual(m.soak.perVenue, []);
  assert.deepEqual(m.instruments, []);
});

check("parseMeta: non-object payloads return null (empty-state path)", () => {
  assert.equal(parseMeta(null), null);
  assert.equal(parseMeta("nope"), null);
  assert.equal(parseMeta([1, 2]), null);
});

// ---- books.json ------------------------------------------------------------

check("parseBooks: happy path keeps window, points, and depth levels", () => {
  const b = parseBooks(booksFixture);
  assert.equal(b.window.stepS, 60);
  assert.equal(b.series.length, 1);
  assert.equal(b.series[0].points.length, 2);
  assert.deepEqual(b.series[0].points[0].bid10[0], [64999.5, 0.5]);
  assert.equal(b.series[0].points[0].mid, 65000.0);
});

check("parseBooks: drops malformed levels and empty series; null when nothing usable", () => {
  const b = parseBooks({
    window: {},
    series: [
      { instrument: 0, label: "x", points: [{ t: 1, bid: 1, ask: 2, mid: 1.5, bid10: [[1, 1], ["bad"], [2]], ask10: null }] },
      { instrument: 1, label: "empty", points: [] },
    ],
  });
  assert.equal(b.series.length, 1);
  assert.deepEqual(b.series[0].points[0].bid10, [[1, 1]]);
  assert.deepEqual(b.series[0].points[0].ask10, []);
  assert.equal(parseBooks({ window: {}, series: [] }), null);
  assert.equal(parseBooks(undefined), null);
});

// ---- lag.json --------------------------------------------------------------

check("parseLag: happy path normalizes buckets, latency rows, queue note", () => {
  const l = parseLag(lagFixture);
  assert.equal(l.perMinute.length, 3);
  assert.equal(l.perMinute[0].segmentBytes, 115200);
  assert.equal(l.venuePathMs[0].p99, 210.4);
  assert.match(l.queueNote, /venue path dominates/);
});

check("perVenueSeries: buckets per venue, sorted by time", () => {
  const l = parseLag(lagFixture);
  const s = perVenueSeries(l.perMinute, "msgs");
  assert.equal(s.length, 2);
  const kraken = s.find((v) => v.venue === "kraken");
  assert.deepEqual(
    kraken.points.map((p) => p.y),
    [1200, 1300],
  );
  assert.ok(kraken.points[0].x < kraken.points[1].x);
});

// ---- bench.json ------------------------------------------------------------

check("parseBench: happy path + section grouping preserves order", () => {
  const b = parseBench(benchFixture);
  assert.equal(b.available, true);
  assert.equal(b.rows.length, 3);
  assert.equal(b.rows[0].value, "42"); // numeric values stringified for display
  const groups = groupBenchRows(b.rows);
  assert.deepEqual(
    groups.map((g) => g.section),
    ["bus", "store"],
  );
  assert.equal(groups[0].rows.length, 2);
});

check("parseBench: available=false and garbage rows survive", () => {
  const b = parseBench({ available: false, rows: [null, 42, { metric: "m" }] });
  assert.equal(b.available, false);
  assert.equal(b.rows.length, 1);
  assert.equal(b.rows[0].section, "misc");
  assert.equal(parseBench(null), null);
});

// ---- timestamp normalization ----------------------------------------------

check("toUnixSec: handles s / ms / us / ns and junk", () => {
  assert.equal(toUnixSec(1751800000), 1751800000);
  assert.equal(toUnixSec(1751800000000), 1751800000);
  assert.equal(toUnixSec(1751800000000000), 1751800000);
  assert.equal(toUnixSec(1751800000000000000), 1751800000);
  assert.equal(toUnixSec("not a time"), 0);
  assert.equal(toUnixSec(-5), 0);
});

console.log(`\n${passed} contract checks passed`);
