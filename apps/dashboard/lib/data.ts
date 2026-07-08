/**
 * Client-side data loading for the static dashboard.
 *
 * All data lives in `public/data/*.json` and is fetched at runtime relative
 * to the deploy prefix (NEXT_PUBLIC_BASE_PATH, inlined at build time). A
 * missing or malformed file resolves to `null`, which every page renders as
 * an explicit "no data exported yet" state — the repo is expected to build
 * and deploy cleanly before any capture data has been exported.
 */

import { parseMeta, parseBooks, parseLag, parseBench } from "./contract.mjs";

/** Deploy prefix, e.g. "" locally or "/flashbook" on GitHub Pages. */
export const BASE_PATH = process.env.NEXT_PUBLIC_BASE_PATH ?? "";

// ---- Normalized shapes (mirrors lib/contract.mjs output) -----------------

/** One `[price, qty]` book level. */
export type Level = [number, number];

/** Per-venue soak counters from meta.json. */
export interface VenueSoak {
  venue: string;
  msgs: number;
  events: number;
  gaps: number;
  reconnects: number;
  parseErrors: number;
}

/** Normalized meta.json. */
export interface Meta {
  generatedUnixMs: number;
  corpus: {
    events: number;
    wsFrames: number;
    spanS: number;
    firstWallNs: number;
    lastWallNs: number;
    rawPayloadBytes: number;
    storeBytes: number;
    bytesPerEvent: number;
    checksumsOk: number;
    checksumMismatches: number;
    parseErrors: number;
    gaps: number;
  };
  soak: {
    present: boolean;
    uptimeS: number;
    msgs: number;
    events: number;
    gaps: number;
    reconnects: number;
    fallbacks: number;
    parseErrors: number;
    restSnaps: number;
    rssMaxMb: number;
    restarts: number;
    perVenue: VenueSoak[];
  };
  instruments: {
    id: number;
    venue: string;
    venueSymbol: string;
    canonical: string;
  }[];
}

/** One sampled book state in a replayed series. */
export interface BookPoint {
  t: number;
  bid: number;
  ask: number;
  mid: number;
  bid10: Level[];
  ask10: Level[];
}

/** Normalized books.json. */
export interface Books {
  window: { startWallNs: number; endWallNs: number; stepS: number };
  series: { instrument: number; label: string; points: BookPoint[] }[];
}

/** One per-minute ingest bucket for a venue. */
export interface MinuteBucket {
  tUnixS: number;
  venue: string;
  msgs: number;
  events: number;
  gaps: number;
  reconnects: number;
  segmentBytes: number;
}

/** Normalized lag.json. */
export interface Lag {
  perMinute: MinuteBucket[];
  venuePathMs: { venue: string; p50: number; p90: number; p99: number; n: number }[];
  queueNote: string;
}

/** One benchmark result row. */
export interface BenchRow {
  section: string;
  metric: string;
  value: string;
  unit: string;
  sourceFile: string;
}

/** Normalized bench.json. */
export interface Bench {
  available: boolean;
  generatedFrom: string;
  rows: BenchRow[];
}

// ---- Fetching -------------------------------------------------------------

async function fetchJson(name: string): Promise<unknown> {
  const res = await fetch(`${BASE_PATH}/data/${name}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${name}: HTTP ${res.status}`);
  return res.json();
}

async function load<T>(name: string, parse: (raw: unknown) => T | null): Promise<T | null> {
  try {
    return parse(await fetchJson(name));
  } catch {
    return null;
  }
}

/** Fetch and normalize meta.json; null if missing or malformed. */
export const loadMeta = (): Promise<Meta | null> => load("meta.json", parseMeta as (r: unknown) => Meta | null);
/** Fetch and normalize books.json; null if missing or malformed. */
export const loadBooks = (): Promise<Books | null> => load("books.json", parseBooks as (r: unknown) => Books | null);
/** Fetch and normalize lag.json; null if missing or malformed. */
export const loadLag = (): Promise<Lag | null> => load("lag.json", parseLag as (r: unknown) => Lag | null);
/** Fetch and normalize bench.json; null if missing or malformed. */
export const loadBench = (): Promise<Bench | null> => load("bench.json", parseBench as (r: unknown) => Bench | null);
