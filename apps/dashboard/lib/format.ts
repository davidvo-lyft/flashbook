/** Number/time formatting helpers shared across pages. */

/** Integer with thousands separators, e.g. 1234567 → "1,234,567". */
export function fmtInt(n: number): string {
  if (!Number.isFinite(n)) return "–";
  return Math.round(n).toLocaleString("en-US");
}

/** Compact byte count, e.g. 1536 → "1.5 KiB". */
export function fmtBytes(n: number): string {
  if (!Number.isFinite(n) || n < 0) return "–";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
}

/** Duration in seconds → "2d 03h 14m" style. */
export function fmtDuration(s: number): string {
  if (!Number.isFinite(s) || s < 0) return "–";
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d > 0) return `${d}d ${String(h).padStart(2, "0")}h ${String(m).padStart(2, "0")}m`;
  if (h > 0) return `${h}h ${String(m).padStart(2, "0")}m`;
  return `${m}m ${String(Math.floor(s % 60)).padStart(2, "0")}s`;
}

/** Price with sensible precision for crypto pairs. */
export function fmtPrice(p: number): string {
  if (!Number.isFinite(p) || p === 0) return "–";
  const abs = Math.abs(p);
  const dp = abs >= 1000 ? 1 : abs >= 10 ? 2 : abs >= 0.1 ? 4 : 6;
  return p.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp });
}

/** Quantity (base units) with up to 4 decimals. */
export function fmtQty(q: number): string {
  if (!Number.isFinite(q)) return "–";
  return q.toLocaleString("en-US", { maximumFractionDigits: 4 });
}

/** Unix seconds → "HH:MM:SS" UTC. */
export function fmtClock(unixS: number): string {
  if (!Number.isFinite(unixS) || unixS <= 0) return "–";
  return new Date(unixS * 1000).toISOString().slice(11, 19);
}

/** Unix seconds → "YYYY-MM-DD HH:MM:SS UTC". */
export function fmtStamp(unixS: number): string {
  if (!Number.isFinite(unixS) || unixS <= 0) return "–";
  return `${new Date(unixS * 1000).toISOString().slice(0, 19).replace("T", " ")} UTC`;
}

/** Milliseconds with one decimal, e.g. "12.3 ms". */
export function fmtMs(ms: number): string {
  if (!Number.isFinite(ms)) return "–";
  return `${ms.toFixed(1)} ms`;
}

/** Stable venue → accent color mapping (dark-background friendly). */
const VENUE_COLORS = ["#4cc38a", "#5fa8f5", "#e5a13c", "#c584f5", "#f56a6a", "#3cc8c8"];

/** Deterministic color per venue/series index. */
export function seriesColor(i: number): string {
  return VENUE_COLORS[((i % VENUE_COLORS.length) + VENUE_COLORS.length) % VENUE_COLORS.length];
}
