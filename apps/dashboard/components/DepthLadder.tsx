/**
 * Hand-rolled SVG depth ladder: top-10 asks stacked above top-10 bids,
 * horizontal size bars scaled to the largest level on either side.
 */

import type { Level } from "@/lib/data";
import { fmtPrice, fmtQty } from "@/lib/format";

interface Props {
  bids: Level[];
  asks: Level[];
}

const W = 420;
const ROW = 20;
const PRICE_X = 108;
const BAR_X = 118;
const BAR_W = W - BAR_X - 78;

/** One side of the book rendered as rows of price / bar / size. */
function rows(
  side: Level[],
  color: string,
  maxQty: number,
  yOf: (i: number) => number,
) {
  return side.map(([p, q], i) => {
    const y = yOf(i);
    const w = maxQty > 0 ? Math.max(1, (q / maxQty) * BAR_W) : 1;
    return (
      <g key={`${p}-${i}`}>
        <text x={PRICE_X} y={y + 14} textAnchor="end" className="ladder-price" fill={color}>
          {fmtPrice(p)}
        </text>
        <rect x={BAR_X} y={y + 4} width={w} height={ROW - 8} fill={color} opacity={0.35} rx={1} />
        <text x={W - 4} y={y + 14} textAnchor="end" className="ladder-qty">
          {fmtQty(q)}
        </text>
      </g>
    );
  });
}

/** Depth ladder for one instrument at one scrubber position. */
export default function DepthLadder({ bids, asks }: Props) {
  const b = bids.slice(0, 10);
  const a = asks.slice(0, 10);
  if (b.length === 0 && a.length === 0) {
    return <div className="chart-empty">no depth at this point</div>;
  }
  const maxQty = Math.max(...b.map(([, q]) => q), ...a.map(([, q]) => q), 0);
  const bestBid = b[0]?.[0] ?? 0;
  const bestAsk = a[0]?.[0] ?? 0;
  const spread = bestBid > 0 && bestAsk > 0 ? bestAsk - bestBid : NaN;
  const midH = 22;
  const height = (a.length + b.length) * ROW + midH + 8;
  // Asks: worst at top, best just above the spread row.
  const askY = (i: number) => (a.length - 1 - i) * ROW + 4;
  const bidY = (i: number) => a.length * ROW + midH + i * ROW + 4;

  return (
    <svg viewBox={`0 0 ${W} ${height}`} width="100%" role="img" aria-label="depth ladder">
      {rows(a, "var(--ask)", maxQty, askY)}
      <text x={PRICE_X} y={a.length * ROW + midH - 5} textAnchor="end" className="ladder-spread">
        {Number.isFinite(spread) ? `spread ${fmtPrice(spread)}` : "spread –"}
      </text>
      <line
        x1={0}
        x2={W}
        y1={a.length * ROW + midH / 2 + 2}
        y2={a.length * ROW + midH / 2 + 2}
        className="chart-grid"
      />
      {rows(b, "var(--bid)", maxQty, bidY)}
    </svg>
  );
}
