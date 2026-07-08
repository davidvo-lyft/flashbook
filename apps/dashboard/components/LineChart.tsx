/**
 * Hand-rolled SVG multi-series line/area chart. No chart library —
 * keeps the static bundle tiny and every pixel reviewable.
 */

export interface XY {
  x: number;
  y: number;
}

export interface Series {
  label: string;
  color: string;
  points: XY[];
}

interface Props {
  series: Series[];
  height?: number;
  /** Fill under each line with a translucent area. */
  area?: boolean;
  /** Format y-axis tick values. */
  yFmt?: (y: number) => string;
  /** Format x-axis tick values (x is unix seconds unless caller says otherwise). */
  xFmt?: (x: number) => string;
  /** Draw a vertical marker at this x (scrubber position). */
  markerX?: number;
  /** Force y-axis to start at zero (counters/rates). */
  zeroBase?: boolean;
}

const W = 720;
const PAD = { l: 62, r: 10, t: 10, b: 24 };

function extent(vals: number[]): [number, number] {
  let lo = Infinity;
  let hi = -Infinity;
  for (const v of vals) {
    if (v < lo) lo = v;
    if (v > hi) hi = v;
  }
  if (!Number.isFinite(lo)) return [0, 1];
  if (lo === hi) return [lo - 1, hi + 1];
  return [lo, hi];
}

function ticks(lo: number, hi: number, n: number): number[] {
  const out: number[] = [];
  for (let i = 0; i <= n; i += 1) out.push(lo + ((hi - lo) * i) / n);
  return out;
}

/** SVG line chart with grid, axes, optional area fill and scrubber marker. */
export default function LineChart({
  series,
  height = 220,
  area = false,
  yFmt = (y) => y.toPrecision(4),
  xFmt = (x) => String(Math.round(x)),
  markerX,
  zeroBase = false,
}: Props) {
  const drawn = series.filter((s) => s.points.length > 0);
  if (drawn.length === 0) {
    return <div className="chart-empty">no points</div>;
  }

  const allX = drawn.flatMap((s) => s.points.map((p) => p.x));
  const allY = drawn.flatMap((s) => s.points.map((p) => p.y));
  const [x0, x1] = extent(allX);
  let [y0, y1] = extent(allY);
  if (zeroBase) y0 = Math.min(0, y0);
  const ySpan = y1 - y0;
  y0 -= ySpan * 0.05;
  y1 += ySpan * 0.05;
  if (zeroBase && y0 < 0 && allY.every((y) => y >= 0)) y0 = 0;

  const iw = W - PAD.l - PAD.r;
  const ih = height - PAD.t - PAD.b;
  const sx = (x: number) => PAD.l + ((x - x0) / (x1 - x0)) * iw;
  const sy = (y: number) => PAD.t + (1 - (y - y0) / (y1 - y0)) * ih;

  const yTicks = ticks(y0, y1, 4);
  const xTicks = ticks(x0, x1, 4);

  return (
    <div className="chart">
      <div className="chart-legend">
        {drawn.map((s) => (
          <span key={s.label} className="chart-legend-item">
            <span className="chart-swatch" style={{ background: s.color }} />
            {s.label}
          </span>
        ))}
      </div>
      <svg viewBox={`0 0 ${W} ${height}`} width="100%" role="img" aria-label="line chart">
        {yTicks.map((t) => (
          <g key={`y${t}`}>
            <line x1={PAD.l} x2={W - PAD.r} y1={sy(t)} y2={sy(t)} className="chart-grid" />
            <text x={PAD.l - 6} y={sy(t) + 3} textAnchor="end" className="chart-tick">
              {yFmt(t)}
            </text>
          </g>
        ))}
        {xTicks.map((t, i) => (
          <text
            key={`x${t}`}
            x={sx(t)}
            y={height - 6}
            textAnchor={i === 0 ? "start" : i === xTicks.length - 1 ? "end" : "middle"}
            className="chart-tick"
          >
            {xFmt(t)}
          </text>
        ))}
        {drawn.map((s) => {
          const pts = [...s.points].sort((a, b) => a.x - b.x);
          const path = pts.map((p, i) => `${i === 0 ? "M" : "L"}${sx(p.x).toFixed(1)},${sy(p.y).toFixed(1)}`).join("");
          const base = sy(Math.max(y0, Math.min(y1, zeroBase ? 0 : y0)));
          return (
            <g key={s.label}>
              {area && pts.length > 1 && (
                <path
                  d={`${path}L${sx(pts[pts.length - 1].x).toFixed(1)},${base}L${sx(pts[0].x).toFixed(1)},${base}Z`}
                  fill={s.color}
                  opacity={0.12}
                />
              )}
              <path d={path} fill="none" stroke={s.color} strokeWidth={1.5} />
            </g>
          );
        })}
        {markerX !== undefined && markerX >= x0 && markerX <= x1 && (
          <line x1={sx(markerX)} x2={sx(markerX)} y1={PAD.t} y2={height - PAD.b} className="chart-marker" />
        )}
      </svg>
    </div>
  );
}
