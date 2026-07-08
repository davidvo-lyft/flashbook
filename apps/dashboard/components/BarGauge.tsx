/**
 * Hand-rolled SVG horizontal bar gauge — one labeled bar per row,
 * scaled against the largest value (or an explicit max).
 */

interface Row {
  label: string;
  value: number;
  color: string;
  /** Preformatted value text shown at the end of the bar. */
  text: string;
}

interface Props {
  rows: Row[];
  max?: number;
}

const W = 720;
const ROW_H = 26;
const LABEL_W = 120;
const VALUE_W = 110;

/** Horizontal bar gauge (e.g. bytes written per venue). */
export default function BarGauge({ rows, max }: Props) {
  if (rows.length === 0) return <div className="chart-empty">no rows</div>;
  const m = max ?? Math.max(...rows.map((r) => r.value), 1);
  const barW = W - LABEL_W - VALUE_W - 16;
  const height = rows.length * ROW_H + 4;
  return (
    <svg viewBox={`0 0 ${W} ${height}`} width="100%" role="img" aria-label="bar gauge">
      {rows.map((r, i) => {
        const y = i * ROW_H + 2;
        const w = m > 0 ? Math.max(1, (r.value / m) * barW) : 1;
        return (
          <g key={r.label}>
            <text x={LABEL_W} y={y + 16} textAnchor="end" className="gauge-label">
              {r.label}
            </text>
            <rect x={LABEL_W + 8} y={y + 4} width={barW} height={ROW_H - 10} fill="var(--grid)" rx={2} />
            <rect x={LABEL_W + 8} y={y + 4} width={w} height={ROW_H - 10} fill={r.color} opacity={0.75} rx={2} />
            <text x={LABEL_W + 16 + barW} y={y + 16} className="gauge-value">
              {r.text}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
