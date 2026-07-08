/** Dense headline stat card: label, big monospace value, optional footnote. */

interface Props {
  label: string;
  value: string;
  note?: string;
  /** Visual accent: "ok" (green), "warn" (amber), "bad" (red). */
  tone?: "ok" | "warn" | "bad";
}

/** One stat tile for the overview grid. */
export default function StatCard({ label, value, note, tone }: Props) {
  return (
    <div className={`stat${tone ? ` stat-${tone}` : ""}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {note && <div className="stat-note">{note}</div>}
    </div>
  );
}
