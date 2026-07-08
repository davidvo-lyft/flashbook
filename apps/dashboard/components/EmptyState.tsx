/** Explicit empty state used when a data file is missing or malformed. */

interface Props {
  file: string;
  hint?: string;
}

/** "No data exported yet" panel; the repo builds clean without exports. */
export default function EmptyState({ file, hint }: Props) {
  return (
    <div className="empty">
      <div className="empty-title">no data exported yet</div>
      <p>
        <code>public/data/{file}</code> is missing or unreadable. This dashboard is a static
        viewer over exported capture artifacts; run the exporter to generate it.
      </p>
      {hint && <p className="empty-hint">{hint}</p>}
    </div>
  );
}
