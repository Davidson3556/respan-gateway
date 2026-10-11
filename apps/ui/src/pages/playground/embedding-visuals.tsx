const STRIP_CELLS = 64;

/** Cosine similarity on its -1 to 1 range, with the score marked. */
export function SimilarityMeter({ score }: { score: number }) {
  const at = ((Math.max(-1, Math.min(1, score)) + 1) / 2) * 100;
  return (
    <div className="grid gap-2">
      <div
        role="meter"
        aria-label="Cosine similarity"
        aria-valuemin={-1}
        aria-valuemax={1}
        aria-valuenow={score}
        className="relative h-2 rounded-full bg-gradient-to-r from-warning/40 via-muted to-brand"
      >
        <span
          className="absolute top-1/2 size-4 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-brand bg-background shadow-sm"
          style={{ left: `${at}%` }}
        />
      </div>
      <div className="flex justify-between text-[11px] text-muted-foreground">
        <span>-1 opposite</span>
        <span>0 unrelated</span>
        <span>1 identical</span>
      </div>
    </div>
  );
}

/** The first dimensions of a vector as cells: accent for positive, amber for negative, stronger when larger. */
export function VectorStrip({ vector }: { vector: number[] }) {
  const cells = vector.slice(0, STRIP_CELLS);
  const max = Math.max(1e-9, ...cells.map(Math.abs));
  return (
    <div
      className="grid gap-px overflow-hidden rounded-md"
      style={{ gridTemplateColumns: `repeat(${STRIP_CELLS / 2}, minmax(0, 1fr))` }}
      aria-hidden="true"
    >
      {cells.map((v, i) => (
        <span
          key={i}
          className="h-3"
          style={{
            backgroundColor: `color-mix(in oklab, var(${v < 0 ? "--warning" : "--brand"}) ${Math.round((Math.abs(v) / max) * 100)}%, var(--muted))`,
          }}
        />
      ))}
    </div>
  );
}
