import { useId, useState } from "react";
import { cn } from "@/lib/utils";

const WIDTH = 300;
const HEIGHT = 100;
const PAD = 4;

/**
 * A small area chart; values are drawn left (oldest) to right. Pointing at it
 * marks one value and reports its index, so the caller can show it.
 */
export function Sparkline({
  values,
  label,
  className,
  onActiveChange,
}: {
  values: number[];
  label: string;
  className?: string;
  onActiveChange?: (index: number | null) => void;
}) {
  const id = useId();
  const [active, setActive] = useState<number | null>(null);
  const max = Math.max(1, ...values);
  const x = (i: number) => (values.length > 1 ? (i * WIDTH) / (values.length - 1) : WIDTH / 2);
  const y = (v: number) => PAD + (HEIGHT - PAD * 2) * (1 - v / max);
  const line = `M${values.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" L")}`;
  const area = `${line} L${WIDTH},${HEIGHT} L0,${HEIGHT} Z`;

  const point = (index: number | null) => {
    setActive(index);
    onActiveChange?.(index);
  };

  return (
    <div
      className={cn("relative h-full w-full touch-pan-y text-brand", className)}
      onPointerMove={(e) => {
        const box = e.currentTarget.getBoundingClientRect();
        const ratio = (e.clientX - box.left) / box.width;
        point(Math.min(values.length - 1, Math.max(0, Math.round(ratio * (values.length - 1)))));
      }}
      onPointerLeave={() => point(null)}
    >
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        preserveAspectRatio="none"
        className="h-full w-full overflow-visible"
        role="img"
        aria-label={label}
      >
        <defs>
          <linearGradient id={id} x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="currentColor" stopOpacity="0.28" />
            <stop offset="100%" stopColor="currentColor" stopOpacity="0" />
          </linearGradient>
        </defs>
        {[0.25, 0.5, 0.75].map((f) => (
          <line
            key={f}
            x1="0"
            x2={WIDTH}
            y1={HEIGHT * f}
            y2={HEIGHT * f}
            className="stroke-border"
            strokeDasharray="2 4"
            vectorEffect="non-scaling-stroke"
          />
        ))}
        <path d={area} fill={`url(#${id})`} />
        <path
          d={line}
          fill="none"
          stroke="currentColor"
          strokeWidth="1.75"
          strokeLinejoin="round"
          strokeLinecap="round"
          vectorEffect="non-scaling-stroke"
        />
      </svg>
      {active !== null && (
        <>
          <span
            className="pointer-events-none absolute inset-y-0 w-px bg-foreground/15"
            style={{ left: `${(x(active) / WIDTH) * 100}%` }}
          />
          <span
            className="pointer-events-none absolute size-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-brand bg-background"
            style={{ left: `${(x(active) / WIDTH) * 100}%`, top: `${(y(values[active]) / HEIGHT) * 100}%` }}
          />
        </>
      )}
    </div>
  );
}
