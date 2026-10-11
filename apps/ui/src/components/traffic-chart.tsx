import { useState } from "react";
import { Sparkline } from "@/components/sparkline";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useRequests, useTraffic } from "@/hooks/use-live-requests";
import { useNow } from "@/hooks/use-now";
import { formatMs } from "@/lib/format";
import type { Bucket } from "@/lib/series";
import { cn } from "@/lib/utils";

const WINDOW_MINUTES = 30;

const metrics = {
  requests: {
    label: "Requests",
    unit: "per minute",
    pick: (b: Bucket) => b.requests,
    show: (n: number) => String(n),
  },
  latency: {
    label: "Latency",
    unit: "mean",
    pick: (b: Bucket) => b.latencyMs,
    show: (n: number) => formatMs(n || null),
  },
  errors: {
    label: "Errors",
    unit: "per minute",
    pick: (b: Bucket) => b.errors,
    show: (n: number) => String(n),
  },
};
type Metric = keyof typeof metrics;

function minutesAgo(index: number): string {
  const ago = WINDOW_MINUTES - 1 - index;
  return ago === 0 ? "this minute" : `${ago} min ago`;
}

/** Requests, latency or errors per minute, with the value under the pointer. */
export function TrafficChart({ className }: { className?: string }) {
  const [metric, setMetric] = useState<Metric>("requests");
  const [active, setActive] = useState<number | null>(null);
  const traffic = useTraffic(WINDOW_MINUTES, useNow(10_000));
  const { isPending } = useRequests();
  const { label, unit, pick, show } = metrics[metric];
  const values = traffic.map(pick);
  const index = active ?? values.length - 1;
  const peak = Math.max(...values);

  return (
    <Card className={cn("px-5", className)}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-xs font-medium text-muted-foreground">
            {label} · {unit}
          </div>
          <div className="mt-1.5 flex items-baseline gap-2 text-3xl font-semibold tracking-tight tabular">
            {isPending ? <Skeleton className="h-9 w-20" /> : show(values[index])}
            <span className="text-xs font-normal tracking-normal text-muted-foreground">
              {minutesAgo(index)}
            </span>
          </div>
        </div>
        <Tabs value={metric} onValueChange={(v) => setMetric(v as Metric)}>
          <TabsList aria-label="Metric">
            {Object.entries(metrics).map(([key, m]) => (
              <TabsTrigger key={key} value={key} className="px-2.5 text-xs">
                {m.label}
              </TabsTrigger>
            ))}
          </TabsList>
        </Tabs>
      </div>
      <div className="mt-4 h-36 flex-1">
        {isPending ? (
          <Skeleton className="h-full w-full" />
        ) : (
          <Sparkline
            values={values}
            onActiveChange={setActive}
            label={`${label} per minute over the last ${WINDOW_MINUTES} minutes; peak ${show(peak)}`}
          />
        )}
      </div>
      <div className="mt-3 flex justify-between text-[11px] text-muted-foreground tabular">
        <span>{WINDOW_MINUTES} min ago</span>
        <span>peak {show(peak)}</span>
        <span>now</span>
      </div>
    </Card>
  );
}
