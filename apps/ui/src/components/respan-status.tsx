import type { RespanHealth } from "@/lib/api";
import { formatAgo, formatCount } from "@/lib/format";
import { cn } from "@/lib/utils";

/** How sending traces to Respan goes, when its plugin is on. */
export function RespanStatus({ health }: { health: RespanHealth }) {
  const lost = health.failed + health.rejected;
  if (health.failing) {
    return (
      <div role="alert" className="flex flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border border-destructive/30 bg-destructive/5 px-4 py-3 text-sm">
        <span className="size-2 rounded-full bg-destructive" aria-hidden />
        <span className="font-medium">Traces aren't reaching Respan</span>
        <span className="min-w-0 text-muted-foreground">{health.error?.message}</span>
        <span className="ml-auto text-xs text-muted-foreground">
          {formatCount(health.failed)} spans lost{health.ok_at && ` · last sent ${formatAgo(health.ok_at)}`}
        </span>
      </div>
    );
  }
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border px-4 py-2.5 text-sm">
      <span className={cn("size-2 rounded-full", health.ok_at ? "bg-success" : "bg-muted-foreground")} aria-hidden />
      <span className="font-medium">Traces to Respan</span>
      <span className="text-muted-foreground">
        {health.ok_at ? `${formatCount(health.sent)} spans sent · last ${formatAgo(health.ok_at)}` : "none sent yet"}
      </span>
      {(lost > 0 || health.dropped > 0) && (
        <span className="text-warning" title={health.error?.message}>
          {lost > 0 && `${formatCount(lost)} spans not taken`}
          {lost > 0 && health.dropped > 0 && " · "}
          {health.dropped > 0 && `${formatCount(health.dropped)} traces dropped`}
        </span>
      )}
    </div>
  );
}
