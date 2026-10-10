import { Badge } from "@/components/ui/badge";
import type { RequestRecord } from "@/lib/api";
import { outcomeOf } from "@/lib/requests";
import { cn } from "@/lib/utils";

export function StatusBadge({ record }: { record: RequestRecord }) {
  const outcome = outcomeOf(record);
  const tone =
    outcome === "pending"
      ? "bg-brand"
      : outcome === "ok"
        ? "bg-success"
        : record.status !== null && record.status < 500
          ? "bg-warning"
          : "bg-destructive";
  return (
    <span className="inline-flex items-center gap-1.5 font-mono text-xs tabular">
      <span className="relative flex size-2">
        {outcome === "pending" && (
          <span className={cn("absolute inset-0 animate-ping rounded-full opacity-60", tone)} />
        )}
        <span className={cn("relative size-2 rounded-full", tone)} />
      </span>
      {record.status ?? "…"}
    </span>
  );
}

export function KindBadge({ kind }: { kind: string | null }) {
  if (!kind) return <span className="text-muted-foreground">—</span>;
  return (
    <Badge variant="secondary" className="font-normal">
      {kind}
    </Badge>
  );
}
