import { useNavigate } from "react-router";
import { KindBadge, StatusBadge } from "@/components/request-badges";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useNow } from "@/hooks/use-now";
import type { RequestRecord } from "@/lib/api";
import { formatAgo, formatClock, formatCost, formatCount, formatMs } from "@/lib/format";
import { cn } from "@/lib/utils";

function Tokens({ record }: { record: RequestRecord }) {
  if (!record.usage) return <span className="text-muted-foreground">—</span>;
  const { input_tokens, output_tokens, seconds } = record.usage;
  if (seconds !== undefined && input_tokens + output_tokens === 0) {
    return <span className="tabular">{seconds} s audio</span>;
  }
  return (
    <span className="tabular">
      {formatCount(record.usage.input_tokens)}
      <span className="text-muted-foreground"> → </span>
      {formatCount(record.usage.output_tokens)}
    </span>
  );
}

export function RequestsTable({
  requests,
  loading,
  compact = false,
  selectedId,
}: {
  requests: RequestRecord[];
  loading?: boolean;
  compact?: boolean;
  selectedId?: string;
}) {
  const navigate = useNavigate();
  const now = useNow(5000);
  const open = (id: string) => navigate(`/requests/${encodeURIComponent(id)}`);
  const hideOnCompact = compact ? "hidden" : "hidden md:table-cell";
  // A cost column only where the gateway prices anything.
  const priced = requests.some((r) => r.cost_usd !== undefined);

  return (
    <Table>
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <TableHead className={"hidden w-28 pl-4 sm:table-cell"}>Time</TableHead>
          <TableHead className="pl-4 sm:pl-3">Model</TableHead>
          <TableHead className={hideOnCompact}>Kind</TableHead>
          <TableHead className="hidden sm:table-cell">Provider</TableHead>
          <TableHead>Status</TableHead>
          <TableHead className="text-right">Latency</TableHead>
          <TableHead className={cn("text-right", !priced && "pr-4", hideOnCompact)}>Tokens in → out</TableHead>
          {priced && <TableHead className={cn("pr-4 text-right", hideOnCompact)}>Cost</TableHead>}
        </TableRow>
      </TableHeader>
      <TableBody>
        {loading
          ? Array.from({ length: compact ? 5 : 10 }, (_, i) => (
              <TableRow key={i} className="hover:bg-transparent">
                <TableCell colSpan={priced ? 8 : 7} className="px-4">
                  <Skeleton className="h-5 w-full" />
                </TableCell>
              </TableRow>
            ))
          : requests.map((r) => (
              <TableRow
                key={r.id}
                tabIndex={0}
                aria-label={`Request ${r.id}`}
                data-state={r.id === selectedId ? "selected" : undefined}
                className="cursor-pointer focus-visible:bg-muted/60 focus-visible:outline-none"
                onClick={() => open(r.id)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    open(r.id);
                  }
                }}
              >
                <TableCell className={"hidden pl-4 font-mono text-xs text-muted-foreground tabular sm:table-cell"}>
                  <span title={formatClock(r.started_at)}>
                    {compact ? formatAgo(r.started_at, now) : formatClock(r.started_at)}
                  </span>
                </TableCell>
                <TableCell className={"max-w-44 pl-4 sm:max-w-56 sm:pl-3"}>
                  <div className="truncate font-medium">{r.model ?? r.path}</div>
                  {!compact && (
                    <div className="truncate font-mono text-[11px] text-muted-foreground">
                      {r.method} {r.path}
                      {r.stream && " · stream"}
                    </div>
                  )}
                </TableCell>
                <TableCell className={hideOnCompact}>
                  <KindBadge kind={r.kind} />
                </TableCell>
                <TableCell className="hidden text-muted-foreground sm:table-cell">
                  {r.provider ?? "—"}
                </TableCell>
                <TableCell>
                  <StatusBadge record={r} />
                </TableCell>
                <TableCell className="text-right text-xs tabular">
                  {formatMs(r.duration_ms)}
                </TableCell>
                <TableCell className={cn("text-right text-xs", !priced && "pr-4", hideOnCompact)}>
                  <Tokens record={r} />
                </TableCell>
                {priced && (
                  <TableCell className={cn("pr-4 text-right text-xs tabular", hideOnCompact)}>
                    {r.cost_usd === undefined ? (
                      <span className="text-muted-foreground">—</span>
                    ) : (
                      formatCost(r.cost_usd)
                    )}
                  </TableCell>
                )}
              </TableRow>
            ))}
      </TableBody>
    </Table>
  );
}
