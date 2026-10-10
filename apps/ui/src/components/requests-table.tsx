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
import { formatAgo, formatClock, formatCount, formatMs } from "@/lib/format";
import { cn } from "@/lib/utils";

function Tokens({ record }: { record: RequestRecord }) {
  if (!record.usage) return <span className="text-muted-foreground">—</span>;
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

  return (
    <Table>
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <TableHead className="w-28 pl-4">Time</TableHead>
          <TableHead>Model</TableHead>
          <TableHead className={hideOnCompact}>Kind</TableHead>
          <TableHead className="hidden sm:table-cell">Provider</TableHead>
          <TableHead>Status</TableHead>
          <TableHead className="text-right">Latency</TableHead>
          <TableHead className={cn("pr-4 text-right", hideOnCompact)}>Tokens in → out</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {loading
          ? Array.from({ length: compact ? 5 : 10 }, (_, i) => (
              <TableRow key={i} className="hover:bg-transparent">
                <TableCell colSpan={7} className="px-4">
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
                <TableCell className="pl-4 font-mono text-xs text-muted-foreground tabular">
                  <span title={formatClock(r.started_at)}>
                    {compact ? formatAgo(r.started_at, now) : formatClock(r.started_at)}
                  </span>
                </TableCell>
                <TableCell className="max-w-40 sm:max-w-56">
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
                <TableCell className={cn("pr-4 text-right text-xs", hideOnCompact)}>
                  <Tokens record={r} />
                </TableCell>
              </TableRow>
            ))}
      </TableBody>
    </Table>
  );
}
