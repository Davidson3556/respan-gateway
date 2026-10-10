import { useQuery, useQueryClient } from "@tanstack/react-query";
import { CircleAlert, Ghost } from "lucide-react";
import type { ReactNode } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router";
import { CodeBlock } from "@/components/code-block";
import { CopyButton } from "@/components/copy-button";
import { EmptyState } from "@/components/empty-state";
import { KindBadge, StatusBadge } from "@/components/request-badges";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Skeleton } from "@/components/ui/skeleton";
import { queryKeys } from "@/hooks/queries";
import { ApiError, api, type RequestRecord } from "@/lib/api";
import { formatCount, formatDateTime, formatMs } from "@/lib/format";

function useRequestRecord(id: string) {
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: queryKeys.request(id),
    queryFn: ({ signal }) => api.request(id, signal),
    initialData: () =>
      queryClient.getQueryData<RequestRecord[]>(queryKeys.requests)?.find((r) => r.id === id),
    // Records in flight change; finished ones don't.
    staleTime: (q) => (q.state.data?.status == null ? 0 : Infinity),
  });
}

function Field({ label, children, mono }: { label: string; children: ReactNode; mono?: boolean }) {
  return (
    <div className="grid grid-cols-[8rem_1fr] items-baseline gap-3 py-1.5 text-sm">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={mono ? "min-w-0 font-mono text-[13px] break-all" : "min-w-0"}>
        {children ?? <span className="text-muted-foreground">—</span>}
      </dd>
    </div>
  );
}

function Timing({ record }: { record: RequestRecord }) {
  const { duration_ms: total, ttfb_ms: ttfb } = record;
  if (total == null) {
    return <p className="text-sm text-muted-foreground">Still in flight…</p>;
  }
  const share = ttfb != null && total > 0 ? Math.min(100, (ttfb / total) * 100) : 0;
  return (
    <div className="grid gap-2">
      <div className="flex h-2 overflow-hidden rounded-full bg-muted" aria-hidden="true">
        <div className="bg-brand/50" style={{ width: `${share}%` }} />
        <div className="flex-1 bg-brand" />
      </div>
      <div className="flex justify-between text-xs text-muted-foreground tabular">
        <span>
          <span className="mr-1.5 inline-block size-2 rounded-full bg-brand/50" />
          First byte {formatMs(ttfb)}
        </span>
        <span>
          <span className="mr-1.5 inline-block size-2 rounded-full bg-brand" />
          Total {formatMs(total)}
        </span>
      </div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="grid gap-2">
      <h3 className="text-xs font-medium tracking-wide text-muted-foreground uppercase">{title}</h3>
      {children}
    </section>
  );
}

function Detail({ record }: { record: RequestRecord }) {
  return (
    <div className="grid gap-6 px-4 pb-6">
      {record.error && (
        <div className="flex gap-3 rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm">
          <CircleAlert className="mt-0.5 size-4 shrink-0 text-destructive" />
          <p className="min-w-0 break-words">{record.error}</p>
        </div>
      )}
      <Section title="Timing">
        <Timing record={record} />
      </Section>
      <Section title="Request">
        <dl>
          <Field label="Started">{formatDateTime(record.started_at)}</Field>
          <Field label="Endpoint" mono>
            {record.method} {record.path}
          </Field>
          <Field label="Format" mono>
            {record.format}
          </Field>
          <Field label="Kind">{record.kind && <KindBadge kind={record.kind} />}</Field>
          <Field label="Model" mono>
            {record.model}
          </Field>
          <Field label="Provider">{record.provider}</Field>
          <Field label="Streaming">{record.stream ? "Yes" : "No"}</Field>
        </dl>
      </Section>
      <Section title="Response">
        <dl>
          <Field label="Status">
            <StatusBadge record={record} />
          </Field>
          <Field label="Duration">{formatMs(record.duration_ms)}</Field>
          <Field label="Time to first byte">{formatMs(record.ttfb_ms)}</Field>
          <Field label="Input tokens">
            {record.usage && <span className="tabular">{formatCount(record.usage.input_tokens)}</span>}
          </Field>
          <Field label="Output tokens">
            {record.usage && <span className="tabular">{formatCount(record.usage.output_tokens)}</span>}
          </Field>
        </dl>
      </Section>
      <Separator />
      <Section title="Raw record">
        <CodeBlock code={JSON.stringify(record, null, 2)} />
      </Section>
    </div>
  );
}

export function RequestDetailSheet() {
  const { id = "" } = useParams();
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const { data, error, isPending } = useRequestRecord(id);
  const close = () => navigate({ pathname: "/requests", search: params.toString() });

  return (
    <Sheet open onOpenChange={(open) => !open && close()}>
      <SheetContent className="w-full gap-0 overflow-y-auto sm:max-w-lg">
        <SheetHeader className="pb-4">
          <div className="flex items-center gap-2">
            <SheetTitle className="font-mono text-base">{id}</SheetTitle>
            <CopyButton value={id} label="Copy request id" />
            {data?.stream && <Badge variant="outline">stream</Badge>}
          </div>
          <SheetDescription>
            {data ? (data.model ?? data.path) : "Request details"}
          </SheetDescription>
        </SheetHeader>
        {data ? (
          <Detail record={data} />
        ) : isPending ? (
          <div className="grid gap-3 px-4">
            {Array.from({ length: 6 }, (_, i) => (
              <Skeleton key={i} className="h-6 w-full" />
            ))}
          </div>
        ) : (
          <EmptyState icon={Ghost} title="Request not found" className="mx-4">
            <p>
              {error instanceof ApiError && error.status === 404
                ? "The gateway keeps the last 1,000 requests; this one has been evicted."
                : "It couldn't be loaded from the gateway."}
            </p>
          </EmptyState>
        )}
      </SheetContent>
    </Sheet>
  );
}
