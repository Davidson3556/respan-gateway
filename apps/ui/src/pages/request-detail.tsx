import { useQuery, useQueryClient } from "@tanstack/react-query";
import { CircleAlert, Ghost } from "lucide-react";
import type { ReactNode } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router";
import { CodeBlock } from "@/components/code-block";
import { AgentSteps } from "@/components/agent-steps";
import { CopyButton } from "@/components/copy-button";
import { EmptyState } from "@/components/empty-state";
import { KindBadge, StatusBadge } from "@/components/request-badges";
import { Badge } from "@/components/ui/badge";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Skeleton } from "@/components/ui/skeleton";
import { queryKeys, useOverview } from "@/hooks/queries";
import { ApiError, api, type RequestRecord } from "@/lib/api";
import { formatCount, formatDateTime, formatMs, formatPercent } from "@/lib/format";
import { reproduceCurl, timingOf, tokensPerSecond } from "@/lib/requests";

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

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="grid grid-cols-[minmax(0,1fr)] gap-3">
      <h3 className="text-xs font-medium tracking-wide text-muted-foreground uppercase">{title}</h3>
      {children}
    </section>
  );
}

function Field({ label, children, mono }: { label: string; children: ReactNode; mono?: boolean }) {
  return (
    <div className="grid grid-cols-[7rem_1fr] items-baseline gap-3 py-1.5 text-sm">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={mono ? "min-w-0 font-mono text-[13px] break-all" : "min-w-0"}>
        {children ?? <span className="text-muted-foreground">—</span>}
      </dd>
    </div>
  );
}

function Stat({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="grid gap-0.5 rounded-lg border bg-muted/30 px-3 py-2.5">
      <span className="text-[11px] text-muted-foreground">{label}</span>
      <span className="font-mono text-base font-medium tabular">{value}</span>
      {hint && <span className="text-[11px] text-muted-foreground">{hint}</span>}
    </div>
  );
}

/** The first byte against the rest of the response, on one bar. */
function Timing({ record }: { record: RequestRecord }) {
  const timing = timingOf(record);
  if (!timing) return <p className="text-sm text-muted-foreground">Still in flight…</p>;
  const { ttfb, rest, total, ttfbShare } = timing;
  const unmeasured = record.ttfb_ms == null;
  return (
    <div className="grid grid-cols-[minmax(0,1fr)] gap-3">
      <div className="flex items-baseline justify-between">
        <span className="font-mono text-2xl font-semibold tabular">{formatMs(total)}</span>
        <span className="text-xs text-muted-foreground">total</span>
      </div>
      <div
        className="flex h-3 overflow-hidden rounded-full bg-muted"
        role="img"
        aria-label={
          unmeasured
            ? `Total ${formatMs(total)}`
            : `First byte ${formatMs(ttfb)} of ${formatMs(total)} total`
        }
      >
        <div className="bg-brand/45" style={{ width: `${ttfbShare * 100}%` }} />
        <div className="flex-1 bg-brand" />
      </div>
      {!unmeasured && (
        <dl className="grid grid-cols-[repeat(2,minmax(0,1fr))] gap-3 text-xs">
          <div className="flex items-start gap-2">
            <span className="mt-1 size-2 shrink-0 rounded-full bg-brand/45" aria-hidden="true" />
            <div className="min-w-0">
              <dt className="text-muted-foreground">Time to first byte</dt>
              <dd className="font-mono text-sm tabular">
                {formatMs(ttfb)} <span className="text-muted-foreground">{formatPercent(ttfbShare)}</span>
              </dd>
            </div>
          </div>
          <div className="flex items-start gap-2">
            <span className="mt-1 size-2 shrink-0 rounded-full bg-brand" aria-hidden="true" />
            <div className="min-w-0">
              <dt className="text-muted-foreground">{record.stream ? "Streaming the rest" : "Body"}</dt>
              <dd className="font-mono text-sm tabular">
                {formatMs(rest)} <span className="text-muted-foreground">{formatPercent(1 - ttfbShare)}</span>
              </dd>
            </div>
          </div>
        </dl>
      )}
    </div>
  );
}

function Usage({ record }: { record: RequestRecord }) {
  const { usage } = record;
  if (!usage) return <p className="text-sm text-muted-foreground">No usage was reported.</p>;
  const speed = tokensPerSecond(record);
  return (
    <div className="grid grid-cols-[repeat(2,minmax(0,1fr))] gap-2 sm:grid-cols-[repeat(3,minmax(0,1fr))]">
      <Stat label="Input tokens" value={formatCount(usage.input_tokens)} />
      <Stat label="Output tokens" value={formatCount(usage.output_tokens)} />
      {usage.seconds !== undefined && <Stat label="Audio billed" value={`${usage.seconds} s`} />}
      {speed !== null && <Stat label="Output speed" value={`${Math.round(speed)} tok/s`} hint="after first byte" />}
    </div>
  );
}

function Reproduce({ record }: { record: RequestRecord }) {
  const { data } = useOverview();
  const curl = reproduceCurl(record, window.location.origin, data?.auth_required ?? false);
  if (!curl) return null;
  return (
    <Section title="Reproduce">
      <CodeBlock code={curl} />
      <p className="text-xs text-muted-foreground">
        The gateway keeps no request bodies, so the prompt is a stand-in; the model, endpoint and
        streaming match.
      </p>
    </Section>
  );
}

function Detail({ record }: { record: RequestRecord }) {
  return (
    <div className="grid grid-cols-[minmax(0,1fr)] gap-6 px-4 pb-6">
      {record.error && (
        <div role="alert" className="flex gap-3 rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm">
          <CircleAlert className="mt-0.5 size-4 shrink-0 text-destructive" />
          <div className="min-w-0">
            <p className="font-medium">
              {record.status !== null && record.status >= 400 ? `Failed with ${record.status}` : "Reported an error"}
            </p>
            <p className="break-words text-muted-foreground">{record.error}</p>
          </div>
        </div>
      )}
      <Section title="Timing">
        <Timing record={record} />
      </Section>
      <Section title="Usage">
        <Usage record={record} />
      </Section>
      {record.steps && record.steps.length > 0 && (
        <Section title="Agent loop">
          <AgentSteps steps={record.steps} />
        </Section>
      )}
      <Section title="Request">
        <dl>
          <Field label="Status">
            <StatusBadge record={record} />
          </Field>
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
      <Reproduce record={record} />
      <details className="group grid grid-cols-[minmax(0,1fr)] gap-2">
        <summary className="w-fit cursor-pointer rounded text-xs font-medium tracking-wide text-muted-foreground uppercase outline-none hover:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50">
          Raw record
        </summary>
        <CodeBlock code={JSON.stringify(record, null, 2)} />
      </details>
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
      <SheetContent className="w-full gap-0 overflow-y-auto sm:max-w-xl">
        <SheetHeader className="pb-4">
          <div className="flex items-center gap-2 pr-8">
            <SheetTitle className="font-mono text-base">{id}</SheetTitle>
            <CopyButton value={id} label="Copy request id" />
            {data?.stream && <Badge variant="outline">stream</Badge>}
          </div>
          <SheetDescription>{data ? (data.model ?? data.path) : "Request details"}</SheetDescription>
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
