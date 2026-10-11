import { KeyRound, Server, TriangleAlert, UserRound } from "lucide-react";
import type { ReactNode } from "react";
import { CodeBlock } from "@/components/code-block";
import { CopyButton } from "@/components/copy-button";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useProviders } from "@/hooks/queries";
import type { Provider, Quirk } from "@/lib/api";
import { formatCount, formatMs, formatPercent } from "@/lib/format";

const exampleConfig = `providers:
  openai:
    format: openai-chat
    base_url: https://api.openai.com/v1
    auth: { type: bearer, key: env:OPENAI_API_KEY }`;

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="grid gap-0.5">
      <span className="text-[11px] text-muted-foreground">{label}</span>
      <span className="font-mono text-sm font-medium tabular">{value}</span>
    </div>
  );
}

function Chips({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="grid gap-1.5">
      <span className="text-[11px] font-medium tracking-wide text-muted-foreground uppercase">{label}</span>
      <div className="flex flex-wrap gap-1.5">{children}</div>
    </div>
  );
}

/** A description with its `backticked` parts set as code. */
function Prose({ text }: { text: string }) {
  return text.split("`").map((part, i) =>
    i % 2 ? (
      <code key={i} className="rounded bg-muted px-1 py-0.5 text-[11px] text-foreground">
        {part}
      </code>
    ) : (
      part
    ),
  );
}

function Quirks({ quirks }: { quirks: Quirk[] }) {
  return (
    <div className="grid gap-2">
      <span className="flex items-center gap-1.5 text-[11px] font-medium tracking-wide text-warning uppercase">
        <TriangleAlert className="size-3" aria-hidden="true" />
        Quirks
      </span>
      <ul className="grid gap-2">
        {quirks.map((q) => (
          <li key={q.name} className="rounded-md border-l-2 border-warning bg-warning/10 py-2 pr-3 pl-3">
            <div className="font-mono text-xs font-medium">{q.name}</div>
            <p className="mt-0.5 text-xs leading-relaxed text-muted-foreground">
              <Prose text={q.description} />
            </p>
          </li>
        ))}
      </ul>
    </div>
  );
}

function ProviderCard({ provider: p }: { provider: Provider }) {
  const gatewayKey = p.credentials === "gateway";
  return (
    <Card className="min-w-0 gap-5">
      <CardHeader className="gap-1">
        <div className="flex items-start justify-between gap-3">
          <div className="flex min-w-0 items-center gap-2.5">
            <span
              aria-hidden="true"
              className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-brand-soft text-sm font-semibold text-brand uppercase"
            >
              {p.name[0]}
            </span>
            <div className="min-w-0">
              <CardTitle className="truncate text-base">{p.name}</CardTitle>
              {p.vendor && p.vendor !== p.name && (
                <div className="truncate text-xs text-muted-foreground">{p.vendor}</div>
              )}
            </div>
          </div>
          <Badge
            variant="outline"
            className="shrink-0 font-normal text-muted-foreground"
            title={
              gatewayKey
                ? "The gateway sends its own configured key"
                : "Each client's own credentials pass through"
            }
          >
            {gatewayKey ? <KeyRound /> : <UserRound />}
            {gatewayKey ? "Gateway key" : "Client key"}
          </Badge>
        </div>
        <div className="flex min-w-0 items-center gap-1 pt-1">
          <p className="truncate font-mono text-xs text-muted-foreground" title={p.base_url}>
            {p.base_url}
          </p>
          {p.base_url && (
            <CopyButton value={p.base_url} label="Copy base URL" className="size-6 shrink-0" />
          )}
        </div>
      </CardHeader>
      <CardContent className="flex flex-1 flex-col gap-4">
        <Chips label="Serves">
          {p.kinds.map((k) => (
            <Badge key={k} className="bg-brand-soft font-normal text-brand">
              {k}
            </Badge>
          ))}
        </Chips>
        <Chips label="Formats">
          {p.formats.map((f) => (
            <Badge key={f} variant="secondary" className="font-mono text-[11px] font-normal">
              {f}
            </Badge>
          ))}
        </Chips>
        {p.quirks.length > 0 && <Quirks quirks={p.quirks} />}
        <div className="mt-auto grid grid-cols-3 gap-3 border-t pt-4">
          <Metric label="Requests" value={formatCount(p.requests)} />
          <Metric label="Errors" value={p.requests ? formatPercent(p.errors / p.requests) : "—"} />
          <Metric label="Avg latency" value={formatMs(p.avg_latency_ms)} />
        </div>
      </CardContent>
    </Card>
  );
}

export function ProvidersPage() {
  const { data, isPending } = useProviders();
  return (
    <div className="grid gap-6">
      <PageHeader
        title="Providers"
        description="Upstreams the gateway can call, what they speak and how they're doing."
      />
      {isPending ? (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {Array.from({ length: 3 }, (_, i) => (
            <Skeleton key={i} className="h-52 rounded-xl" />
          ))}
        </div>
      ) : data?.length ? (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {data.map((p) => (
            <ProviderCard key={p.name} provider={p} />
          ))}
        </div>
      ) : (
        <EmptyState icon={Server} title="No providers configured">
          <p>Add a provider to gateway.yaml and restart the gateway:</p>
          <CodeBlock className="w-full text-left" code={exampleConfig} />
        </EmptyState>
      )}
    </div>
  );
}
