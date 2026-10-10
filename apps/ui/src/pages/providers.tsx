import { KeyRound, Server, UserRound } from "lucide-react";
import { CodeBlock } from "@/components/code-block";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useProviders } from "@/hooks/queries";
import type { Provider } from "@/lib/api";
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
      <span className="text-sm font-medium tabular">{value}</span>
    </div>
  );
}

function ProviderCard({ provider: p }: { provider: Provider }) {
  const gatewayKey = p.credentials === "gateway";
  return (
    <Card className="gap-4 shadow-none">
      <CardHeader className="gap-1">
        <div className="flex items-start justify-between gap-3">
          <CardTitle className="truncate text-base">{p.name}</CardTitle>
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
        <p className="truncate font-mono text-xs text-muted-foreground" title={p.base_url}>
          {p.vendor && <span className="text-foreground/70">{p.vendor} · </span>}
          {p.base_url}
        </p>
      </CardHeader>
      <CardContent className="flex flex-1 flex-col gap-4">
        <div className="flex flex-wrap gap-1.5">
          {p.kinds.map((k) => (
            <Badge key={k} className="bg-brand-soft font-normal text-brand">
              {k}
            </Badge>
          ))}
          {p.formats.map((f) => (
            <Badge key={f} variant="secondary" className="font-mono text-[11px] font-normal">
              {f}
            </Badge>
          ))}
        </div>
        {p.quirks.length > 0 && (
          <ul className="grid gap-1.5">
            {p.quirks.map((q) => (
              <li key={q.name} className="text-xs text-muted-foreground">
                <span className="font-mono text-foreground/70">{q.name}</span> · {q.description}
              </li>
            ))}
          </ul>
        )}
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
