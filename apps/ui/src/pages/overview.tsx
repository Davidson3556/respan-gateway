import {
  Activity,
  AlertTriangle,
  ArrowDownToLine,
  ArrowRight,
  ArrowUpFromLine,
  Clock,
  DollarSign,
  Loader,
} from "lucide-react";
import { Link } from "react-router";
import { NoRequests } from "@/components/no-requests";
import { PageHeader } from "@/components/page-header";
import { RequestsTable } from "@/components/requests-table";
import { RespanStatus } from "@/components/respan-status";
import { StatCard } from "@/components/stat-card";
import { TrafficChart } from "@/components/traffic-chart";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useOverview, useProviders } from "@/hooks/queries";
import { useRequests } from "@/hooks/use-live-requests";
import { formatCost, formatCount, formatMs, formatPercent, formatUptime } from "@/lib/format";
import { cn } from "@/lib/utils";
import { describeError } from "@/lib/query-client";

function Stats() {
  const { data } = useOverview();
  const t = data?.totals;
  return (
    <div
      className={cn(
        "grid grid-cols-2 gap-3 md:grid-cols-3",
        t?.cost_usd !== undefined ? "xl:grid-cols-7" : "xl:grid-cols-6",
      )}
    >
      <StatCard label="Requests" icon={Activity} value={t && formatCount(t.requests)} hint="since start" />
      <StatCard
        label="Error rate"
        icon={AlertTriangle}
        value={t && formatPercent(t.requests ? t.errors / t.requests : 0)}
        hint={t && `${formatCount(t.errors)} errors`}
      />
      <StatCard label="Tokens in" icon={ArrowDownToLine} value={t && formatCount(t.input_tokens)} hint="prompt" />
      <StatCard label="Tokens out" icon={ArrowUpFromLine} value={t && formatCount(t.output_tokens)} hint="completion" />
      {t?.cost_usd !== undefined && (
        <StatCard label="Cost" icon={DollarSign} value={formatCost(t.cost_usd)} hint="since start" />
      )}
      <StatCard label="In flight" icon={Loader} value={t && formatCount(t.in_flight)} hint="right now" />
      <StatCard
        label="Uptime"
        icon={Clock}
        value={data && formatUptime(data.uptime_seconds)}
        hint={data && `config: ${data.config_path?.split(/[\\/]/).pop() ?? data.config_source}`}
      />
    </div>
  );
}

function ProvidersSummary() {
  const { data, isPending, error } = useProviders();
  return (
    <Card className="gap-4">
      <CardHeader className="flex flex-row items-center justify-between">
        <div className="grid gap-1.5">
          <CardTitle className="text-base">Providers</CardTitle>
          <CardDescription>
            {data ? `${data.length} configured` : error ? `Couldn't load: ${describeError(error)}` : "Loading…"}
          </CardDescription>
        </div>
        <Button variant="ghost" size="sm" asChild>
          <Link to="/providers">
            All <ArrowRight />
          </Link>
        </Button>
      </CardHeader>
      <CardContent className="grid gap-1">
        {isPending &&
          Array.from({ length: 3 }, (_, i) => <Skeleton key={i} className="h-10 w-full" />)}
        {data?.length === 0 && (
          <p className="text-sm text-muted-foreground">
            No providers yet. Add one to <code className="text-xs">gateway.toml</code>.
          </p>
        )}
        {data?.slice(0, 6).map((p) => (
          <div key={p.name} className="flex items-center gap-3 rounded-md py-2">
            <div className="min-w-0 flex-1">
              <div className="truncate text-sm font-medium">{p.name}</div>
              <div className="flex flex-wrap gap-1 pt-1">
                {p.kinds.map((k) => (
                  <Badge key={k} variant="secondary" className="px-1.5 py-0 text-[11px] font-normal">
                    {k}
                  </Badge>
                ))}
              </div>
            </div>
            <div className="text-right text-xs tabular">
              <div>{formatCount(p.requests)} req</div>
              <div className="text-muted-foreground">{formatMs(p.avg_latency_ms)}</div>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function RecentRequests() {
  const { data, isPending } = useRequests();
  if (data?.length === 0) return <NoRequests />;
  return (
    <Card className="gap-0 overflow-hidden py-0">
      <div className="flex items-center justify-between border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Recent requests</h2>
        <Button variant="ghost" size="sm" asChild>
          <Link to="/requests">
            View all <ArrowRight />
          </Link>
        </Button>
      </div>
      <RequestsTable requests={data?.slice(0, 8) ?? []} loading={isPending} compact />
    </Card>
  );
}

export function OverviewPage() {
  const { data, isError, error } = useOverview();
  return (
    <div className="grid gap-6">
      {isError && !data && (
        <div
          role="alert"
          className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm"
        >
          <p className="font-medium">Couldn't load the overview</p>
          <p className="text-muted-foreground">{describeError(error)}</p>
        </div>
      )}
      <PageHeader
        title="Overview"
        description={
          data ? (
            <>
              Gateway{" "}
              {data.listen && <span className="font-mono">{data.listen} · </span>}v{data.version}
              {data.auth_required && " · keys required"}
            </>
          ) : (
            "Traffic through this gateway at a glance."
          )
        }
      />
      <Stats />
      {data?.respan && <RespanStatus health={data.respan} />}
      <div className="grid gap-4 lg:grid-cols-3">
        <TrafficChart className="lg:col-span-2" />
        <ProvidersSummary />
      </div>
      <RecentRequests />
    </div>
  );
}
