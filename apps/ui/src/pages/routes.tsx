import { ArrowRight, ChevronDown, Route as RouteIcon } from "lucide-react";
import { Link } from "react-router";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { ProviderIcon } from "@/components/provider-icon";
import { RouteCard } from "@/components/route-card";
import { RouteTester } from "@/components/route-tester";
import { TomlCode } from "@/components/toml-code";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useRoutes } from "@/hooks/queries";
import { describeError } from "@/lib/query-client";
import { EXAMPLE_ROUTES } from "@/lib/routes";

function Destination({ name }: { name: string }) {
  return (
    <Link to="/providers" className="flex min-w-0 items-center gap-2 text-sm font-medium hover:underline">
      <ProviderIcon name={name} />
      <span className="truncate">{name}</span>
    </Link>
  );
}

export function RoutesPage() {
  const { data, isPending, error } = useRoutes(true);
  const kinds = data
    ? [...new Set([...data.serve.map((s) => s.kind), ...data.defaults.map((d) => d.kind)])].filter((k) => k !== "mcp")
    : [];

  return (
    <div className="grid min-w-0 grid-cols-1 gap-6">
      <PageHeader title="Routes"
        description="Where each model a client asks for goes: your routes first, then a provider/ prefix, then the provider whose model list has it, then the kind's default." />
      {error && !data ? (
        <div role="alert" className="text-sm text-destructive">
          <p>Couldn't load routes</p>
          <p>{describeError(error)}</p>
        </div>
      ) : isPending ? (
        <Skeleton className="h-64 w-full" />
      ) : data && (
        <>
          {kinds.length > 0 && <RouteTester routes={data} kinds={kinds} />}

          <section className="grid min-w-0 gap-3" aria-labelledby="your-routes">
            <div className="flex items-baseline justify-between gap-4">
              <h2 id="your-routes" className="text-sm font-semibold">Your routes</h2>
              {data.rules.length > 0 && (
                <span className="text-xs text-muted-foreground">An exact name before a pattern; patterns in order</span>
              )}
            </div>
            {data.rules.length ? (
              data.rules.map((rule) => <RouteCard key={rule.name} rule={rule} />)
            ) : (
              <EmptyState icon={RouteIcon} title="No routes yet">
                <p>Give a model name its own providers: picked in order, by price or by speed, falling back or raced. Add to the config:</p>
                <pre className="w-full overflow-x-auto rounded-lg border bg-muted/40 p-4 text-left text-xs"><TomlCode code={EXAMPLE_ROUTES} /></pre>
              </EmptyState>
            )}
          </section>

          <Card className="min-w-0 gap-0 overflow-hidden py-0">
            <h2 className="border-b px-5 py-4 text-sm font-semibold">Default destinations</h2>
            {data.defaults.length ? (
              <div className="divide-y">
                {data.defaults.map((d) => (
                  <div key={d.kind} className="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-4 px-5 py-3">
                    <span className="text-sm capitalize">{d.kind}</span>
                    <ArrowRight className="size-4 text-muted-foreground" aria-hidden />
                    <Destination name={d.provider} />
                  </div>
                ))}
              </div>
            ) : <p className="p-5 text-sm text-muted-foreground">No default provider</p>}
          </Card>

          <details className="group min-w-0 rounded-xl border bg-card">
            <summary className="flex cursor-pointer list-none items-center justify-between px-5 py-4 text-sm font-semibold [&::-webkit-details-marker]:hidden">
              <span>API endpoints <span className="ml-2 font-normal text-muted-foreground">{data.serve.reduce((n, s) => n + s.endpoints.length, 0)}</span></span>
              <ChevronDown className="size-4 text-muted-foreground transition-transform group-open:rotate-180" />
            </summary>
            {data.serve.length ? (
              <div className="divide-y border-t">
                {data.serve.flatMap((s) => s.endpoints.map((endpoint) => {
                  const [method, path] = endpoint.split(" ");
                  return (
                    <div key={`${s.format}:${endpoint}`} className="flex flex-wrap items-center gap-x-4 gap-y-1 px-5 py-3">
                      <span className="w-12 font-mono text-xs font-semibold text-muted-foreground">{method}</span>
                      <code className="min-w-0 flex-1 break-all text-xs">{path}</code>
                      <span className="text-xs text-muted-foreground">{s.format}</span>
                    </div>
                  );
                }))}
              </div>
            ) : <p className="px-5 pb-4 text-sm text-muted-foreground">No endpoints configured.</p>}
          </details>
        </>
      )}
    </div>
  );
}
