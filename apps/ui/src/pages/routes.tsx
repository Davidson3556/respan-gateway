import { ArrowRight, ChevronDown } from "lucide-react";
import { Link } from "react-router";
import { PageHeader } from "@/components/page-header";
import { ProviderIcon } from "@/components/provider-icon";
import { RouteTester } from "@/components/route-tester";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useProviders, useRoutes } from "@/hooks/queries";
import type { Provider } from "@/lib/api";
import { describeError } from "@/lib/query-client";

function Destination({ name, providers }: { name: string; providers: Provider[] }) {
  const provider = providers.find((p) => p.name === name);
  return (
    <Link to="/providers" className="flex min-w-0 items-center gap-2 text-sm font-medium hover:underline">
      <ProviderIcon name={name} baseUrl={provider?.base_url ?? ""} />
      <span className="truncate">{name}</span>
    </Link>
  );
}

export function RoutesPage() {
  const { data, isPending, error } = useRoutes();
  const { data: providers = [] } = useProviders();
  const kinds = data
    ? [...new Set([...data.serve.map((s) => s.kind), ...data.defaults.map((d) => d.kind)])]
    : [];

  return (
    <div className="grid min-w-0 grid-cols-1 gap-6">
      <PageHeader title="Routes" />
      {error && !data ? (
        <div role="alert" className="text-sm text-destructive">
          <p>Couldn't load routes</p>
          <p>{describeError(error)}</p>
        </div>
      ) : isPending ? (
        <Skeleton className="h-64 w-full" />
      ) : data && (
        <>
          {kinds.length > 0 && <RouteTester routes={data} providers={providers} kinds={kinds} />}

          <div className="grid min-w-0 gap-5 lg:grid-cols-2">
            <Card className="min-w-0 gap-0 overflow-hidden py-0">
              <h2 className="border-b px-5 py-4 text-sm font-semibold">Default destinations</h2>
              {data.defaults.length ? (
                <div className="divide-y">
                  {data.defaults.map((d) => (
                    <div key={d.kind} className="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-4 px-5 py-4">
                      <span className="text-sm capitalize">{d.kind}</span>
                      <ArrowRight className="size-4 text-muted-foreground" aria-hidden />
                      <Destination name={d.provider} providers={providers} />
                    </div>
                  ))}
                </div>
              ) : <p className="p-5 text-sm text-muted-foreground">No default provider</p>}
            </Card>

            <Card className="min-w-0 gap-0 overflow-hidden py-0">
              <div className="flex items-center justify-between border-b px-5 py-4">
                <h2 className="text-sm font-semibold">Model overrides</h2>
                {data.routes.length > 0 && <span className="text-xs text-muted-foreground">First match wins</span>}
              </div>
              {data.routes.length ? (
                <div className="divide-y">
                  {data.routes.map((r) => (
                    <div key={r.prefix} className="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-4 px-5 py-4">
                      <code className="truncate text-sm" title={`Models starting with ${r.prefix}`}>{r.prefix}*</code>
                      <ArrowRight className="size-4 text-muted-foreground" aria-hidden />
                      <Destination name={r.provider} providers={providers} />
                    </div>
                  ))}
                </div>
              ) : <p className="p-5 text-sm text-muted-foreground">No overrides. Using defaults.</p>}
            </Card>
          </div>

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
