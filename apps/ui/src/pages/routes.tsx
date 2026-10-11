import type { ReactNode } from "react";
import { PageHeader } from "@/components/page-header";
import { RouteTester } from "@/components/route-tester";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useProviders, useRoutes } from "@/hooks/queries";
import type { Kind, ServedFormat } from "@/lib/api";
import { cn } from "@/lib/utils";

const steps: { title: string; body: ReactNode; example: string }[] = [
  {
    title: "Explicit provider",
    body: "A model named provider/model goes straight to that provider, with the prefix removed.",
    example: "anthropic/claude-sonnet-4-5",
  },
  {
    title: "Prefix routes",
    body: "Otherwise the first route whose prefix the model name starts with picks the provider.",
    example: "claude-sonnet-4-5 → anthropic",
  },
  {
    title: "Default per kind",
    body: "Anything left goes to the default provider for the request's kind. Across formats, the gateway translates.",
    example: "gpt-4o-mini → openai",
  },
];

function Explainer() {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">How routing works</CardTitle>
        <CardDescription>
          Each request is matched by the format it arrives in, then sent to a provider in this
          order.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ol className="grid gap-4 md:grid-cols-3">
          {steps.map((s, i) => (
            <li key={s.title} className="grid content-start gap-2 rounded-lg border bg-muted/30 p-4">
              <div className="flex items-center gap-2">
                <span className="flex size-5 items-center justify-center rounded-full bg-brand text-[11px] font-semibold text-white tabular">
                  {i + 1}
                </span>
                <span className="text-sm font-medium">{s.title}</span>
              </div>
              <p className="text-sm text-muted-foreground">{s.body}</p>
              <code className="w-fit max-w-full rounded bg-background px-2 py-1 text-xs break-all ring-1 ring-border">
                {s.example}
              </code>
            </li>
          ))}
        </ol>
      </CardContent>
    </Card>
  );
}

function Empty({ children }: { children: ReactNode }) {
  return <p className="px-5 pb-5 text-sm text-muted-foreground">{children}</p>;
}

const Loading = () => (
  <div className="grid gap-2 px-5 pb-5">
    {Array.from({ length: 3 }, (_, i) => (
      <Skeleton key={i} className="h-8 w-full" />
    ))}
  </div>
);

function Method({ endpoint }: { endpoint: string }) {
  const [method, path] = endpoint.split(" ");
  return (
    <div className="flex items-baseline gap-2 font-mono text-xs">
      <span
        className={cn(
          "w-11 shrink-0 text-[11px] font-semibold",
          method === "GET" ? "text-success" : method === "DELETE" ? "text-destructive" : "text-brand",
        )}
      >
        {method}
      </span>
      <span className="min-w-0 break-all">{path}</span>
    </div>
  );
}

/** Served formats, a card per kind of traffic with its formats and endpoints. */
function Served({ serve }: { serve: ServedFormat[] }) {
  const kinds = [...new Set(serve.map((s) => s.kind))];
  return (
    <div className="grid gap-4 md:grid-cols-2">
      {kinds.map((kind) => (
        <Card key={kind} className="gap-3">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Badge variant="secondary" className="font-normal">
                {kind}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4">
            {serve
              .filter((s) => s.kind === kind)
              .map((s) => (
                <div key={s.format} className="grid gap-1.5">
                  <span className="font-mono text-[13px] font-medium">{s.format}</span>
                  {s.endpoints.map((e) => (
                    <Method key={e} endpoint={e} />
                  ))}
                </div>
              ))}
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

export function RoutesPage() {
  const { data, isPending } = useRoutes();
  const providers = useProviders();
  const kinds: Kind[] = data
    ? [...new Set([...data.serve.map((s) => s.kind), ...data.defaults.map((d) => d.kind)])]
    : [];

  return (
    <div className="grid grid-cols-1 gap-6">
      <PageHeader
        title="Routes"
        description="Which client formats the gateway serves, and where each model goes."
      />

      {data && providers.data && kinds.length > 0 && (
        <RouteTester routes={data} providers={providers.data} kinds={kinds} />
      )}

      <div className="grid gap-4 lg:grid-cols-[3fr_2fr]">
        <Card className="gap-4 pb-0">
          <CardHeader>
            <CardTitle className="text-base">Model prefix routes</CardTitle>
            <CardDescription>Tried in order; the first match wins.</CardDescription>
          </CardHeader>
          {isPending ? (
            <Loading />
          ) : data?.routes.length ? (
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="w-10 pl-5">#</TableHead>
                  <TableHead>Model starts with</TableHead>
                  <TableHead className="pr-5">Provider</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.routes.map((r, i) => (
                  <TableRow key={r.prefix}>
                    <TableCell className="pl-5 text-muted-foreground tabular">{i + 1}</TableCell>
                    <TableCell className="font-mono text-[13px]">{r.prefix}…</TableCell>
                    <TableCell className="pr-5 font-medium">{r.provider}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <Empty>
              No prefix routes. Add them with the <code>models</code> plugin.
            </Empty>
          )}
        </Card>

        <Card className="gap-4 pb-0">
          <CardHeader>
            <CardTitle className="text-base">Defaults</CardTitle>
            <CardDescription>Where each kind goes when nothing else matches.</CardDescription>
          </CardHeader>
          {isPending ? (
            <Loading />
          ) : data?.defaults.length ? (
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="pl-5">Kind</TableHead>
                  <TableHead className="pr-5">Provider</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.defaults.map((d) => (
                  <TableRow key={d.kind}>
                    <TableCell className="pl-5">
                      <Badge variant="secondary" className="font-normal">
                        {d.kind}
                      </Badge>
                    </TableCell>
                    <TableCell className="pr-5 font-medium">{d.provider}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <Empty>No default provider; unmatched models are refused.</Empty>
          )}
        </Card>
      </div>

      <section className="grid gap-3">
        <div>
          <h2 className="text-base font-semibold">Served endpoints</h2>
          <p className="text-sm text-muted-foreground">
            What clients can call, by kind. Each format recognises its own requests.
          </p>
        </div>
        {isPending ? (
          <Skeleton className="h-40 w-full" />
        ) : data?.serve.length ? (
          <Served serve={data.serve} />
        ) : (
          <Card>
            <Empty>No formats are served, so every request is a 404.</Empty>
          </Card>
        )}
      </section>

      <Explainer />
    </div>
  );
}
