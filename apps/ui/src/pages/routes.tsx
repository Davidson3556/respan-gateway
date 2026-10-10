import { ArrowRight, CornerDownRight, Route as RouteIcon } from "lucide-react";
import type { ReactNode } from "react";
import { PageHeader } from "@/components/page-header";
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
import { useRoutes } from "@/hooks/queries";

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
    <Card className="shadow-none">
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
              <code className="w-fit rounded bg-background px-2 py-1 text-xs ring-1 ring-border">
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
  return <p className="px-6 pb-6 text-sm text-muted-foreground">{children}</p>;
}

export function RoutesPage() {
  const { data, isPending } = useRoutes();
  const loading = (
    <div className="grid gap-2 px-6 pb-6">
      {Array.from({ length: 3 }, (_, i) => (
        <Skeleton key={i} className="h-8 w-full" />
      ))}
    </div>
  );

  return (
    <div className="grid gap-6">
      <PageHeader
        title="Routes"
        description="Which client formats the gateway serves, and where each model goes."
      />
      <Explainer />

      <Card className="gap-4 pb-0 shadow-none">
        <CardHeader>
          <CardTitle className="text-base">Served formats</CardTitle>
          <CardDescription>Tried in order; each recognises its own requests.</CardDescription>
        </CardHeader>
        {isPending ? (
          loading
        ) : data?.serve.length ? (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="pl-6">Format</TableHead>
                <TableHead>Kind</TableHead>
                <TableHead className="pr-6">Endpoints</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.serve.map((s) => (
                <TableRow key={s.format}>
                  <TableCell className="pl-6 font-mono text-[13px]">{s.format}</TableCell>
                  <TableCell>
                    <Badge variant="secondary" className="font-normal">
                      {s.kind}
                    </Badge>
                  </TableCell>
                  <TableCell className="pr-6">
                    <div className="flex flex-wrap gap-1.5">
                      {s.endpoints.map((e) => (
                        <code key={e} className="rounded bg-muted px-1.5 py-0.5 text-xs">
                          {e}
                        </code>
                      ))}
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <Empty>No formats are served, so every request is a 404.</Empty>
        )}
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="gap-4 shadow-none">
          <CardHeader>
            <CardTitle className="text-base">Model prefix routes</CardTitle>
            <CardDescription>First match wins.</CardDescription>
          </CardHeader>
          {isPending ? (
            loading
          ) : data?.routes.length ? (
            <CardContent className="grid gap-2">
              {data.routes.map((r) => (
                <div
                  key={r.prefix}
                  className="flex items-center gap-3 rounded-lg border px-3 py-2 text-sm"
                >
                  <code className="font-mono">{r.prefix}*</code>
                  <ArrowRight className="size-3.5 text-muted-foreground" />
                  <span className="font-medium">{r.provider}</span>
                </div>
              ))}
            </CardContent>
          ) : (
            <Empty>
              No prefix routes. Add them with the <code>models</code> plugin.
            </Empty>
          )}
        </Card>

        <Card className="gap-4 shadow-none">
          <CardHeader>
            <CardTitle className="text-base">Defaults</CardTitle>
            <CardDescription>Where requests go when nothing else matches.</CardDescription>
          </CardHeader>
          {isPending ? (
            loading
          ) : data?.defaults.length ? (
            <CardContent className="grid gap-2">
              {data.defaults.map((d) => (
                <div
                  key={d.kind}
                  className="flex items-center gap-3 rounded-lg border px-3 py-2 text-sm"
                >
                  <Badge variant="secondary" className="font-normal">
                    {d.kind}
                  </Badge>
                  <CornerDownRight className="size-3.5 text-muted-foreground" />
                  <span className="font-medium">{d.provider}</span>
                </div>
              ))}
            </CardContent>
          ) : (
            <Empty>
              <RouteIcon className="mr-1 inline size-3.5" />
              No default provider; unmatched models are refused.
            </Empty>
          )}
        </Card>
      </div>
    </div>
  );
}
