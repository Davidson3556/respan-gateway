import { ArrowRight, Ban } from "lucide-react";
import { useId, useState, type ReactNode } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import type { Kind, Provider, Routes } from "@/lib/api";
import { resolveModel, type Resolution } from "@/lib/routing";

function Step({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="grid min-w-0 gap-1">
      <span className="text-[11px] font-medium tracking-wide text-muted-foreground uppercase">{label}</span>
      <span className="truncate rounded-md border bg-muted/40 px-2.5 py-1.5 font-mono text-sm">{children}</span>
    </div>
  );
}

function why(r: Exclude<Resolution, { by: "none" }>, kind: Kind): string {
  if (r.by === "explicit") return "provider/ prefix";
  if (r.by === "prefix") return `prefix ${r.prefix}*`;
  return `default for ${kind}`;
}

/** Type a model name, see which provider the routes above would send it to. */
export function RouteTester({
  routes,
  providers,
  kinds,
}: {
  routes: Pick<Routes, "routes" | "defaults">;
  providers: Provider[];
  kinds: Kind[];
}) {
  const modelId = useId();
  const [model, setModel] = useState("");
  const [kind, setKind] = useState<Kind>(kinds.includes("chat") ? "chat" : kinds[0]);
  const result = resolveModel(kind, model, routes, providers);
  const examples = routes.routes.slice(0, 3).map((r) => `${r.prefix}-example`);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Where would this model go?</CardTitle>
        <CardDescription>
          Resolved here from the tables below. A provider's own model claims and format translation
          aren't visible to this page.
        </CardDescription>
      </CardHeader>
      <CardContent className="grid gap-4">
        <div className="grid gap-3 sm:grid-cols-[1fr_11rem]">
          <div className="grid gap-2">
            <Label htmlFor={modelId}>Model</Label>
            <Input
              id={modelId}
              value={model}
              onChange={(e) => setModel(e.target.value)}
              placeholder="claude-sonnet-4-5 or openai/gpt-4o"
              autoComplete="off"
              spellCheck={false}
              className="font-mono"
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor={`${modelId}-kind`}>Kind</Label>
            <Select value={kind} onValueChange={(v) => setKind(v as Kind)}>
              <SelectTrigger id={`${modelId}-kind`} className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {kinds.map((k) => (
                  <SelectItem key={k} value={k}>
                    {k}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
        {examples.length > 0 && (
          <div className="flex flex-wrap items-center gap-1.5" role="group" aria-label="Example models">
            <span className="text-xs text-muted-foreground">Try</span>
            {examples.map((e) => (
              <button
                key={e}
                type="button"
                onClick={() => setModel(e)}
                className="rounded-full border px-2.5 py-0.5 font-mono text-xs text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50"
              >
                {e}
              </button>
            ))}
          </div>
        )}
        <div aria-live="polite">
          {result.by === "none" ? (
            <p className="flex items-center gap-2 rounded-lg border border-warning/40 bg-warning/10 p-3 text-sm">
              <Ban className="size-4 shrink-0 text-warning" aria-hidden="true" />
              No provider serves {kind}, so the gateway answers 404.
            </p>
          ) : (
            <div className="grid items-end gap-3 rounded-lg border p-3 sm:grid-cols-[1fr_auto_1fr_auto_1fr]">
              <Step label="Model">{model.trim() || "(none given)"}</Step>
              <ArrowRight className="mb-2 hidden size-4 text-muted-foreground sm:block" aria-hidden="true" />
              <Step label="Rule">{why(result, kind)}</Step>
              <ArrowRight className="mb-2 hidden size-4 text-muted-foreground sm:block" aria-hidden="true" />
              <Step label="Provider">
                <span className="font-semibold text-brand">{result.provider}</span>
                {result.by === "explicit" && <span className="text-muted-foreground"> as {result.model}</span>}
              </Step>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
