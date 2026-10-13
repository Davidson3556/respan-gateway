import { ArrowRight, Ban } from "lucide-react";
import { useId, useState } from "react";
import { ProviderIcon } from "@/components/provider-icon";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import type { Kind, Provider, Routes } from "@/lib/api";
import { resolveModel, type Resolution } from "@/lib/routing";

function why(r: Exclude<Resolution, { by: "none" }>, kind: Kind): string {
  if (r.by === "explicit") return "provider/ prefix";
  if (r.by === "prefix") return `prefix ${r.prefix}*`;
  return `default for ${kind}`;
}

export function RouteTester({ routes, providers, kinds }: {
  routes: Pick<Routes, "routes" | "defaults">;
  providers: Provider[];
  kinds: Kind[];
}) {
  const modelId = useId();
  const [model, setModel] = useState("");
  const [kind, setKind] = useState<Kind>(kinds.includes("chat") ? "chat" : kinds[0]);
  const result = resolveModel(kind, model, routes, providers);
  const provider = result.by !== "none" ? providers.find((p) => p.name === result.provider) : undefined;

  return (
    <Card className="gap-4">
      <CardHeader className="flex items-center justify-between">
        <CardTitle className="text-sm">Model lookup</CardTitle>
        <span className="text-xs text-muted-foreground" title="Uses configured rules only. No request is sent; provider model discovery and API translation are excluded.">Preview</span>
      </CardHeader>
      <CardContent className="grid min-w-0 gap-4">
        <div className="grid min-w-0 gap-3 sm:grid-cols-[minmax(0,1fr)_10rem]">
          <div className="grid min-w-0 gap-2">
            <Label htmlFor={modelId} className="sr-only">Model</Label>
            <Input id={modelId} value={model} onChange={(e) => setModel(e.target.value)}
              placeholder="Enter a model ID…" autoComplete="off" spellCheck={false} className="font-mono" />
          </div>
          <div className="grid gap-2">
            <Label htmlFor={`${modelId}-kind`} className="sr-only">Request type</Label>
            <Select value={kind} onValueChange={(v) => setKind(v as Kind)}>
              <SelectTrigger id={`${modelId}-kind`} className="w-full"><SelectValue /></SelectTrigger>
              <SelectContent>
                {kinds.map((k) => <SelectItem key={k} value={k}>{k}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
        </div>
        {routes.routes.length > 0 && (
          <div className="flex flex-wrap gap-2" role="group" aria-label="Example models">
            {routes.routes.slice(0, 3).map(({ prefix }) => (
              <button key={prefix} type="button" onClick={() => setModel(prefix)}
                className="rounded border px-2 py-1 font-mono text-xs text-muted-foreground hover:bg-muted focus-visible:ring-2 focus-visible:ring-ring">
                {prefix}*
              </button>
            ))}
          </div>
        )}
        {model.trim() && (
          <div aria-live="polite">
            {result.by === "none" ? (
              <p className="flex items-center gap-2 text-sm text-warning"><Ban className="size-4" />No matching destination</p>
            ) : (
              <div className="flex flex-wrap items-center gap-3 rounded-lg border bg-muted/30 px-4 py-3">
                <ArrowRight className="size-4 text-muted-foreground" aria-hidden />
                <ProviderIcon name={result.provider} baseUrl={provider?.base_url ?? ""} />
                <span className="min-w-0 break-all text-sm font-semibold">{result.provider}</span>
                {result.by === "explicit" && <span className="break-all font-mono text-xs text-muted-foreground">as {result.model}</span>}
                <span className="ml-auto text-xs text-muted-foreground">{why(result, kind)}</span>
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
