import { Ban, CircleSlash } from "lucide-react";
import { useDeferredValue, useId, useState } from "react";
import { ProviderIcon } from "@/components/provider-icon";
import { PickBadge, Seen } from "@/components/route-card";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useExplain } from "@/hooks/queries";
import type { Explained, Routes } from "@/lib/api";
import { describeError } from "@/lib/query-client";
import { formatPrice } from "@/lib/routes";

const ANY = "any client";

function why(e: Explained): string {
  switch (e.by?.type) {
    case "rule": return `route ${e.by.name}`;
    case "named": return "provider/ prefix";
    case "listed": return "the provider's model list";
    case "default": return "the kind's default";
    default: return "";
  }
}

const MODES = {
  single: "one try",
  fallback: "in this order until one answers",
  hedge: "raced, the first to answer wins",
};

/** Models worth trying: the routes' names. */
function examples(routes: Routes): string[] {
  const named = routes.rules.map((r) => (r.glob ? r.name.replace(/\*/g, "x") : r.name));
  return [...new Set(named)].slice(0, 5);
}

export function RouteTester({ routes, kinds }: {
  routes: Routes;
  kinds: string[];
}) {
  const id = useId();
  const [model, setModel] = useState("");
  const [kind, setKind] = useState(kinds.includes("chat") ? "chat" : kinds[0]);
  const [client, setClient] = useState(ANY);
  const clients = [...new Set(routes.rules.flatMap((r) => r.clients))];
  const asked = useDeferredValue(model.trim());
  const { data, error } = useExplain(asked, kind, client === ANY ? undefined : client);

  return (
    <Card className="gap-4">
      <CardHeader className="flex items-center justify-between">
        <CardTitle className="text-sm">Try a model</CardTitle>
        <span className="text-xs text-muted-foreground" title="Asks the router where a request would go now, with each candidate's price and record. Nothing is sent to a provider.">
          Nothing is sent
        </span>
      </CardHeader>
      <CardContent className="grid min-w-0 gap-4">
        <div className={`grid min-w-0 gap-3 ${clients.length ? "sm:grid-cols-[minmax(0,1fr)_9rem_9rem]" : "sm:grid-cols-[minmax(0,1fr)_10rem]"}`}>
          <div className="grid min-w-0 gap-2">
            <Label htmlFor={id} className="sr-only">Model</Label>
            <Input id={id} value={model} onChange={(e) => setModel(e.target.value)}
              placeholder="A model clients ask for…" autoComplete="off" spellCheck={false} className="font-mono" />
          </div>
          <div className="grid gap-2">
            <Label htmlFor={`${id}-kind`} className="sr-only">Request type</Label>
            <Select value={kind} onValueChange={setKind}>
              <SelectTrigger id={`${id}-kind`} className="w-full"><SelectValue /></SelectTrigger>
              <SelectContent>
                {kinds.map((k) => <SelectItem key={k} value={k}>{k}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
          {clients.length > 0 && (
            <div className="grid gap-2">
              <Label htmlFor={`${id}-client`} className="sr-only">Client key</Label>
              <Select value={client} onValueChange={setClient}>
                <SelectTrigger id={`${id}-client`} className="w-full"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {[ANY, ...clients].map((c) => <SelectItem key={c} value={c}>{c}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
          )}
        </div>
        {examples(routes).length > 0 && (
          <div className="flex flex-wrap gap-2" role="group" aria-label="Example models">
            {examples(routes).map((name) => (
              <button key={name} type="button" onClick={() => setModel(name)}
                className="rounded border px-2 py-1 font-mono text-xs text-muted-foreground hover:bg-muted focus-visible:ring-2 focus-visible:ring-ring">
                {name}
              </button>
            ))}
          </div>
        )}
        {asked && (
          <div aria-live="polite" className="grid gap-3">
            {error ? (
              <p role="alert" className="text-sm text-destructive">{describeError(error)}</p>
            ) : !data ? null : !data.by ? (
              <p className="flex items-center gap-2 text-sm text-warning"><Ban className="size-4" />No provider serves {kind} requests</p>
            ) : (
              <>
                <p className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                  <span>By <span className="font-medium text-foreground">{why(data)}</span></span>
                  {data.pick && <PickBadge pick={data.pick} />}
                  {data.mode && (data.to?.length ?? 0) > 1 && <span>· {MODES[data.mode]}</span>}
                </p>
                <ol className="divide-y rounded-lg border bg-muted/30" aria-label="Where it goes">
                  {data.to?.map((c, i) => {
                    return (
                      <li key={`${c.provider}/${c.model}`} className="flex flex-wrap items-center gap-x-3 gap-y-1 px-4 py-3 text-sm">
                        <span className="w-4 font-mono text-xs text-muted-foreground">{i + 1}</span>
                        <ProviderIcon name={c.provider} />
                        <span className="font-semibold">{c.provider}</span>
                        {c.model && <code className="min-w-0 break-all text-xs text-muted-foreground">{c.model}</code>}
                        <span className="ml-auto flex flex-wrap items-center gap-3 text-xs">
                          {c.price && <span className="text-muted-foreground" title="Per million tokens: input / output">{formatPrice(c.price)}</span>}
                          <Seen seen={c.seen} />
                        </span>
                      </li>
                    );
                  })}
                </ol>
                {(data.passed?.length ?? 0) > 0 && (
                  <ul className="grid gap-1 text-xs text-muted-foreground" aria-label="Passed over">
                    {data.passed?.map((p) => (
                      <li key={`${p.candidate}:${p.why}`} className="flex items-center gap-2">
                        <CircleSlash className="size-3" aria-hidden />
                        <code>{p.candidate}</code> {p.why}
                      </li>
                    ))}
                  </ul>
                )}
              </>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
