import {
  AudioLines,
  Boxes,
  CalendarClock,
  FileText,
  FlaskConical,
  ImageIcon,
  Route,
  Search,
  SearchX,
  Server,
  Video,
  X,
} from "lucide-react";
import { Link, useSearchParams } from "react-router";
import { CopyButton } from "@/components/copy-button";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { ProviderIcon } from "@/components/provider-icon";
import { KindBadge } from "@/components/request-badges";
import { FilterSelect } from "@/components/request-filters";
import { StatCard } from "@/components/stat-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useAllModels } from "@/hooks/queries";
import type { Model } from "@/lib/api";
import { formatDate } from "@/lib/format";
import {
  distinct,
  filterModels,
  playgroundTab,
  retirement,
  SOON_DAYS,
  sorts,
  summarize,
  traits,
  type ModelFilter,
  type Sort,
  type Trait,
} from "@/lib/model-list";
import { tokensShort } from "@/lib/models";
import { describeError } from "@/lib/query-client";
import { ANY } from "@/lib/requests";
import { formatPrice } from "@/lib/routes";
import { cn } from "@/lib/utils";


const reads = [
  { value: "image", label: "Images", icon: ImageIcon },
  { value: "audio", label: "Audio", icon: AudioLines },
  { value: "video", label: "Video", icon: Video },
  { value: "document", label: "Documents", icon: FileText },
] as const;

const can = { tools: "tools", reasoning: "reasoning", structured_outputs: "structured" } as const;

/** The filter in the URL, so a view can be linked. */
function useFilter() {
  const [params, setParams] = useSearchParams();
  const known = new Set<string>(traits.map((t) => t.value));
  const filter: ModelFilter = {
    q: params.get("q") ?? "",
    provider: params.get("provider"),
    kind: params.get("kind"),
    traits: (params.get("traits") ?? "").split(",").filter((t): t is Trait => known.has(t)),
    sort: (sorts.some((s) => s.value === params.get("sort")) ? params.get("sort") : "name") as Sort,
  };
  const set = (name: string, value: string | null) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (value && value !== ANY) next.set(name, value);
        else next.delete(name);
        return next;
      },
      { replace: true },
    );
  const toggle = (trait: Trait) => {
    const on = filter.traits.includes(trait);
    const next = on ? filter.traits.filter((t) => t !== trait) : [...filter.traits, trait];
    set("traits", next.join(",") || null);
  };
  const clear = () => setParams({}, { replace: true });
  return { filter, set, toggle, clear };
}

function Retires({ model }: { model: Model }) {
  const when = retirement(model);
  if (!when || !model.retires_at) return <span className="text-muted-foreground">—</span>;
  const date = formatDate(model.retires_at);
  return (
    <span
      className={cn("whitespace-nowrap", when === "past" && "text-destructive", when === "soon" && "text-warning")}
      title={when === "past" ? "Retired" : when === "soon" ? "Retiring soon" : undefined}
    >
      {date}
    </span>
  );
}

function ModelRow({ model: m }: { model: Model }) {
  const tab = m.kind && playgroundTab[m.kind];
  const tryIt = tab && `/playground?${new URLSearchParams({ tab, kind: m.kind!, model: m.id })}`;
  return (
    <TableRow>
      <TableCell className="max-w-80 pl-4">
        <div className="flex min-w-0 items-center gap-2">
          <code className="truncate text-[13px]" title={m.id}>
            {m.id}
          </code>
          {m.route && (
            <Badge variant="secondary" className="shrink-0 font-normal">
              route
            </Badge>
          )}
        </div>
        {(m.display_name || m.route) && (
          <div className="truncate text-xs text-muted-foreground" title={m.description}>
            {m.route ? `${m.route.pick}: ${m.route.to.join(", ")}` : m.display_name}
          </div>
        )}
      </TableCell>
      <TableCell>
        <span className="flex items-center gap-1.5">
          {!m.route && <ProviderIcon name={m.owned_by} size="sm" />}
          <span className="truncate">{m.owned_by}</span>
          {m.maker && m.maker !== m.owned_by && (
            <span className="truncate text-xs text-muted-foreground">by {m.maker}</span>
          )}
        </span>
      </TableCell>
      <TableCell className="hidden md:table-cell">
        <KindBadge kind={m.kind ?? null} />
      </TableCell>
      <TableCell className="hidden text-right tabular-nums lg:table-cell">
        {m.context_window ? tokensShort(m.context_window) : "—"}
        {m.max_output_tokens && (
          <span className="text-muted-foreground"> / {tokensShort(m.max_output_tokens)}</span>
        )}
      </TableCell>
      <TableCell className="hidden text-right whitespace-nowrap tabular-nums sm:table-cell">
        <span title="Per million tokens: input / output">
          {formatPrice(m.pricing)}
        </span>
      </TableCell>
      <TableCell className="hidden xl:table-cell">
        <span className="flex gap-1.5 text-muted-foreground">
          {reads
            .filter((r) => m.input_modalities?.includes(r.value))
            .map(({ value, label, icon: Icon }) => (
              <Icon key={value} className="size-4" aria-label={label} role="img" />
            ))}
        </span>
      </TableCell>
      <TableCell className="hidden xl:table-cell">
        <span className="flex gap-1">
          {m.capabilities?.map((c) => (
            <Badge key={c} variant="outline" className="font-normal">
              {can[c]}
            </Badge>
          ))}
        </span>
      </TableCell>
      <TableCell className="hidden lg:table-cell">
        <Retires model={m} />
      </TableCell>
      <TableCell className="pr-4">
        <span className="flex justify-end gap-0.5">
          <CopyButton value={m.id} label={`Copy ${m.id}`} className="size-7" />
          {tryIt && (
            <Button asChild variant="ghost" size="icon" className="size-7">
              <Link to={tryIt} aria-label={`Try ${m.id} in the playground`} title="Try in the playground">
                <FlaskConical />
              </Link>
            </Button>
          )}
        </span>
      </TableCell>
    </TableRow>
  );
}

export function ModelsPage() {
  const { data, isPending, error } = useAllModels();
  const { filter, set, toggle, clear } = useFilter();
  const models = data ?? [];
  const shown = filterModels(models, filter);
  const counts = summarize(models);
  const filtered = filter.q || filter.provider || filter.kind || filter.traits.length > 0;

  return (
    <div className="grid min-w-0 gap-6">
      <PageHeader
        title="Models"
        description="Every model the gateway serves, as clients name it: from each provider's model list, and the config's routes."
      />
      {error && !data ? (
        <div role="alert" className="text-sm text-destructive">
          <p>Couldn't load models</p>
          <p>{describeError(error)}</p>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <StatCard icon={Boxes} label="Models" value={data && counts.models.toLocaleString()} />
            <StatCard icon={Server} label="Providers" value={data && counts.providers.toLocaleString()} />
            <StatCard icon={Route} label="Routes" value={data && counts.routes.toLocaleString()} />
            <StatCard
              icon={CalendarClock}
              label="Retiring soon"
              value={data && counts.retiring.toLocaleString()}
              hint={`within ${SOON_DAYS} days`}
            />
          </div>
          <Card className="min-w-0 gap-0 overflow-hidden py-0">
            <div className="grid gap-3 border-b p-3">
              <div className="grid grid-cols-2 gap-2 sm:flex sm:flex-wrap sm:items-center">
                <div className="relative col-span-2 sm:max-w-xs sm:min-w-56 sm:flex-1">
                  <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    type="search"
                    value={filter.q}
                    onChange={(e) => set("q", e.target.value)}
                    placeholder="Search model or provider"
                    aria-label="Search model or provider"
                    className="h-8 pl-8"
                  />
                </div>
                <FilterSelect label="Kind" value={filter.kind ?? ANY} options={distinct(models, (m) => m.kind)} onChange={(v) => set("kind", v)} />
                <FilterSelect label="Provider" value={filter.provider ?? ANY} options={distinct(models, (m) => m.owned_by)} onChange={(v) => set("provider", v)} />
                <Select value={filter.sort} onValueChange={(v) => set("sort", v === "name" ? null : v)}>
                  <SelectTrigger size="sm" className="w-full sm:w-44" aria-label="Sort">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {sorts.map((s) => (
                      <SelectItem key={s.value} value={s.value}>
                        Sort: {s.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <span className="col-span-2 text-xs text-muted-foreground tabular sm:ml-auto" aria-live="polite">
                  {shown.length.toLocaleString()} of {models.length.toLocaleString()}
                </span>
              </div>
              <div className="flex flex-wrap items-center gap-1.5" role="group" aria-label="Only models that can">
                {traits.map((t) => {
                  const on = filter.traits.includes(t.value);
                  return (
                    <button
                      key={t.value}
                      type="button"
                      aria-pressed={on}
                      onClick={() => toggle(t.value)}
                      className={cn(
                        "rounded-full border px-2.5 py-0.5 text-xs transition-colors outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50",
                        on ? "border-brand/40 bg-brand-soft text-brand" : "text-muted-foreground hover:bg-muted hover:text-foreground",
                      )}
                    >
                      {t.label}
                    </button>
                  );
                })}
                {filtered && (
                  <Button variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={clear}>
                    <X /> Clear
                  </Button>
                )}
              </div>
            </div>
            {isPending ? (
              <div className="grid gap-2 p-4">
                {Array.from({ length: 6 }, (_, i) => (
                  <Skeleton key={i} className="h-9" />
                ))}
              </div>
            ) : !models.length ? (
              <EmptyState icon={Boxes} title="No models listed" className="border-none">
                <p>
                  No provider here lists its models. Add one on the <Link to="/providers" className="underline">Providers</Link> page's config, or name a route.
                </p>
              </EmptyState>
            ) : !shown.length ? (
              <EmptyState icon={SearchX} title="No models match" className="border-none">
                <Button variant="outline" size="sm" onClick={clear}>
                  Clear filters
                </Button>
              </EmptyState>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="pl-4">Model</TableHead>
                    <TableHead>Provider</TableHead>
                    <TableHead className="hidden md:table-cell">Kind</TableHead>
                    <TableHead className="hidden text-right lg:table-cell" title="Input / output tokens at most">
                      Context
                    </TableHead>
                    <TableHead className="hidden text-right sm:table-cell" title="US dollars per million tokens: input / output">
                      Price / M
                    </TableHead>
                    <TableHead className="hidden xl:table-cell">Reads</TableHead>
                    <TableHead className="hidden xl:table-cell">Can</TableHead>
                    <TableHead className="hidden lg:table-cell">Retires</TableHead>
                    <TableHead className="pr-4">
                      <span className="sr-only">Actions</span>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {shown.map((m) => (
                    <ModelRow key={`${m.owned_by}/${m.id}`} model={m} />
                  ))}
                </TableBody>
              </Table>
            )}
          </Card>
        </>
      )}
    </div>
  );
}
