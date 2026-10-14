import { ArrowDownUp, Dices, Gauge, ListOrdered, PiggyBank, Scale, Snowflake } from "lucide-react";
import { ProviderIcon } from "@/components/provider-icon";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import type { Provider, RouteCandidate, RoutePick, RouteRule, RouteSeen } from "@/lib/api";
import { formatCount, formatMs, formatPercent } from "@/lib/format";
import { PICKS, attempts, formatPrice, shares } from "@/lib/routes";

const PICK_ICONS: Record<RoutePick, typeof ListOrdered> = {
  first: ListOrdered,
  cheapest: PiggyBank,
  fastest: Gauge,
  weighted: Scale,
  random: Dices,
};

export function PickBadge({ pick }: { pick: RoutePick }) {
  const Icon = PICK_ICONS[pick];
  return (
    <Badge variant="secondary" title={PICKS[pick].hint}>
      <Icon aria-hidden />
      {PICKS[pick].label}
    </Badge>
  );
}

/** A candidate's record: latency and errors, or that it has none yet. */
export function Seen({ seen }: { seen: RouteSeen | null }) {
  if (!seen) return <span className="text-muted-foreground">no calls yet</span>;
  return (
    <span className="flex flex-wrap items-center gap-x-3 gap-y-1">
      <span title="Time to a response's head, a moving average">{formatMs(seen.latency_ms)}</span>
      <span
        className={seen.error_rate > 0.2 ? "text-destructive" : "text-muted-foreground"}
        title={`${formatCount(seen.failures)} of ${formatCount(seen.calls)} calls failed`}
      >
        {formatPercent(seen.error_rate)} errors
      </span>
      {seen.cooling && (
        <Badge variant="outline" className="text-warning" title="Failed its last few calls: tried last for a while">
          <Snowflake aria-hidden />
          cooling
        </Badge>
      )}
    </span>
  );
}

/** The model a glob's candidate sends, `{model}` the one asked for; null if sent as asked. */
function templated(target: string): string | null {
  const slash = target.indexOf("/");
  return slash > 0 ? target.slice(slash + 1) : null;
}

function Candidate({ candidate, share, rank, providers }: {
  candidate: RouteCandidate;
  share: number | null;
  rank: number;
  providers: Provider[];
}) {
  const provider = providers.find((p) => p.name === candidate.provider);
  return (
    <li className="grid grid-cols-[1.5rem_minmax(0,1fr)] items-start gap-x-3 gap-y-1 px-5 py-3 text-sm md:grid-cols-[1.5rem_minmax(0,1.4fr)_minmax(0,1fr)_minmax(0,1.2fr)] md:items-center">
      <span className="pt-0.5 font-mono text-xs text-muted-foreground md:pt-0">{rank}</span>
      <span className="flex min-w-0 items-center gap-2">
        <ProviderIcon name={candidate.provider} baseUrl={provider?.base_url ?? ""} />
        <span className="font-medium">{candidate.provider}</span>
        <code className="min-w-0 truncate text-xs text-muted-foreground" title={candidate.target}>
          {candidate.model ?? templated(candidate.target) ?? "as asked"}
        </code>
        {share != null && <span className="ml-auto text-xs text-muted-foreground md:ml-0">{formatPercent(share)}</span>}
      </span>
      <span className="col-start-2 text-xs text-muted-foreground md:col-start-auto" title="Per million tokens: input / output">
        {formatPrice(candidate.price)}
      </span>
      <span className="col-start-2 text-xs md:col-start-auto">
        <Seen seen={candidate.seen} />
      </span>
    </li>
  );
}

/** One `[routes.<name>]`: what it matches, how it picks, and each candidate. */
export function RouteCard({ rule, providers }: { rule: RouteRule; providers: Provider[] }) {
  const weights = rule.pick === "weighted" ? shares(rule) : null;
  const when = [
    ...(rule.kinds.length ? [`${rule.kinds.join(", ")} only`] : []),
    ...(rule.clients.length ? [`for ${rule.clients.join(", ")}`] : []),
  ];
  return (
    <Card className="min-w-0 gap-0 overflow-hidden py-0">
      <div className="flex flex-wrap items-center gap-2 border-b px-5 py-4">
        <code className="text-sm font-semibold">{rule.name}</code>
        {rule.glob && <Badge variant="outline" title="Matches every model this pattern does">pattern</Badge>}
        <PickBadge pick={rule.pick} />
        <span className="flex items-center gap-1 text-xs text-muted-foreground">
          <ArrowDownUp className="size-3" aria-hidden />
          {attempts(rule)}
        </span>
        {rule.needs.map((need) => (
          <Badge key={need} variant="outline" title="Candidates whose listing says they lack it are passed over">needs {need}</Badge>
        ))}
        {when.length > 0 && <span className="ml-auto text-xs text-muted-foreground">{when.join(" · ")}</span>}
      </div>
      <ol className="divide-y" aria-label={`Candidates of ${rule.name}`}>
        {rule.to.map((candidate, i) => (
          <Candidate key={candidate.target} candidate={candidate} rank={i + 1}
            share={weights ? weights[i] : null} providers={providers} />
        ))}
      </ol>
    </Card>
  );
}
