import type { Price, RoutePick, RouteRule } from "@/lib/api";

export const PICKS: Record<RoutePick, { label: string; hint: string }> = {
  first: { label: "In order", hint: "The first that can serve it; the rest as listed" },
  cheapest: { label: "Cheapest", hint: "By price: input plus three times output, per million tokens" },
  fastest: { label: "Fastest", hint: "By the gateway's own record of each one's time to respond" },
  weighted: { label: "Weighted", hint: "At random, by weight" },
  random: { label: "Random", hint: "At random" },
};

/** "$0.15 / $0.6 per M" */
export function formatPrice(price: Price | null | undefined): string {
  if (!price) return "—";
  if (price.per_second != null && !price.input && !price.output) {
    return `$${+(price.per_second * 60).toFixed(4)} / min`;
  }
  return `$${+price.input.toFixed(3)} / $${+price.output.toFixed(3)}`;
}

/** What happens when its first pick fails: "falls back", "hedged 200 ms", "no fallback". */
export function attempts(rule: RouteRule): string {
  const tries = rule.tries > 1 ? ` · ${rule.tries} tries each` : "";
  if (rule.hedge_ms != null) return `raced, ${rule.hedge_ms} ms apart${tries}`;
  if (rule.fallback && rule.to.length > 1) return `falls back in turn${tries}`;
  return `${rule.tries > 1 ? `${rule.tries} tries` : "one try"}, no fallback`;
}

/** Each candidate's share of a weighted route, from 0 to 1. */
export function shares(rule: RouteRule): number[] {
  const total = rule.to.reduce((n, c) => n + c.weight, 0);
  return rule.to.map((c) => (total ? c.weight / total : 0));
}

/** A route's TOML, as a starting point. */
export const EXAMPLE_ROUTES = `[routes.smart]
to = ["anthropic/claude-sonnet-4-5", "openai/gpt-4.1"]
pick = "cheapest"

[routes."llama-*"]
to = "groq"`;
