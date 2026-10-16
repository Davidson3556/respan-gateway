import type { Model } from "@/lib/api";
import { formatPrice } from "@/lib/format";

/** A model to choose: its ID, the provider that lists it, and what its list says of it, if known. */
export type Listed = Pick<Model, "id"> &
  Partial<Pick<Model, "owned_by" | "display_name" | "context_window" | "pricing" | "route">>;

/** Tokens as a person reads them: `8K`, `1M`. */
export function tokensShort(n: number): string {
  if (n >= 1_000_000) return `${+(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${Math.round(n / 1_000)}K`;
  return String(n);
}

/** What a picker shows beside a model's ID: its name, context and price, where known. */
export function modelFacts(m: Listed): string {
  const facts: string[] = [];
  if (m.route) {
    const [first] = m.route.to;
    const more = m.route.to.length - 1;
    facts.push(more > 0 ? `${m.route.pick}: ${first} +${more}` : `→ ${first}`);
  }
  if (m.display_name && m.display_name !== m.id) facts.push(m.display_name);
  if (m.context_window) facts.push(`${tokensShort(m.context_window)} ctx`);
  if (m.pricing) facts.push(formatPrice(m.pricing, " per M"));
  return facts.join(" · ");
}

/** `models` by provider, providers in the order they are first listed. */
export function byProvider<M extends Listed>(models: M[]): [string, M[]][] {
  const groups = new Map<string, M[]>();
  for (const m of models) {
    const provider = m.owned_by ?? "";
    groups.set(provider, [...(groups.get(provider) ?? []), m]);
  }
  return [...groups];
}
