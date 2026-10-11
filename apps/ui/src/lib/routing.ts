import type { Kind, Provider, Routes } from "@/lib/api";

export type Resolution =
  | { by: "explicit"; provider: string; model: string }
  | { by: "prefix"; provider: string; model: string; prefix: string }
  | { by: "default"; provider: string; model: string }
  | { by: "none" };

/**
 * Where a request of `kind` for `model` goes, as the router decides it:
 * `provider/model` names a configured provider that serves the kind, else
 * the first prefix route whose provider serves it, else the kind's default.
 * It reads the admin API's tables alone, so it can't see what a provider
 * claims itself or how formats translate.
 */
export function resolveModel(
  kind: Kind,
  model: string,
  routes: Pick<Routes, "routes" | "defaults">,
  providers: Pick<Provider, "name" | "kinds">[],
): Resolution {
  const serves = (name: string) => providers.some((p) => p.name === name && p.kinds.includes(kind));
  const name = model.trim();
  const slash = name.indexOf("/");
  if (slash > 0 && serves(name.slice(0, slash))) {
    return { by: "explicit", provider: name.slice(0, slash), model: name.slice(slash + 1) };
  }
  const route = name && routes.routes.find((r) => name.startsWith(r.prefix) && serves(r.provider));
  if (route) return { by: "prefix", provider: route.provider, model: name, prefix: route.prefix };
  const fallback = routes.defaults.find((d) => d.kind === kind);
  return fallback ? { by: "default", provider: fallback.provider, model: name } : { by: "none" };
}
