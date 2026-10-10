import { useQuery } from "@tanstack/react-query";
import { useSyncExternalStore } from "react";
import { api, getGatewayKey, subscribeGatewayKey, type Kind } from "@/lib/api";

export const queryKeys = {
  overview: ["overview"] as const,
  providers: ["providers"] as const,
  routes: ["routes"] as const,
  requests: ["requests"] as const,
  request: (id: string) => ["request", id] as const,
  models: ["models"] as const,
};

export function useOverview() {
  return useQuery({
    queryKey: queryKeys.overview,
    queryFn: ({ signal }) => api.overview(signal),
    refetchInterval: 5000,
  });
}

export function useProviders() {
  return useQuery({
    queryKey: queryKeys.providers,
    queryFn: ({ signal }) => api.providers(signal),
    refetchInterval: 10_000,
  });
}

export function useRoutes() {
  return useQuery({ queryKey: queryKeys.routes, queryFn: ({ signal }) => api.routes(signal) });
}

/**
 * Models from /v1/models that serve `kind`. `kind` on a model is an optional
 * extension: when the gateway reports none, every model is taken to be a chat
 * model, as /v1/models lists for OpenAI.
 */
export function useModels(kind: Kind) {
  return useQuery({
    queryKey: queryKeys.models,
    queryFn: ({ signal }) => api.models(signal),
    staleTime: 60_000,
    select: (models) =>
      models.some((m) => m.kind !== undefined)
        ? models.filter((m) => m.kind === kind)
        : kind === "chat"
          ? models
          : [],
  });
}

export function useGatewayKey(): string | null {
  return useSyncExternalStore(subscribeGatewayKey, getGatewayKey, () => null);
}
