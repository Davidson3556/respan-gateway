import { useQuery } from "@tanstack/react-query";
import { useSyncExternalStore } from "react";
import { api, getGatewayKey, subscribeGatewayKey, type Kind } from "@/lib/api";

export const queryKeys = {
  overview: ["overview"] as const,
  providers: ["providers"] as const,
  mcp: ["mcp"] as const,
  routes: ["routes"] as const,
  requests: ["requests"] as const,
  request: (id: string) => ["request", id] as const,
  models: ["models"] as const,
  config: ["config"] as const,
};

export function useOverview() {
  return useQuery({
    queryKey: queryKeys.overview,
    queryFn: ({ signal }) => api.overview(signal),
    refetchInterval: 5000,
  });
}

export function useConfig() {
  return useQuery({
    queryKey: queryKeys.config,
    queryFn: ({ signal }) => api.config(signal),
  });
}

export function useProviders() {
  return useQuery({
    queryKey: queryKeys.providers,
    queryFn: ({ signal }) => api.providers(signal),
    refetchInterval: 10_000,
  });
}

/** MCP servers; none if the gateway predates them or has none. */
export function useMcpServers() {
  return useQuery({
    queryKey: queryKeys.mcp,
    queryFn: ({ signal }) => api.mcp(signal).catch(() => []),
    refetchInterval: 10_000,
  });
}

/** The routes; `live` refetches them for each candidate's latest record. */
export function useRoutes(live = false) {
  return useQuery({
    queryKey: queryKeys.routes,
    queryFn: ({ signal }) => api.routes(signal),
    refetchInterval: live ? 5000 : false,
  });
}

/** Where a request for `model` of `kind` would go now; idle without a model. */
export function useExplain(model: string, kind: string, client?: string) {
  return useQuery({
    queryKey: ["explain", model, kind, client ?? ""] as const,
    queryFn: ({ signal }) => api.explain({ model, kind, client }, signal),
    enabled: model.length > 0,
    placeholderData: (previous) => previous,
  });
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
