import { useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { createContext, useContext, useMemo } from "react";
import { api, type Overview, type RequestRecord } from "@/lib/api";
import { queryKeys } from "@/hooks/queries";
import { bucketByMinute, type Bucket } from "@/lib/series";

/** How fresh the request list is: streamed, polled, or not yet known. */
export type LiveMode = "connecting" | "live" | "polling";

export const LiveModeContext = createContext<LiveMode>("connecting");

/** The gateway's default history (`request_log.history`), until the overview says the configured one. */
export const REQUEST_LIMIT = 1000;

/** How many requests the gateway keeps, as its overview says. */
export function historyOf(queryClient: QueryClient): number {
  return queryClient.getQueryData<Overview>(queryKeys.overview)?.history ?? REQUEST_LIMIT;
}

export function useLiveMode(): LiveMode {
  return useContext(LiveModeContext);
}

/** Inserts or replaces a record, keeping the list newest first and bounded. */
export function upsertRecord(list: RequestRecord[], record: RequestRecord, limit = REQUEST_LIMIT): RequestRecord[] {
  const index = list.findIndex((r) => r.id === record.id);
  if (index !== -1) {
    const next = list.slice();
    next[index] = record;
    return next;
  }
  const next = [record, ...list];
  if (list.length > 0 && record.started_at < list[0].started_at) {
    next.sort((a, b) => (a.started_at < b.started_at ? 1 : -1));
  }
  return next.slice(0, limit);
}

/**
 * Folds a freshly fetched list into the cached one. The stream may have
 * updated records while the fetch was in flight, so a finished record
 * (status set) is never replaced by an unfinished one, and records the fetch
 * did not see yet are kept.
 */
export function mergeRecords(
  fetched: RequestRecord[],
  cached: RequestRecord[] = [],
  limit = REQUEST_LIMIT,
): RequestRecord[] {
  const byId = new Map(fetched.map((r) => [r.id, r]));
  for (const c of cached) {
    const f = byId.get(c.id);
    if (!f || (f.status === null && c.status !== null)) byId.set(c.id, c);
  }
  return [...byId.values()]
    .sort((a, b) => (a.started_at < b.started_at ? 1 : a.started_at > b.started_at ? -1 : 0))
    .slice(0, limit);
}

/** Recent requests, kept current by the event stream or, failing that, polling. */
export function useRequests() {
  const mode = useLiveMode();
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: queryKeys.requests,
    queryFn: async ({ signal }) => {
      const limit = historyOf(queryClient);
      return mergeRecords(
        await api.requests(limit, signal),
        queryClient.getQueryData<RequestRecord[]>(queryKeys.requests),
        limit,
      );
    },
    refetchInterval: mode === "live" ? false : 4000,
  });
}

/** Per-minute traffic over the last `minutes`, oldest first. */
export function useTraffic(minutes: number, now: number): Bucket[] {
  const { data } = useRequests();
  return useMemo(() => bucketByMinute(data ?? [], minutes, now), [data, minutes, now]);
}
