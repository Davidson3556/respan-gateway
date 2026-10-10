import { useQuery, useQueryClient } from "@tanstack/react-query";
import { createContext, useContext, useMemo } from "react";
import { api, type RequestRecord } from "@/lib/api";
import { queryKeys } from "@/hooks/queries";

/** How fresh the request list is: streamed, polled, or not yet known. */
export type LiveMode = "connecting" | "live" | "polling";

export const LiveModeContext = createContext<LiveMode>("connecting");

/** Mirrors the gateway's ring buffer. */
export const REQUEST_LIMIT = 1000;

export function useLiveMode(): LiveMode {
  return useContext(LiveModeContext);
}

/** Inserts or replaces a record, keeping the list newest first and bounded. */
export function upsertRecord(list: RequestRecord[], record: RequestRecord): RequestRecord[] {
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
  return next.slice(0, REQUEST_LIMIT);
}

/**
 * Folds a freshly fetched list into the cached one. The stream may have
 * updated records while the fetch was in flight, so a finished record
 * (status set) is never replaced by an unfinished one, and records the fetch
 * did not see yet are kept.
 */
export function mergeRecords(fetched: RequestRecord[], cached: RequestRecord[] = []): RequestRecord[] {
  const byId = new Map(fetched.map((r) => [r.id, r]));
  for (const c of cached) {
    const f = byId.get(c.id);
    if (!f || (f.status === null && c.status !== null)) byId.set(c.id, c);
  }
  return [...byId.values()]
    .sort((a, b) => (a.started_at < b.started_at ? 1 : a.started_at > b.started_at ? -1 : 0))
    .slice(0, REQUEST_LIMIT);
}

/** Recent requests, kept current by the event stream or, failing that, polling. */
export function useRequests() {
  const mode = useLiveMode();
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: queryKeys.requests,
    queryFn: async ({ signal }) =>
      mergeRecords(
        await api.requests(REQUEST_LIMIT, signal),
        queryClient.getQueryData<RequestRecord[]>(queryKeys.requests),
      ),
    refetchInterval: mode === "live" ? false : 4000,
  });
}

/** Requests started per minute over the last `minutes`, oldest first. */
export function useRequestsPerMinute(minutes: number, now: number): number[] {
  const { data } = useRequests();
  return useMemo(() => {
    const buckets = new Array<number>(minutes).fill(0);
    const end = Math.floor(now / 60_000);
    for (const r of data ?? []) {
      const age = end - Math.floor(new Date(r.started_at).getTime() / 60_000);
      if (age >= 0 && age < minutes) buckets[minutes - 1 - age] += 1;
    }
    return buckets;
  }, [data, minutes, now]);
}
