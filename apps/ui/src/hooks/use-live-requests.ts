import { useQuery } from "@tanstack/react-query";
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

/** Recent requests, kept current by the event stream or, failing that, polling. */
export function useRequests() {
  const mode = useLiveMode();
  return useQuery({
    queryKey: queryKeys.requests,
    queryFn: ({ signal }) => api.requests(REQUEST_LIMIT, signal),
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
