import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState, type ReactNode } from "react";
import { gatewayFetch, type RequestRecord } from "@/lib/api";
import { readSse } from "@/lib/sse";
import { queryKeys, useGatewayKey } from "@/hooks/queries";
import { LiveModeContext, upsertRecord, type LiveMode } from "@/hooks/use-live-requests";

/**
 * Holds one connection to /ui/api/events for the whole app and folds each
 * record into the query cache. While it is down, useRequests() polls, and the
 * stream is retried with backoff.
 */
export function LiveProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const key = useGatewayKey();
  const [mode, setMode] = useState<LiveMode>("connecting");

  useEffect(() => {
    let stopped = false;
    let attempt = 0;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let refresh: ReturnType<typeof setTimeout> | undefined;
    const controller = new AbortController();

    // Totals and provider counts change when a request finishes; refresh them
    // at most once a second rather than on every event.
    const refreshStats = () => {
      refresh ??= setTimeout(() => {
        refresh = undefined;
        void queryClient.invalidateQueries({ queryKey: queryKeys.overview });
        void queryClient.invalidateQueries({ queryKey: queryKeys.providers });
      }, 1000);
    };

    const apply = (record: RequestRecord) => {
      queryClient.setQueryData<RequestRecord[]>(queryKeys.requests, (old) =>
        upsertRecord(old ?? [], record),
      );
      queryClient.setQueryData(queryKeys.request(record.id), record);
      refreshStats();
    };

    const connect = async () => {
      try {
        const res = await gatewayFetch("/ui/api/events", {
          signal: controller.signal,
          headers: { accept: "text/event-stream" },
        });
        if (!res.body) throw new Error("event stream has no body");
        attempt = 0;
        setMode("live");
        // Catch up on anything that happened before the stream opened.
        void queryClient.invalidateQueries({ queryKey: queryKeys.requests });
        await readSse(res.body, (event) => {
          if (event.event !== "request") return;
          try {
            apply(JSON.parse(event.data) as RequestRecord);
          } catch {
            // A malformed event is skipped; the next poll or event repairs it.
          }
        });
      } catch {
        // Fall through to polling and a retry.
      }
      if (stopped) return;
      setMode("polling");
      attempt += 1;
      retry = setTimeout(connect, Math.min(30_000, 1000 * 2 ** attempt));
    };

    void connect();
    return () => {
      stopped = true;
      controller.abort();
      clearTimeout(retry);
      clearTimeout(refresh);
    };
  }, [queryClient, key]);

  return <LiveModeContext.Provider value={mode}>{children}</LiveModeContext.Provider>;
}
