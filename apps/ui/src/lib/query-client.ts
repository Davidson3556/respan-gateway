import { QueryCache, QueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { ApiError } from "@/lib/api";

/** Short human description of a failed call. */
export function describeError(error: unknown): string {
  if (error instanceof ApiError) return error.message;
  if (error instanceof TypeError) return "The gateway is unreachable. Is it running?";
  return error instanceof Error ? error.message : String(error);
}

export function createQueryClient(): QueryClient {
  return new QueryClient({
    queryCache: new QueryCache({
      onError: (error, query) => {
        // 401s and 403s open the key dialog instead; 404s are handled by the page.
        if (error instanceof ApiError && (error.status === 401 || error.status === 403 || error.status === 404)) return;
        // One toast per distinct failure, so background polling doesn't stack them.
        const message = describeError(error);
        toast.error(message, { id: message, description: `While loading ${query.queryKey[0]}` });
      },
    }),
    defaultOptions: {
      queries: {
        retry: (count, error) =>
          !(error instanceof ApiError && error.status < 500) && count < 2,
        refetchOnWindowFocus: false,
      },
    },
  });
}
