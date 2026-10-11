import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { vi } from "vitest";
import { TooltipProvider } from "@/components/ui/tooltip";

/** Renders `ui` with a fresh query client that doesn't retry; returns it too. */
export function renderApp(ui: ReactNode, route = "/") {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const view = render(
    <QueryClientProvider client={client}>
      <TooltipProvider>
        <MemoryRouter initialEntries={[route]}>{ui}</MemoryRouter>
      </TooltipProvider>
    </QueryClientProvider>,
  );
  return { client, ...view };
}

export type Handler = (req: { path: string; init: RequestInit }) => Response | Promise<Response>;

/**
 * Stubs `fetch` with handlers keyed by "METHOD path" (query dropped); an
 * unknown call is a 404. Returns the log of calls made.
 */
export function serve(routes: Record<string, Handler>) {
  const calls: { key: string; init: RequestInit }[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
      const key = `${init.method ?? "GET"} ${String(input).split("?")[0]}`;
      calls.push({ key, init });
      const handler = routes[key];
      return handler
        ? handler({ path: String(input), init })
        : new Response(JSON.stringify({ error: { message: `no route ${key}` } }), { status: 404 });
    }),
  );
  return calls;
}

export const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
