import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { Provider, RequestRecord } from "@/lib/api";
import { ProvidersPage } from "./providers";
import { RequestsPage } from "./requests";

function serve(routes: Record<string, unknown>) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input).split("?")[0];
      return path in routes
        ? new Response(JSON.stringify(routes[path]), { status: 200 })
        : new Response("{}", { status: 404 });
    }),
  );
}

function renderPage(page: React.ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <TooltipProvider>
        <MemoryRouter>{page}</MemoryRouter>
      </TooltipProvider>
    </QueryClientProvider>,
  );
}

const provider: Provider = {
  name: "anthropic",
  vendor: "anthropic",
  formats: ["anthropic-messages"],
  kinds: ["chat"],
  base_url: "https://api.anthropic.com",
  credentials: "client",
  requests: 40,
  errors: 2,
  avg_latency_ms: 1250,
};

const record: RequestRecord = {
  id: "r_000001",
  started_at: "2026-10-10T12:00:00.000Z",
  method: "POST",
  path: "/v1/chat/completions",
  format: "openai-chat",
  kind: "chat",
  model: "claude-sonnet-4-5",
  provider: "anthropic",
  status: 200,
  duration_ms: 812,
  ttfb_ms: 230,
  stream: true,
  usage: { input_tokens: 12, output_tokens: 40 },
  error: null,
};

describe("ProvidersPage", () => {
  it("renders each provider with its formats, credentials and stats", async () => {
    serve({ "/ui/api/providers": { providers: [provider] } });
    renderPage(<ProvidersPage />);
    expect(await screen.findByText("anthropic-messages")).toBeInTheDocument();
    expect(screen.getByText("Client key")).toBeInTheDocument();
    expect(screen.getByText("5.0%")).toBeInTheDocument();
    expect(screen.getByText("1.25 s")).toBeInTheDocument();
  });

  it("teaches how to add a provider when none are configured", async () => {
    serve({ "/ui/api/providers": { providers: [] } });
    renderPage(<ProvidersPage />);
    expect(await screen.findByText("No providers configured")).toBeInTheDocument();
  });
});

describe("RequestsPage", () => {
  it("lists requests from the admin API", async () => {
    serve({ "/ui/api/requests": { requests: [record] } });
    renderPage(<RequestsPage />);
    expect(await screen.findByText("claude-sonnet-4-5")).toBeInTheDocument();
    expect(screen.getByText("812 ms")).toBeInTheDocument();
  });

  it("shows a copyable first request when the gateway has seen none", async () => {
    serve({ "/ui/api/requests": { requests: [] } });
    renderPage(<RequestsPage />);
    expect(await screen.findByText("No requests yet")).toBeInTheDocument();
    expect(screen.getByText(/curl .*\/v1\/chat\/completions/)).toBeInTheDocument();
  });

  it("shows an error, not an empty filter result, when loading fails", async () => {
    serve({});
    renderPage(<RequestsPage />);
    expect(await screen.findByText("Couldn't load requests")).toBeInTheDocument();
    expect(screen.queryByText("No requests match")).not.toBeInTheDocument();
  });
});
