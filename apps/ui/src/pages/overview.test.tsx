import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Overview, Provider, RequestRecord } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { OverviewPage } from "./overview";

const overview: Overview = {
  version: "1.2.3",
  listen: "127.0.0.1:8080",
  started_at: "2026-10-10T12:00:00.000Z",
  uptime_seconds: 3700,
  auth_required: true,
  config_source: "gateway.yaml",
  totals: { requests: 200, errors: 10, in_flight: 2, input_tokens: 1500, output_tokens: 4200 },
};

const provider: Provider = {
  name: "anthropic",
  vendor: "anthropic",
  formats: ["anthropic-messages"],
  kinds: ["chat"],
  base_url: "https://api.anthropic.com",
  credentials: "gateway",
  quirks: [],
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
  stream: false,
  usage: { input_tokens: 12, output_tokens: 40 },
  error: null,
};

const api = (over: { providers?: Provider[]; requests?: RequestRecord[] } = {}) =>
  serve({
    "GET /ui/api/overview": () => json(overview),
    "GET /ui/api/providers": () => json({ providers: over.providers ?? [provider] }),
    "GET /ui/api/requests": () => json({ requests: over.requests ?? [record] }),
  });

describe("OverviewPage", () => {
  it("shows the totals, the providers and the recent requests", async () => {
    api();
    renderApp(<OverviewPage />);
    expect(await screen.findByText(/v1\.2\.3/)).toBeInTheDocument();
    expect(screen.getByText(/keys required/)).toBeInTheDocument();
    expect(await screen.findByText("5.0%")).toBeInTheDocument();
    expect(screen.getByText("10 errors")).toBeInTheDocument();
    expect(screen.getByText("config: gateway.yaml")).toBeInTheDocument();
    expect(await screen.findByText("1 configured")).toBeInTheDocument();
    expect(screen.getAllByText("anthropic").length).toBeGreaterThan(0);
    expect(await screen.findByText("claude-sonnet-4-5")).toBeInTheDocument();
  });

  it("teaches the first request and how to add a provider when there is nothing yet", async () => {
    api({ providers: [], requests: [] });
    renderApp(<OverviewPage />);
    expect(await screen.findByText("No requests yet")).toBeInTheDocument();
    expect(await screen.findByText(/No providers yet/)).toBeInTheDocument();
    expect(screen.getByText(/curl .*\/v1\/chat\/completions/)).toBeInTheDocument();
  });

  it("says the overview could not be loaded", async () => {
    serve({});
    renderApp(<OverviewPage />);
    expect(await screen.findByText("Couldn't load the overview")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toBeInTheDocument();
  });
});
