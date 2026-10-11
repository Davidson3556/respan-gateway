import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { formatCost } from "@/lib/format";
import type { RequestRecord } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { OverviewPage } from "./overview";
import { RequestsPage } from "./requests";

const record = (over: Partial<RequestRecord>): RequestRecord => ({
  id: "r_1",
  started_at: "2026-10-10T12:00:00.000Z",
  method: "POST",
  path: "/v1/chat/completions",
  format: "openai-chat",
  kind: "chat",
  model: "gpt-4o-mini",
  provider: "openai",
  status: 200,
  duration_ms: 500,
  ttfb_ms: 100,
  stream: false,
  usage: { input_tokens: 10, output_tokens: 5 },
  error: null,
  ...over,
});

const overview = (cost?: number) => ({
  version: "1",
  listen: null,
  started_at: "2026-10-10T12:00:00.000Z",
  uptime_seconds: 10,
  reloaded_at: null,
  auth_required: false,
  config_source: "file",
  config_path: null,
  totals: { requests: 2, errors: 0, in_flight: 0, input_tokens: 1, output_tokens: 1, cost_usd: cost },
});

beforeAll(() => {
  Element.prototype.hasPointerCapture = () => false;
  Element.prototype.releasePointerCapture = () => {};
  Element.prototype.setPointerCapture = () => {};
});

describe("formatCost", () => {
  it("shows small sums to four places and larger ones to cents", () => {
    expect(formatCost(0)).toBe("$0.00");
    expect(formatCost(0.00004)).toBe("<$0.0001");
    expect(formatCost(0.0042)).toBe("$0.0042");
    expect(formatCost(12.4)).toBe("$12.40");
  });
});

describe("cost", () => {
  it("adds a Cost column when some request has one, with a dash for the unpriced", async () => {
    serve({
      "GET /ui/api/overview": () => json(overview()),
      "GET /ui/api/requests": () =>
        json({ requests: [record({ id: "r_1", cost_usd: 0.0042 }), record({ id: "r_2", model: "llama" })] }),
    });
    renderApp(<RequestsPage />);
    expect(await screen.findByRole("columnheader", { name: "Cost" })).toBeInTheDocument();
    expect(screen.getByText("$0.0042")).toBeInTheDocument();
  });

  it("has no Cost column when nothing is priced", async () => {
    serve({ "GET /ui/api/overview": () => json(overview()), "GET /ui/api/requests": () => json({ requests: [record({})] }) });
    renderApp(<RequestsPage />);
    await screen.findByRole("columnheader", { name: "Latency" });
    expect(screen.queryByRole("columnheader", { name: "Cost" })).not.toBeInTheDocument();
  });

  const quiet = { "GET /ui/api/providers": () => json({ providers: [] }), "GET /ui/api/requests": () => json({ requests: [] }) };

  it("shows a Cost stat on the overview when the gateway prices requests", async () => {
    serve({ ...quiet, "GET /ui/api/overview": () => json(overview(12.4)) });
    renderApp(<OverviewPage />);
    expect(await screen.findByText("$12.40")).toBeInTheDocument();
    expect(screen.getByText("Cost")).toBeInTheDocument();
  });

  it("shows no Cost stat on the overview when it does not", async () => {
    serve({ ...quiet, "GET /ui/api/overview": () => json(overview()) });
    renderApp(<OverviewPage />);
    await screen.findByText("Error rate");
    expect(screen.queryByText("Cost")).not.toBeInTheDocument();
  });
});

describe("Export menu", () => {
  it("offers JSONL and CSV and requests the export with the chosen format", async () => {
    const calls = serve({
      "GET /ui/api/overview": () => json(overview()),
      "GET /ui/api/requests": () => json({ requests: [record({})] }),
      "GET /ui/api/requests/export": () => new Response("id\n"),
    });
    URL.createObjectURL = () => "blob:x";
    URL.revokeObjectURL = () => {};
    renderApp(<RequestsPage />, "/requests?kind=chat&status=error");
    await userEvent.click(await screen.findByRole("button", { name: /export/i }));
    expect(screen.getByRole("menuitem", { name: /jsonl/i })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("menuitem", { name: /csv/i }));
    await vi.waitFor(() => expect(calls.some((c) => c.key === "GET /ui/api/requests/export")).toBe(true));
  });
});
