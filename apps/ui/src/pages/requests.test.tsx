import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { RequestRecord } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { RequestsPage } from "./requests";

const record: RequestRecord = {
  id: "r_1",
  started_at: "2026-10-10T12:00:00.000Z",
  method: "POST",
  path: "/v1/chat/completions",
  format: "openai-chat",
  kind: "chat",
  model: "model-a",
  provider: "openai",
  status: 200,
  duration_ms: 100,
  ttfb_ms: 50,
  stream: false,
  usage: null,
  error: null,
};

describe("RequestsPage clients", () => {
  it("shows a Client column and a client filter when records carry clients", async () => {
    serve({
      "GET /ui/api/requests": () =>
        json({ requests: [{ ...record, client: "ci" }, { ...record, id: "r_2", model: "model-b", client: "web" }] }),
    });
    renderApp(<RequestsPage />);
    expect(await screen.findByText("model-b")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Client" })).toBeInTheDocument();
    expect(screen.getByText("ci")).toBeInTheDocument();
    expect(screen.getByLabelText("Client")).toBeInTheDocument();
  });

  it("filters by the client in the URL", async () => {
    serve({
      "GET /ui/api/requests": () =>
        json({ requests: [{ ...record, client: "ci" }, { ...record, id: "r_2", model: "model-b", client: "web" }] }),
    });
    renderApp(<RequestsPage />, "/?client=web");
    expect(await screen.findByText("model-b")).toBeInTheDocument();
    expect(screen.queryByText("model-a")).not.toBeInTheDocument();
  });

  it("leaves the column and filter out without client keys", async () => {
    serve({ "GET /ui/api/requests": () => json({ requests: [record] }) });
    renderApp(<RequestsPage />);
    expect(await screen.findByText("model-a")).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Client" })).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Client")).not.toBeInTheDocument();
  });
});
