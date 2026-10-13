import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { Provider, Routes } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { RoutesPage } from "./routes";

const provider = (name: string): Provider => ({
  name,
  vendor: name,
  formats: [],
  kinds: ["chat"],
  base_url: `https://api.${name}.test`,
  credentials: "gateway",
  quirks: [],
  requests: 0,
  errors: 0,
  avg_latency_ms: null,
});

const routes: Routes = {
  serve: [
    { format: "openai-chat", kind: "chat", endpoints: ["POST /v1/chat/completions"] },
    { format: "openai-embeddings", kind: "embedding", endpoints: ["POST /v1/embeddings"] },
  ],
  routes: [
    { prefix: "claude", provider: "anthropic" },
    { prefix: "llama", provider: "groq" },
  ],
  defaults: [
    { kind: "chat", provider: "openai" },
    { kind: "embedding", provider: "openai" },
  ],
};

const api = (body: Routes) =>
  serve({
    "GET /ui/api/routes": () => json(body),
    "GET /ui/api/providers": () =>
      json({ providers: [provider("openai"), provider("anthropic"), provider("groq")] }),
  });

describe("RoutesPage", () => {
  it("lists prefix routes, defaults and served endpoints", async () => {
    api(routes);
    renderApp(<RoutesPage />);
    expect(await screen.findByText("claude*", { selector: "code" })).toBeInTheDocument();
    expect(screen.getByText("llama*", { selector: "code" })).toBeInTheDocument();
    expect(screen.getByText("/v1/chat/completions")).toBeInTheDocument();
    expect(screen.getByText("/v1/embeddings")).toBeInTheDocument();
    expect(screen.getByText("openai-embeddings")).toBeInTheDocument();
  });

  it("explains an empty configuration instead of showing blank tables", async () => {
    api({ serve: [], routes: [], defaults: [] });
    renderApp(<RoutesPage />);
    expect(await screen.findByText(/No overrides/)).toBeInTheDocument();
    expect(screen.getByText(/No default provider/)).toBeInTheDocument();
    expect(screen.getByText(/No endpoints configured/)).toBeInTheDocument();
  });

  it("says the routes could not be loaded, and does not claim there are none", async () => {
    serve({});
    renderApp(<RoutesPage />);
    expect(await screen.findByText("Couldn't load routes")).toBeInTheDocument();
    expect(screen.queryByText(/No overrides/)).not.toBeInTheDocument();
  });

  describe("route tester", () => {
    it("names the rule and provider for an explicit prefix, a route and a default", async () => {
      api(routes);
      renderApp(<RoutesPage />);
      const model = await screen.findByLabelText("Model", { exact: true });

      await userEvent.type(model, "groq/llama-3.3");
      expect(await screen.findByText("provider/ prefix")).toBeInTheDocument();
      expect(screen.getByText(/as llama-3\.3/)).toBeInTheDocument();

      await userEvent.clear(model);
      await userEvent.type(model, "claude-sonnet-4-5");
      expect(await screen.findByText("prefix claude*")).toBeInTheDocument();
      expect(screen.getAllByText("anthropic").length).toBeGreaterThan(0);

      await userEvent.clear(model);
      await userEvent.type(model, "gpt-4o");
      expect(await screen.findByText("default for chat")).toBeInTheDocument();
    });

    it("offers example models from the routes and fills one on a click", async () => {
      api(routes);
      renderApp(<RoutesPage />);
      const examples = await screen.findByRole("group", { name: "Example models" });
      await userEvent.click(within(examples).getByRole("button", { name: "llama*" }));
      expect(screen.getByLabelText("Model", { exact: true })).toHaveValue("llama");
      expect(await screen.findByText("prefix llama*")).toBeInTheDocument();
    });
  });
});
