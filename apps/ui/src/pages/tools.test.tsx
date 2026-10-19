import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { McpOverview, McpServer, McpTools } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { ToolsPage } from "./tools";

const github: McpServer = {
  name: "github",
  transport: "http",
  target: "https://api.githubcopilot.com",
  endpoint: "/mcp/github",
  credentials: "gateway",
  auth: "bearer (env GITHUB_TOKEN)",
  status: "ok",
  requests: 10,
  errors: 1,
  avg_latency_ms: 80,
};

const files: McpServer = {
  ...github,
  name: "files",
  transport: "stdio",
  target: "npx",
  endpoint: "/mcp/files",
  credentials: "none",
  auth: "none (a child process)",
  status: "idle",
  requests: 0,
  errors: 0,
  avg_latency_ms: null,
};

const overview: McpOverview = {
  servers: [github, files],
  tool_loop: { servers: ["github"] },
  aggregate: { servers: ["github", "files"], endpoint: "/mcp" },
};

const tools: McpTools = {
  tools: [
    { name: "search_issues", title: null, description: "Searches issues.", read_only: true, destructive: null, tool_loop: true, aggregate: true },
    { name: "delete_repo", title: null, description: null, read_only: null, destructive: true, tool_loop: false, aggregate: true },
  ],
  took_ms: 412,
  asked_at: "2026-10-13T09:30:00Z",
};

describe("ToolsPage", () => {
  it("shows each server, how it signs in and who uses it", async () => {
    serve({ "GET /ui/api/mcp": () => json(overview) });
    renderApp(<ToolsPage />);
    expect(await screen.findByText("/mcp/github")).toBeInTheDocument();
    expect(screen.getByText("bearer (env GITHUB_TOKEN)")).toBeInTheDocument();
    expect(screen.getByText("runs npx")).toBeInTheDocument();
    expect(screen.getByText("idle")).toBeInTheDocument();
    expect(screen.getByText("One endpoint for all")).toBeInTheDocument();
    expect(screen.getByText(/Offers the tools of github/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Its requests →" })).toHaveAttribute("href", "/requests?provider=github");
  });

  it("asks a server for its tools only when they are opened", async () => {
    const calls = serve({
      "GET /ui/api/mcp": () => json(overview),
      "GET /ui/api/mcp/github/tools": () => json(tools),
    });
    renderApp(<ToolsPage />);
    await screen.findByText("/mcp/github");
    expect(calls.some((c) => c.key.includes("/tools"))).toBe(false);
    await userEvent.click(screen.getAllByRole("button", { name: /Tools/ })[0]);
    const list = await screen.findByRole("list", { name: "Tools of github" });
    const [search, remove] = within(list).getAllByRole("listitem");
    expect(search).toHaveTextContent("search_issues");
    expect(search).toHaveTextContent("read-only");
    expect(search).toHaveTextContent("tool loop");
    expect(remove).toHaveTextContent("destructive");
    expect(remove).not.toHaveTextContent("tool loop");
  });

  it("says why a server would not list, and how to log in again", async () => {
    const login = { ...github, auth: "oauth login (.respan/mcp/github.json)" };
    serve({
      "GET /ui/api/mcp": () => json({ ...overview, servers: [login] }),
      "GET /ui/api/mcp/github/tools": () =>
        json({ error: { type: "upstream_error", message: "it refused the gateway's credentials (401 Unauthorized)" } }, 502),
    });
    renderApp(<ToolsPage />);
    await userEvent.click(await screen.findByRole("button", { name: /Tools/ }));
    expect(await screen.findByText(/refused the gateway's credentials/)).toBeInTheDocument();
    expect(screen.getByText("respan-gateway mcp login github")).toBeInTheDocument();
  });

  it("teaches how to add a server when there are none", async () => {
    serve({ "GET /ui/api/mcp": () => json({ servers: [], tool_loop: null, aggregate: null }) });
    renderApp(<ToolsPage />);
    expect(await screen.findByText("No tool servers")).toBeInTheDocument();
  });
});
