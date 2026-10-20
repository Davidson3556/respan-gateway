import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
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
  enabled: true,
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
    { name: "search_issues", title: null, description: "Searches issues.", read_only: true, destructive: null, tool_loop: true, aggregate: true, enabled: true },
    { name: "delete_repo", title: null, description: null, read_only: null, destructive: true, tool_loop: false, aggregate: true, enabled: true },
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
    expect(within(search).getByLabelText("tool loop")).toBeInTheDocument();
    expect(remove).toHaveTextContent("destructive");
    expect(within(remove).queryByLabelText("tool loop")).toBeNull();
    expect(screen.getByText("2 of 2 on")).toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Turn all 2 tools off" })).toBeChecked();
  });

  it("narrows the list to a kind of tool, by the server's hints", async () => {
    serve({
      "GET /ui/api/mcp": () => json(overview),
      "GET /ui/api/mcp/github/tools": () => json(tools),
    });
    renderApp(<ToolsPage />);
    await userEvent.click((await screen.findAllByRole("button", { name: /Tools/ }))[0]);
    const list = await screen.findByRole("list", { name: "Tools of github" });
    await userEvent.click(screen.getByRole("button", { name: /Destructive/ }));
    expect(within(list).getAllByRole("listitem").map((li) => li.textContent)).toEqual([expect.stringContaining("delete_repo")]);
    await userEvent.click(screen.getByRole("button", { name: /Read-only/ }));
    expect(within(list).getAllByRole("listitem").map((li) => li.textContent)).toEqual([expect.stringContaining("search_issues")]);
    expect(screen.getByRole("button", { name: /Off/ })).toBeDisabled();
  });

  it("filters a long list and turns all those shown off at once", async () => {
    const many: McpTools = {
      ...tools,
      tools: Array.from({ length: 12 }, (_, i) => ({ ...tools.tools[0], description: null, name: i < 3 ? `issue_${i}` : `doc_${i}` })),
    };
    const calls = serve({
      "GET /ui/api/mcp": () => json(overview),
      "GET /ui/api/mcp/github/tools": () => json(many),
      ...Object.fromEntries(many.tools.map((t) => [`PUT /ui/api/mcp/github/tools/${t.name}`, () => new Response(null, { status: 204 })])),
    });
    renderApp(<ToolsPage />);
    await userEvent.click((await screen.findAllByRole("button", { name: /Tools/ }))[0]);
    await userEvent.type(await screen.findByRole("textbox", { name: "Filter the tools of github" }), "issue");
    expect(within(screen.getByRole("list", { name: "Tools of github" })).getAllByRole("listitem")).toHaveLength(3);
    const master = screen.getByRole("switch", { name: "Turn the 3 shown tools off" });
    expect(master).toBeChecked();
    await userEvent.click(master);
    const puts = calls.filter((c) => c.key.startsWith("PUT "));
    expect(puts.map((c) => c.key).sort()).toEqual(["issue_0", "issue_1", "issue_2"].map((n) => `PUT /ui/api/mcp/github/tools/${n}`));
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

  it("turns a server off and on, in the gateway's memory", async () => {
    const calls = serve({
      "GET /ui/api/mcp": () => json(overview),
      "PUT /ui/api/mcp/github": () => new Response(null, { status: 204 }),
    });
    renderApp(<ToolsPage />);
    const toggle = await screen.findByRole("switch", { name: "Serve github" });
    expect(toggle).toBeChecked();
    await userEvent.click(toggle);
    const put = calls.find((c) => c.key === "PUT /ui/api/mcp/github");
    expect(put?.init.body).toBe(JSON.stringify({ enabled: false }));
  });

  it("turns one tool off, and puts the switch back if the gateway refuses", async () => {
    const calls = serve({
      "GET /ui/api/mcp": () => json(overview),
      "GET /ui/api/mcp/github/tools": () => json(tools),
      "PUT /ui/api/mcp/github/tools/delete_repo": () =>
        json({ error: { type: "forbidden", message: "an admin key is needed" } }, 403),
    });
    renderApp(<ToolsPage />);
    await userEvent.click((await screen.findAllByRole("button", { name: /Tools/ }))[0]);
    const toggle = await screen.findByRole("switch", { name: "Offer delete_repo" });
    await userEvent.click(toggle);
    const put = calls.find((c) => c.key === "PUT /ui/api/mcp/github/tools/delete_repo");
    expect(put?.init.body).toBe(JSON.stringify({ enabled: false }));
    await vi.waitFor(() => expect(toggle).toBeChecked());
  });

  it("teaches how to add a server when there are none", async () => {
    serve({ "GET /ui/api/mcp": () => json({ servers: [], tool_loop: null, aggregate: null }) });
    renderApp(<ToolsPage />);
    expect(await screen.findByText("No tool servers")).toBeInTheDocument();
  });
});
