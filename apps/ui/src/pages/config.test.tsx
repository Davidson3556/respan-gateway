import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConfigView } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { ConfigPage } from "./config";

const view: ConfigView = {
  source: "file",
  path: "/etc/respan/gateway.yaml",
  writes_enabled: true,
  writes_disabled_reason: null,
  placeholder: "…",
  yaml: "client_keys:\n- …\nproviders:\n  openai:\n    vendor: openai\n    key: env:OPENAI_API_KEY\n",
};

const reloaded = { reloaded_at: "2026-10-11T09:30:00.000Z", restart_required: [] };

describe("ConfigPage", () => {
  it("shows the redacted config and the file it came from", async () => {
    serve({ "GET /ui/api/config": () => json({ ...view, writes_enabled: false, writes_disabled_reason: "admin_writes is not set in the config" }) });
    renderApp(<ConfigPage />);
    expect(await screen.findByText(/env:OPENAI_API_KEY/)).toBeInTheDocument();
    expect(screen.getByText("/etc/respan/gateway.yaml")).toBeInTheDocument();
    expect(screen.getByText(/Editing is off: admin_writes is not set/)).toBeInTheDocument();
    expect(screen.queryByLabelText("Config YAML")).not.toBeInTheDocument();
  });

  it("reloads and shows the result, or the error", async () => {
    let fail = false;
    serve({
      "GET /ui/api/config": () => json(view),
      "POST /ui/api/reload": () =>
        fail
          ? json({ error: { message: "providers: bad", type: "invalid_config" } }, 422)
          : json({ ...reloaded, restart_required: ["listen moved"] }),
    });
    renderApp(<ConfigPage />);
    await userEvent.click(await screen.findByRole("button", { name: "Reload" }));
    expect(await screen.findByText(/Reloaded at/)).toBeInTheDocument();
    expect(screen.getByText(/Restart required: listen moved/)).toBeInTheDocument();
    fail = true;
    await userEvent.click(screen.getByRole("button", { name: "Reload" }));
    expect(await screen.findByText(/Reload failed: providers: bad/)).toBeInTheDocument();
  });

  it("validates the edit and shows the error with its place", async () => {
    const calls = serve({
      "GET /ui/api/config": () => json(view),
      "POST /ui/api/config/validate": ({ init }) =>
        String(init.body).includes("bogus")
          ? json({ ok: false, error: "unknown field `bogus`", line: 7, column: 1 })
          : json({ ok: true }),
    });
    renderApp(<ConfigPage />);
    const editor = await screen.findByLabelText("Config YAML");
    expect(editor).toHaveValue(view.yaml);
    expect(screen.getByRole("button", { name: /Save/ })).toBeDisabled();

    await userEvent.type(editor, "bogus: 1");
    await userEvent.click(screen.getByRole("button", { name: "Validate" }));
    expect(await screen.findByText(/Line 7, column 1:/)).toBeInTheDocument();
    expect(screen.getByText(/unknown field `bogus`/)).toBeInTheDocument();

    await userEvent.clear(editor);
    await userEvent.type(editor, "providers: {{}");
    await userEvent.click(screen.getByRole("button", { name: "Validate" }));
    expect(await screen.findByText(/this config would reload/)).toBeInTheDocument();
    expect(calls.filter((c) => c.key === "POST /ui/api/config/validate")).toHaveLength(2);
  });

  it("saves the edit as YAML and shows a refusal inline", async () => {
    let refuse = true;
    const calls = serve({
      "GET /ui/api/config": () => json(view),
      "PUT /ui/api/config": ({ init }) =>
        refuse
          ? json({ error: { message: "providers.n.key: nothing to keep", type: "invalid_config", line: null, column: null } }, 422)
          : json({ saved: true, path: "/etc/respan/gateway.yaml", backup: "/etc/respan/gateway.yaml.bak", ...reloaded, echo: String(init.body) }),
    });
    renderApp(<ConfigPage />);
    const editor = await screen.findByLabelText("Config YAML");
    await userEvent.type(editor, "# note");
    await userEvent.click(screen.getByRole("button", { name: /Save/ }));
    expect(await screen.findByText(/providers.n.key: nothing to keep/)).toBeInTheDocument();

    refuse = false;
    await userEvent.click(screen.getByRole("button", { name: /Save/ }));
    await waitFor(() => expect(calls.filter((c) => c.key === "PUT /ui/api/config")).toHaveLength(2));
    const put = calls.filter((c) => c.key === "PUT /ui/api/config")[1];
    expect(String(put.init.body)).toBe(`${view.yaml}# note`);
    expect(new Headers(put.init.headers).get("content-type")).toBe("text/yaml");
  });
});
