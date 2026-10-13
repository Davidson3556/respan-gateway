import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConfigView } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { ConfigPage } from "./config";

const view: ConfigView = {
  source: "file",
  path: "/etc/respan/gateway.toml",
  placeholder: "…",
  format: "toml",
  text: 'client_keys = ["…"]\n[providers.openai]\nvendor = "openai"\nkey = "env:OPENAI_API_KEY"\n',
};

const reloaded = { reloaded_at: "2026-10-11T09:30:00.000Z", restart_required: [] };

describe("ConfigPage", () => {
  it("shows the redacted config and the file it came from", async () => {
    serve({ "GET /ui/api/config": () => json(view) });
    renderApp(<ConfigPage />);
    expect(await screen.findByText(/env:OPENAI_API_KEY/)).toBeInTheDocument();
    expect(screen.getByText("/etc/respan/gateway.toml")).toBeInTheDocument();
    expect(screen.queryByLabelText("Config TOML")).not.toBeInTheDocument();
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

});
