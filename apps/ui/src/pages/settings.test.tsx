import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { ThemeProvider } from "@/components/theme-provider";
import { getGatewayKey, setGatewayKey, type Overview } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { SettingsPage } from "./settings";

const overview: Overview = {
  version: "1.2.3",
  listen: "127.0.0.1:8080",
  started_at: "2026-10-10T12:00:00.000Z",
  uptime_seconds: 3700,
  reloaded_at: null,
  auth_required: true,
  config_source: "environment",
  config_path: null,
  totals: { requests: 0, errors: 0, in_flight: 0, input_tokens: 0, output_tokens: 0 },
};

const renderSettings = () =>
  renderApp(
    <ThemeProvider>
      <SettingsPage />
    </ThemeProvider>,
  );

describe("SettingsPage", () => {
  it("shows what the gateway is and how to point a client at it", async () => {
    serve({ "GET /ui/api/overview": () => json(overview) });
    renderSettings();
    expect(await screen.findByText("1.2.3")).toBeInTheDocument();
    expect(screen.getByText("127.0.0.1:8080")).toBeInTheDocument();
    expect(screen.getByText("Environment variables")).toBeInTheDocument();
    expect(screen.getByText("Required")).toBeInTheDocument();
    expect(screen.getByText(`${window.location.origin}/v1`)).toBeInTheDocument();
    expect(screen.getByText(/This gateway requires a client key/)).toBeInTheDocument();
  });

  it("shows a config file path and a gateway that is not listening yet", async () => {
    serve({
      "GET /ui/api/overview": () =>
        json({ ...overview, listen: null, config_source: "file", config_path: "/etc/respan/gateway.yaml" }),
    });
    renderSettings();
    expect(await screen.findByText("Config file (/etc/respan/gateway.yaml)")).toBeInTheDocument();
    expect(screen.getByText("not listening yet")).toBeInTheDocument();
  });

  it("saves the gateway key, masked until shown, and clears it", async () => {
    serve({ "GET /ui/api/overview": () => json(overview) });
    renderSettings();
    const input = screen.getByLabelText("Key");
    expect(input).toHaveAttribute("type", "password");
    expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();

    await userEvent.type(input, "sk-gw-1");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    expect(getGatewayKey()).toBe("sk-gw-1");

    await userEvent.click(screen.getByRole("button", { name: "Show key" }));
    expect(screen.getByLabelText("Key")).toHaveAttribute("type", "text");
    expect(screen.getByLabelText("Key")).toHaveValue("sk-gw-1");

    await userEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(getGatewayKey()).toBeNull();
    expect(screen.getByLabelText("Key")).toHaveValue("");
  });

  it("shows a key stored elsewhere, such as by the key dialog", async () => {
    setGatewayKey("sk-stored");
    serve({ "GET /ui/api/overview": () => json({ ...overview, auth_required: false }) });
    renderSettings();
    expect(screen.getByLabelText("Key")).toHaveValue("sk-stored");
    expect(await screen.findByText(/This gateway is open/)).toBeInTheDocument();
  });

  it("still offers the key and the theme when the gateway cannot be reached", async () => {
    serve({});
    renderSettings();
    expect(screen.getByLabelText("Key")).toBeInTheDocument();
    expect(screen.getByRole("radiogroup", { name: "Theme" })).toBeInTheDocument();
    expect(screen.queryByText("About this gateway")).not.toBeInTheDocument();
  });
});
