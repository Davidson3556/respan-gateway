import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { useOverview } from "@/hooks/queries";
import { getGatewayKey, setGatewayKey, type Overview } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { KeyDialog } from "./key-dialog";

const overview: Overview = {
  version: "9.9.9",
  listen: "127.0.0.1:8080",
  started_at: "2026-10-10T12:00:00.000Z",
  uptime_seconds: 60,
  reloaded_at: null,
  auth_required: true,
  config_source: "file",
  config_path: "gateway.yaml",
  totals: { requests: 0, errors: 0, in_flight: 0, input_tokens: 0, output_tokens: 0 },
};

function Version() {
  const { data } = useOverview();
  return <p>{data ? `version ${data.version}` : "waiting"}</p>;
}

/** An admin API that answers only to `Bearer <key>`. */
function gatewayWithKey(key: string) {
  return serve({
    "GET /ui/api/overview": ({ init }) =>
      new Headers(init.headers).get("authorization") === `Bearer ${key}`
        ? json(overview)
        : json({ error: { message: "missing or invalid key" } }, 401),
  });
}

function renderGuarded() {
  return renderApp(
    <>
      <KeyDialog />
      <Version />
    </>,
  );
}

describe("KeyDialog", () => {
  it("asks for a key on a 401, stores it and retries with it as a Bearer token", async () => {
    const calls = gatewayWithKey("sk-gw-1");
    renderGuarded();

    expect(await screen.findByText("This gateway needs a key")).toBeInTheDocument();
    expect(screen.getByText("waiting")).toBeInTheDocument();
    expect(new Headers(calls[0].init.headers).has("authorization")).toBe(false);

    await userEvent.type(screen.getByLabelText("Gateway key"), "  sk-gw-1 ");
    await userEvent.click(screen.getByRole("button", { name: "Save key" }));

    expect(await screen.findByText("version 9.9.9")).toBeInTheDocument();
    expect(getGatewayKey()).toBe("sk-gw-1");
    const last = calls[calls.length - 1];
    expect(new Headers(last.init.headers).get("authorization")).toBe("Bearer sk-gw-1");
    await waitFor(() =>
      expect(screen.queryByText("This gateway needs a key")).not.toBeInTheDocument(),
    );
  });

  it("says a stored key was refused, and does not save an empty one", async () => {
    setGatewayKey("sk-stale");
    gatewayWithKey("sk-gw-1");
    renderGuarded();

    expect(await screen.findByText("That key was refused")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save key" })).toBeDisabled();
  });

  it("stays closed after 'Not now' while background calls keep failing", async () => {
    const calls = gatewayWithKey("sk-gw-1");
    const { client } = renderGuarded();

    await userEvent.click(await screen.findByRole("button", { name: "Not now" }));
    await waitFor(() =>
      expect(screen.queryByText("This gateway needs a key")).not.toBeInTheDocument(),
    );
    const before = calls.length;
    await client.refetchQueries();
    await waitFor(() => expect(calls.length).toBeGreaterThan(before));
    expect(screen.queryByText("This gateway needs a key")).not.toBeInTheDocument();
    expect(getGatewayKey()).toBeNull();
  });

  it("says a valid key is not an admin key on a 403, rather than asking as if none was given", async () => {
    setGatewayKey("sk-client");
    serve({
      "GET /ui/api/overview": () => json({ error: { message: "not an admin key" } }, 403),
    });
    renderGuarded();

    expect(await screen.findByText("This key can't open the admin UI")).toBeInTheDocument();
    expect(screen.queryByText("This gateway needs a key")).not.toBeInTheDocument();
    expect(screen.queryByText("That key was refused")).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Not now" }));
    expect(getGatewayKey()).toBe("sk-client");
  });
});
