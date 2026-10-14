import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { json, renderApp, serve } from "@/test/render";
import { ChatTab } from "./chat";

const serveAll = [
  { format: "openai-chat", kind: "chat", endpoints: ["POST /v1/chat/completions"] },
  { format: "anthropic-messages", kind: "chat", endpoints: ["POST /v1/messages"] },
  { format: "openai-realtime", kind: "realtime", endpoints: ["GET /v1/realtime"] },
];

const stream = (...events: [string, unknown][]) =>
  new Response(events.map(([name, data]) => `event: ${name}\ndata: ${JSON.stringify(data)}\n\n`).join(""), {
    headers: { "content-type": "text/event-stream" },
  });

describe("ChatTab API selector", () => {
  it("offers only the APIs the gateway serves, and speaks the one picked", async () => {
    const calls = serve({
      "GET /v1/models": () => json({ object: "list", data: [] }),
      "GET /ui/api/routes": () => json({ serve: serveAll, routes: [], defaults: [] }),
      "GET /ui/api/providers": () =>
        json({ providers: [{ name: "openai", formats: ["openai-chat"], kinds: ["chat"] }] }),
      "POST /v1/messages": () =>
        stream(
          ["message_start", { message: { usage: { input_tokens: 3 } } }],
          ["content_block_delta", { delta: { type: "text_delta", text: "Bonjour" } }],
          ["message_delta", { delta: { stop_reason: "end_turn" }, usage: { output_tokens: 1 } }],
        ),
      "GET /ui/api/requests": () =>
        json({
          requests: [
            {
              id: "r_9",
              started_at: new Date().toISOString(),
              format: "anthropic-messages",
              model: "gpt-4o-mini",
              provider: "openai",
              status: 200,
              steps: [
                {
                  index: 1,
                  duration_ms: 10,
                  input_tokens: 1,
                  output_tokens: 1,
                  tools: [{ name: "files__read", duration_ms: 12, error: false, bytes: 5 }],
                },
              ],
            },
          ],
        }),
    });
    renderApp(<ChatTab />);

    await userEvent.click(await screen.findByRole("combobox", { name: "API" }));
    expect(screen.queryByRole("option", { name: "Gemini" })).not.toBeInTheDocument();
    await userEvent.click(await screen.findByRole("option", { name: "Anthropic Messages" }));
    expect(screen.getByText("/v1/messages")).toBeInTheDocument();

    await userEvent.type(screen.getByLabelText("Message"), "Salut{Enter}");
    expect(await screen.findByText("Bonjour")).toBeInTheDocument();
    expect(calls.some((c) => c.key === "POST /v1/chat/completions")).toBe(false);

    // Who served it, that it was translated, and the tools the tool loop used.
    expect(await screen.findByText(/translated from Anthropic Messages/)).toBeInTheDocument();
    expect(screen.getByText("openai")).toBeInTheDocument();
    expect(screen.getByText("Used 1 tool")).toBeInTheDocument();
    expect(screen.getByText("files__read")).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: /request details|every step/i })[0]).toHaveAttribute(
      "href",
      "/requests/r_9",
    );
  });

  it("hides the selector when only one API is served", async () => {
    serve({
      "GET /v1/models": () => json({ object: "list", data: [] }),
      "GET /ui/api/routes": () => json({ serve: serveAll.slice(0, 1), routes: [], defaults: [] }),
    });
    renderApp(<ChatTab />);
    await screen.findByLabelText("Message");
    expect(screen.queryByRole("combobox", { name: "API" })).not.toBeInTheDocument();
  });
});

it("keeps partial replies in the conversation after a stream error", async () => {
  let count = 0;
  const calls = serve({
    "GET /v1/models": () => json({ object: "list", data: [] }),
    "GET /ui/api/routes": () => json({ serve: serveAll.slice(0, 1), routes: [], defaults: [] }),
    "GET /ui/api/providers": () => json({ providers: [] }),
    "GET /ui/api/requests": () => json({ requests: [] }),
    "POST /v1/chat/completions": () => ++count === 1
      ? stream(
          ["message", { choices: [{ delta: { content: "The answer starts here." } }] }],
          ["message", { error: { message: "stream ended before completion", code: "truncated" } }],
        )
      : stream(["message", { choices: [{ delta: { content: "And continues here." }, finish_reason: "stop" }] }]),
  });
  renderApp(<ChatTab />);
  await userEvent.type(await screen.findByLabelText("Message"), "Start{Enter}");
  expect(await screen.findByText("Response interrupted")).toBeInTheDocument();
  await userEvent.type(screen.getByLabelText("Message"), "Continue{Enter}");
  expect(await screen.findByText("And continues here.")).toBeInTheDocument();
  expect(screen.getByText("The answer starts here.")).toBeInTheDocument();
  const request = calls.filter((c) => c.key === "POST /v1/chat/completions")[1];
  expect(JSON.parse(String(request.init.body)).messages).toEqual([
    { role: "user", content: "Start" },
    { role: "assistant", content: "The answer starts here." },
    { role: "user", content: "Continue" },
  ]);
});
