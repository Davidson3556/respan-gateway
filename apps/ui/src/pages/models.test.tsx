import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it } from "vitest";
import { json, renderApp, serve } from "@/test/render";
import { ModelsPage } from "./models";

const list = {
  object: "list",
  data: [
    {
      id: "claude-sonnet-5-5",
      object: "model",
      owned_by: "anthropic",
      kind: "chat",
      display_name: "Claude Sonnet 5.5",
      context_window: 1_000_000,
      max_output_tokens: 64_000,
      input_modalities: ["image", "document"],
      capabilities: ["tools", "reasoning"],
      pricing: { input: 3, output: 15, from: "provider" },
    },
    { id: "whisper-1", object: "model", owned_by: "openai", kind: "transcription" },
    { id: "smart-model", object: "model", owned_by: "routes", kind: "chat", route: { pick: "random", to: ["openai/gpt-6-luna"] } },
  ],
};

it("lists every model, filters by what it can do, and links to try one", async () => {
  serve({ "GET /v1/models": () => json(list), "GET /ui/api/providers": () => json({ providers: [] }) });
  renderApp(<ModelsPage />, "/models");
  const claude = (await screen.findByText("claude-sonnet-5-5")).closest("tr")!;
  expect(within(claude).getByText("Claude Sonnet 5.5")).toBeInTheDocument();
  expect(within(claude).getByText("$3 / $15")).toBeInTheDocument();
  expect(within(claude).getByRole("img", { name: "Documents" })).toBeInTheDocument();
  expect(within(claude).getByRole("link", { name: "Try claude-sonnet-5-5 in the playground" })).toHaveAttribute(
    "href",
    "/playground?tab=chat&kind=chat&model=claude-sonnet-5-5",
  );
  expect(screen.getByText("random: openai/gpt-6-luna")).toBeInTheDocument();
  expect(screen.getByText("3 of 3")).toBeInTheDocument();

  await userEvent.click(screen.getByRole("button", { name: "Reasoning" }));
  expect(screen.getByText("1 of 3")).toBeInTheDocument();
  expect(screen.queryByText("whisper-1")).not.toBeInTheDocument();
  await userEvent.click(screen.getByRole("button", { name: "Clear" }));
  await userEvent.type(screen.getByLabelText("Search model or provider"), "whisper");
  expect(screen.getByText("1 of 3")).toBeInTheDocument();
  expect(screen.getByText("whisper-1")).toBeInTheDocument();
});
