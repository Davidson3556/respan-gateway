import { useState } from "react";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it } from "vitest";
import { byProvider, modelFacts } from "@/lib/models";
import type { Model } from "@/lib/api";
import { ModelPicker } from "./model-picker";

const models: Model[] = [
  { id: "claude-haiku-5-5", object: "model", owned_by: "anthropic" },
  { id: "gpt-5-mini", object: "model", owned_by: "openai" },
  { id: "claude-opus-5-5", object: "model", owned_by: "anthropic" },
];

function Picker() {
  const [chosen, setModel] = useState("");
  const model = chosen || models[0].id;
  return <>
    <ModelPicker id="model" choice={{ model, chosen, setModel, models, isPending: false, fallback: "fallback" }} />
    <output aria-label="Selected model">{model}</output>
  </>;
}

it("groups models by provider, in the order providers are first listed", () => {
  const groups = byProvider(models).map(([p, ms]) => [p, ms.map((m) => m.id)]);
  expect(groups).toEqual([
    ["anthropic", ["claude-haiku-5-5", "claude-opus-5-5"]],
    ["openai", ["gpt-5-mini"]],
  ]);
});

it("picks a listed model, found by its provider's name", async () => {
  render(<Picker />);
  await userEvent.click(screen.getByRole("combobox", { name: "Model" }));
  await userEvent.type(screen.getByPlaceholderText(/Search 3 models/), "openai");
  const options = screen.getAllByRole("option");
  const names = options.map((o) => o.getAttribute("aria-label") ?? o.textContent);
  expect(names).toEqual(["gpt-5-mini", "Use openai"]);
  await userEvent.click(options[0]);
  expect(screen.getByLabelText("Selected model")).toHaveTextContent("gpt-5-mini");
  expect(screen.getByRole("combobox", { name: "Model" })).toHaveTextContent("gpt-5-mini");
});

it("uses a typed ID no provider lists", async () => {
  render(<Picker />);
  await userEvent.click(screen.getByRole("combobox", { name: "Model" }));
  await userEvent.type(screen.getByPlaceholderText(/or enter an ID/), "my-provider/private-model-v2");
  await userEvent.click(screen.getByRole("option", { name: "Use my-provider/private-model-v2" }));
  expect(screen.getByLabelText("Selected model")).toHaveTextContent("my-provider/private-model-v2");
});

it("offers no typed ID that is already listed", async () => {
  render(<Picker />);
  await userEvent.click(screen.getByRole("combobox", { name: "Model" }));
  await userEvent.type(screen.getByPlaceholderText(/Search/), "gpt-5-mini");
  const list = screen.getByRole("listbox");
  expect(within(list).queryByText("Use")).not.toBeInTheDocument();
});

it("says what a model's list said of it: name, context and price", () => {
  expect(
    modelFacts({
      id: "claude-haiku-5-5",
      display_name: "Claude Haiku 5.5",
      context_window: 1_000_000,
      pricing: { input: 1, output: 5 },
    }),
  ).toBe("Claude Haiku 5.5 · 1M ctx · $1 / $5 per M");
  expect(modelFacts({ id: "gpt-x", display_name: "gpt-x", context_window: 128_000 })).toBe("128K ctx");
  expect(modelFacts({ id: "plain" })).toBe("");
});

it("says where a route goes", () => {
  const route = { pick: "cheapest", to: ["openai/gpt-4.1", "groq/llama"] };
  expect(modelFacts({ id: "smart", owned_by: "routes", route })).toBe("cheapest: openai/gpt-4.1 +1");
  expect(modelFacts({ id: "pin", route: { pick: "first", to: ["openai/gpt-4o"] } })).toBe("→ openai/gpt-4o");
});
