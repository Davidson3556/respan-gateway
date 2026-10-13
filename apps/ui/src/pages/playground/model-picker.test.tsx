import { useState } from "react";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it } from "vitest";
import { ModelPicker } from "./model-picker";

function Picker() {
  const [chosen, setModel] = useState("");
  const model = chosen || "listed-model";
  return <>
    <ModelPicker id="model" choice={{ model, chosen, setModel, models: [{ id: "listed-model", object: "model", owned_by: "provider" }], isPending: false, fallback: "fallback" }} />
    <output aria-label="Selected model">{model}</output>
  </>;
}

it("accepts a custom ID even with listed models, and can return to the list", async () => {
  render(<Picker />);
  await userEvent.click(screen.getByRole("button", { name: "Custom ID" }));
  const input = screen.getByLabelText("Custom model ID");
  await userEvent.clear(input);
  await userEvent.type(input, "my-provider/private-model-v2");
  expect(screen.getByLabelText("Selected model")).toHaveTextContent("my-provider/private-model-v2");
  await userEvent.click(screen.getByRole("button", { name: "Choose model" }));
  expect(screen.getByRole("combobox", { name: "Model" })).toHaveTextContent("listed-model");
});
