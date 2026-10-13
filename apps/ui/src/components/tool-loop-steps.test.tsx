import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { ToolLoopStep } from "@/lib/api";
import { ToolLoopSteps } from "./tool-loop-steps";

const steps: ToolLoopStep[] = [
  {
    index: 1,
    duration_ms: 820,
    input_tokens: 120,
    output_tokens: 14,
    finish: "tool_use",
    tools: [
      { name: "files__read", duration_ms: 35, error: false, bytes: 2048 },
      { name: "github__search", duration_ms: 1500, error: true, bytes: 90 },
    ],
  },
  { index: 2, duration_ms: 400, input_tokens: 300, output_tokens: 40, finish: "stop", tools: [] },
];

describe("ToolLoopSteps", () => {
  it("lists each call to the model with the tools it ran", () => {
    render(<ToolLoopSteps steps={steps} />);
    expect(screen.getByText("2 calls to the model, 2 tools run by the gateway.")).toBeInTheDocument();
    const list = screen.getByRole("list", { name: "Tool loop steps" });
    const first = within(list).getAllByRole("listitem")[0];
    expect(within(first).getByText("Step 1")).toBeInTheDocument();
    expect(within(first).getByText("tool_use")).toBeInTheDocument();
    expect(within(first).getByText("files__read")).toBeInTheDocument();
    expect(within(first).getByText("35 ms · 2.0 KB")).toBeInTheDocument();
  });

  it("marks a tool that failed", () => {
    render(<ToolLoopSteps steps={steps} />);
    expect(screen.getAllByLabelText("failed")).toHaveLength(1);
    expect(screen.getByText("github__search")).toHaveClass("text-destructive");
    expect(screen.getByText("files__read")).not.toHaveClass("text-destructive");
  });

  it("speaks of one call and one tool in the singular", () => {
    render(<ToolLoopSteps steps={[{ ...steps[0], tools: steps[0].tools.slice(0, 1) }]} />);
    expect(screen.getByText("1 call to the model, 1 tool run by the gateway.")).toBeInTheDocument();
  });
});
