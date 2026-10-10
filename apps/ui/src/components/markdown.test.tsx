import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Markdown } from "./markdown";

describe("Markdown lists", () => {
  it("keeps an ordered list with blank lines between items as one list", () => {
    const { container } = render(<Markdown source={"1. a\n\n2. b"} />);
    expect(container.querySelectorAll("ol")).toHaveLength(1);
    expect(container.querySelectorAll("li")).toHaveLength(2);
  });

  it("honours the start number", () => {
    const { container } = render(<Markdown source={"3. c\n4. d"} />);
    expect(container.querySelector("ol")).toHaveAttribute("start", "3");
  });
});
