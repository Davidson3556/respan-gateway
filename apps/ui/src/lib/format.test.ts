import { describe, expect, it } from "vitest";
import { formatPrice } from "@/lib/format";

describe("formatPrice", () => {
  it("shows tokens per million, and a price by the second per minute", () => {
    expect(formatPrice({ input: 0.25, output: 2 })).toBe("$0.25 / $2");
    expect(formatPrice({ input: 0, output: 0, per_second: 0.0001 })).toBe("$0.006 / min");
    expect(formatPrice({ input: 0.25, output: 2 }, " per M")).toBe("$0.25 / $2 per M");
    expect(formatPrice({ input: 0, output: 0, per_second: 0.0001 }, " per M")).toBe("$0.006 / min");
    expect(formatPrice(null)).toBe("—");
  });
});
