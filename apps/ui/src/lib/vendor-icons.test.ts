import { describe, expect, it } from "vitest";
import { vendorIcon } from "./vendor-icons";

describe("vendorIcon", () => {
  it("finds a vendor's logo by its id, else by its API's domain", () => {
    expect(vendorIcon("openai", undefined)?.mono).toBe(true);
    expect(vendorIcon("gemini", "generativelanguage.googleapis.com")?.mono).toBe(false);
    // A provider configured without a vendor, at its vendor's API.
    expect(vendorIcon(null, "generativelanguage.googleapis.com")).toEqual(vendorIcon("gemini", undefined));
    expect(vendorIcon(null, "openrouter.ai")).toEqual(vendorIcon("openrouter", undefined));
    // A proxy of another vendor's API keeps its own, unknown, identity.
    expect(vendorIcon(null, "respan.ai")).toBeUndefined();
    expect(vendorIcon(null, "notopenai.com")).toBeUndefined();
  });
});
