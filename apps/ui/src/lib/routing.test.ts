import { describe, expect, it } from "vitest";
import type { Kind } from "./api";
import { resolveModel } from "./routing";

const providers: { name: string; kinds: Kind[] }[] = [
  { name: "openai", kinds: ["chat", "image"] },
  { name: "anthropic", kinds: ["chat"] },
];
const routes = {
  routes: [
    { prefix: "claude", provider: "anthropic" },
    { prefix: "gpt", provider: "openai" },
  ],
  defaults: [
    { kind: "chat" as const, provider: "openai" },
    { kind: "image" as const, provider: "openai" },
  ],
};

describe("resolveModel", () => {
  it("sends provider/model to that provider with the prefix removed", () => {
    expect(resolveModel("chat", "anthropic/claude-x", routes, providers)).toEqual({
      by: "explicit",
      provider: "anthropic",
      model: "claude-x",
    });
  });

  it("falls to a prefix route", () => {
    expect(resolveModel("chat", "claude-sonnet-4-5", routes, providers)).toMatchObject({
      by: "prefix",
      provider: "anthropic",
      prefix: "claude",
    });
  });

  it("skips a prefix whose provider does not serve the kind", () => {
    expect(resolveModel("image", "claude-art", routes, providers)).toMatchObject({
      by: "default",
      provider: "openai",
    });
  });

  it("ignores an unknown provider before the slash", () => {
    expect(resolveModel("chat", "meta/llama", routes, providers).by).toBe("default");
  });

  it("uses the kind's default for an empty model", () => {
    expect(resolveModel("chat", "", routes, providers)).toEqual({
      by: "default",
      provider: "openai",
      model: "",
    });
  });

  it("finds nothing when the kind has no default", () => {
    expect(resolveModel("video", "sora", routes, providers)).toEqual({ by: "none" });
  });
});
