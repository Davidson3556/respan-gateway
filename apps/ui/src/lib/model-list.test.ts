import { describe, expect, it } from "vitest";
import type { Model } from "@/lib/api";
import { filterModels, retirement, summarize, type ModelFilter } from "./model-list";

const model = (id: string, over: Partial<Model> = {}): Model => ({
  id,
  object: "model",
  owned_by: "openai",
  kind: "chat",
  ...over,
});
const now = Date.parse("2026-10-14T00:00:00Z");
const models = [
  model("gpt-big", { context_window: 400_000, pricing: { input: 5, output: 20 }, capabilities: ["tools", "reasoning"] }),
  model("gpt-small", { context_window: 128_000, pricing: { input: 0.1, output: 0.4 }, capabilities: ["tools"], retires_at: "2026-11-01T00:00:00Z" }),
  model("claude", { owned_by: "anthropic", input_modalities: ["image", "document"], capabilities: ["tools"], retires_at: "2027-06-01T00:00:00Z" }),
  model("whisper", { kind: "transcription", retires_at: "2026-01-01T00:00:00Z" }),
  model("smart", { owned_by: "routes", route: { pick: "random", to: ["openai/gpt-big"] } }),
];
const all: ModelFilter = { q: "", provider: null, kind: null, traits: [], sort: "name" };
const ids = (filter: Partial<ModelFilter>) => filterModels(models, { ...all, ...filter }).map((m) => m.id);

describe("filterModels", () => {
  it("searches, filters by provider, kind and every trait asked", () => {
    expect(ids({ q: "GPT" })).toEqual(["gpt-big", "gpt-small"]);
    expect(ids({ q: "anthropic" })).toEqual(["claude"]);
    expect(ids({ provider: "anthropic" })).toEqual(["claude"]);
    expect(ids({ kind: "transcription" })).toEqual(["whisper"]);
    expect(ids({ traits: ["tools"] })).toEqual(["claude", "gpt-big", "gpt-small"]);
    expect(ids({ traits: ["tools", "reasoning"] })).toEqual(["gpt-big"]);
    expect(ids({ traits: ["document"] })).toEqual(["claude"]);
  });

  it("sorts with what is unknown last", () => {
    expect(ids({ sort: "price" }).slice(0, 2)).toEqual(["gpt-small", "gpt-big"]);
    expect(ids({ sort: "context" }).slice(0, 2)).toEqual(["gpt-big", "gpt-small"]);
    expect(ids({ sort: "retires" }).slice(0, 3)).toEqual(["whisper", "gpt-small", "claude"]);
  });
});

describe("retirement and summarize", () => {
  it("says how near a retirement is, and counts the soon ones", () => {
    expect(retirement(models[0], now)).toBeNull();
    expect(retirement(models[1], now)).toBe("soon");
    expect(retirement(models[2], now)).toBe("later");
    expect(retirement(models[3], now)).toBe("past");
    expect(summarize(models, now)).toEqual({ models: 4, routes: 1, providers: 2, retiring: 1 });
  });
});
