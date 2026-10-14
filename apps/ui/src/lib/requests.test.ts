import { describe, expect, it } from "vitest";
import type { RequestRecord } from "./api";
import { ANY, distinct, filterRequests, timingOf, tokensPerSecond } from "./requests";

const base: RequestRecord = {
  id: "r_1",
  started_at: "2026-10-10T12:00:00.000Z",
  method: "POST",
  path: "/v1/chat/completions",
  format: "openai-chat",
  kind: "chat",
  model: "gpt-4o-mini",
  provider: "openai",
  status: 200,
  duration_ms: 1000,
  ttfb_ms: 250,
  stream: false,
  usage: null,
  error: null,
};
const rows: RequestRecord[] = [
  base,
  { ...base, id: "r_2", model: "claude-sonnet-4-5", provider: "anthropic", status: 429 },
  { ...base, id: "r_3", kind: "image", path: "/v1/images/generations", status: null },
];
const none = { status: ANY, kind: ANY, provider: ANY, model: ANY, client: ANY, q: "" };
const ids = (f: Partial<typeof none>) => filterRequests(rows, { ...none, ...f }).map((r) => r.id);

describe("filterRequests", () => {
  it("keeps everything when nothing is set", () => expect(ids({})).toEqual(["r_1", "r_2", "r_3"]));

  it("filters by outcome", () => {
    expect(ids({ status: "error" })).toEqual(["r_2"]);
    expect(ids({ status: "pending" })).toEqual(["r_3"]);
    expect(ids({ status: "ok" })).toEqual(["r_1"]);
  });

  it("combines kind, provider and model", () => {
    expect(ids({ kind: "chat", provider: "anthropic" })).toEqual(["r_2"]);
    expect(ids({ model: "gpt-4o-mini" })).toEqual(["r_1", "r_3"]);
  });

  it("filters by client", () => {
    const named = [{ ...base, id: "a", client: "ci" }, { ...base, id: "b", client: "web" }, base];
    expect(filterRequests(named, { ...none, client: "web" }).map((r) => r.id)).toEqual(["b"]);
  });

  it("searches model and path, ignoring case and padding", () => {
    expect(ids({ q: "  CLAUDE " })).toEqual(["r_2"]);
    expect(ids({ q: "images" })).toEqual(["r_3"]);
  });
});

describe("distinct", () => {
  it("sorts and drops nulls and repeats", () => {
    expect(distinct(["b", null, "a", "b"])).toEqual(["a", "b"]);
  });
});

describe("timingOf", () => {
  it("splits the first byte from the rest", () => {
    expect(timingOf(base)).toEqual({ ttfb: 250, rest: 750, total: 1000, ttfbShare: 0.25 });
  });

  it("is null in flight", () => {
    expect(timingOf({ duration_ms: null, ttfb_ms: null })).toBeNull();
  });

  it("clamps a first byte past the end and survives a missing one", () => {
    expect(timingOf({ duration_ms: 100, ttfb_ms: 300 })?.rest).toBe(0);
    expect(timingOf({ duration_ms: 100, ttfb_ms: null })?.ttfbShare).toBe(0);
    expect(timingOf({ duration_ms: 0, ttfb_ms: 0 })?.ttfbShare).toBe(0);
  });
});

describe("tokensPerSecond", () => {
  const streamed = { ...base, stream: true, usage: { input_tokens: 10, output_tokens: 150 } };
  it("counts output tokens over the time after the first byte", () => {
    expect(tokensPerSecond(streamed)).toBe(200);
  });
  it("is null unless streamed, with output, and past the first byte", () => {
    expect(tokensPerSecond({ ...streamed, stream: false })).toBeNull();
    expect(tokensPerSecond({ ...streamed, usage: null })).toBeNull();
    expect(tokensPerSecond({ ...streamed, ttfb_ms: 1000 })).toBeNull();
  });
});

