import { describe, expect, it } from "vitest";
import type { Provider, RequestRecord } from "@/lib/api";
import { findCall, servedBy } from "./served";

const record = (over: Partial<RequestRecord>): RequestRecord => ({
  id: "r_1",
  started_at: "2026-01-01T00:00:10Z",
  method: "POST",
  path: "/v1/messages",
  format: "anthropic-messages",
  kind: "chat",
  model: "claude-sonnet-4-5",
  provider: "openai",
  status: 200,
  duration_ms: 100,
  ttfb_ms: 50,
  stream: true,
  usage: null,
  error: null,
  ...over,
});
const since = Date.parse("2026-01-01T00:00:08Z");
const want = { api: "anthropic-messages" as const, model: "claude-sonnet-4-5", since };

describe("findCall", () => {
  it("takes the newest record of this format and model since the call began", () => {
    const records = [
      record({ id: "old", started_at: "2025-12-31T23:00:00Z" }),
      record({ id: "other-format", format: "openai-chat", started_at: "2026-01-01T00:00:12Z" }),
      record({ id: "other-model", model: "gpt-4o", started_at: "2026-01-01T00:00:12Z" }),
      record({ id: "first" }),
      record({ id: "newest", started_at: "2026-01-01T00:00:11Z" }),
    ];
    expect(findCall(records, want)?.id).toBe("newest");
  });

  it("finds nothing when no record matches", () => {
    expect(findCall([record({ format: "openai-chat" })], want)).toBeUndefined();
  });
});

describe("servedBy", () => {
  const provider = (formats: string[]) => [{ name: "openai", formats }] as Provider[];

  it("marks a call translated when the provider speaks another format", () => {
    expect(servedBy(record({}), provider(["openai-chat"])).translatedFrom).toBe("anthropic-messages");
  });

  it("leaves a call to a provider of the same format untranslated", () => {
    expect(servedBy(record({}), provider(["anthropic-messages"])).translatedFrom).toBeNull();
  });

  it("claims no translation when the providers are unknown", () => {
    expect(servedBy(record({}), undefined).translatedFrom).toBeNull();
  });
});
