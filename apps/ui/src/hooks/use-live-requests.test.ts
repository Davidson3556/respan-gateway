import { describe, expect, it } from "vitest";
import type { RequestRecord } from "@/lib/api";
import { mergeRecords } from "./use-live-requests";

const rec = (id: string, started: number, status: number | null = null): RequestRecord => ({
  id,
  started_at: new Date(started).toISOString(),
  method: "POST",
  path: "/v1/chat/completions",
  format: null,
  kind: "chat",
  model: null,
  provider: null,
  status,
  duration_ms: status === null ? null : 5,
  ttfb_ms: null,
  stream: false,
  usage: null,
  error: null,
});

describe("mergeRecords", () => {
  it("keeps a finished cached record over an in-flight fetched one", () => {
    const merged = mergeRecords([rec("a", 1000)], [rec("a", 1000, 200)]);
    expect(merged[0].status).toBe(200);
  });

  it("takes the fetched record when it is the finished one", () => {
    const merged = mergeRecords([rec("a", 1000, 500)], [rec("a", 1000)]);
    expect(merged[0].status).toBe(500);
  });

  it("keeps records the fetch has not seen and orders newest first", () => {
    const merged = mergeRecords([rec("a", 1000)], [rec("b", 2000)]);
    expect(merged.map((r) => r.id)).toEqual(["b", "a"]);
  });
});
