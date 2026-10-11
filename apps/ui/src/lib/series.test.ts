import { describe, expect, it } from "vitest";
import type { RequestRecord } from "@/lib/api";
import { bucketByMinute } from "./series";

const now = Date.parse("2026-01-01T12:10:30Z");

function record(minutesAgo: number, over: Partial<RequestRecord> = {}): RequestRecord {
  return {
    id: crypto.randomUUID(),
    started_at: new Date(now - minutesAgo * 60_000).toISOString(),
    method: "POST",
    path: "/v1/chat/completions",
    format: null,
    kind: "chat",
    model: "m",
    provider: "p",
    status: 200,
    duration_ms: 100,
    ttfb_ms: null,
    stream: false,
    usage: null,
    error: null,
    ...over,
  };
}

describe("bucketByMinute", () => {
  it("counts requests, errors and mean latency per minute, oldest first", () => {
    const buckets = bucketByMinute(
      [
        record(0),
        record(0, { status: 500, duration_ms: 300 }),
        record(2, { duration_ms: 50 }),
        record(0, { status: null, duration_ms: null }),
        record(99),
      ],
      3,
      now,
    );
    expect(buckets).toEqual([
      { requests: 1, errors: 0, latencyMs: 50 },
      { requests: 0, errors: 0, latencyMs: 0 },
      { requests: 3, errors: 1, latencyMs: 200 },
    ]);
  });
});
