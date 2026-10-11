import { describe, expect, it } from "vitest";
import type { Model } from "@/lib/api";
import { preferred } from "./use-model-choice";

const models = (...ids: string[]): Model[] => ids.map((id) => ({ id, object: "model", owned_by: "x" }));

describe("preferred", () => {
  it("picks an alias over dated snapshots listed before it", () => {
    expect(preferred(models("gpt-realtime-2025-08-28", "gpt-realtime-mini-20251006", "gpt-realtime-mini"))).toBe(
      "gpt-realtime-mini",
    );
  });

  it("falls back to the first model, and to nothing", () => {
    expect(preferred(models("a-2025-08-28", "b-2025-08-29"))).toBe("a-2025-08-28");
    expect(preferred(models())).toBeUndefined();
    expect(preferred(undefined)).toBeUndefined();
  });
});
