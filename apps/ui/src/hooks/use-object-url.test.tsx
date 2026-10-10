import { renderHook } from "@testing-library/react";
import { StrictMode } from "react";
import { describe, expect, it, vi } from "vitest";
import { useObjectUrl } from "./use-object-url";

describe("useObjectUrl", () => {
  it("keeps the displayed URL live under StrictMode and revokes on unmount", () => {
    let n = 0;
    const live = new Set<string>();
    URL.createObjectURL = vi.fn(() => {
      const u = `blob:${n++}`;
      live.add(u);
      return u;
    });
    URL.revokeObjectURL = vi.fn((u: string) => void live.delete(u));
    const blob = new Blob(["x"]);
    const { result, unmount } = renderHook(() => useObjectUrl(blob), { wrapper: StrictMode });
    expect(result.current).not.toBeNull();
    expect(live.has(result.current!)).toBe(true);
    unmount();
    expect(live.size).toBe(0);
  });
});
