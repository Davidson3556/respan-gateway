import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { useRecorder } from "./use-recorder";

function setup() {
  const stop = vi.fn();
  const getUserMedia = vi.fn(async () => ({ getTracks: () => [{ stop }] }));
  vi.stubGlobal("navigator", { mediaDevices: { getUserMedia } });
  class FakeRecorder {
    mimeType = "audio/webm";
    stream: unknown;
    onstop: (() => void) | null = null;
    constructor(stream: unknown) {
      this.stream = stream;
    }
    start() {}
    stop() {}
  }
  vi.stubGlobal("MediaRecorder", FakeRecorder);
  Object.defineProperty(window, "MediaRecorder", { value: FakeRecorder, configurable: true });
  return { stop, getUserMedia };
}

describe("useRecorder", () => {
  it("ignores a second start while the permission prompt is open", async () => {
    const { getUserMedia } = setup();
    const { result } = renderHook(() => useRecorder(() => {}));
    await act(async () => {
      await Promise.all([result.current.start(), result.current.start()]);
    });
    expect(getUserMedia).toHaveBeenCalledTimes(1);
  });

  it("stops the stream when unmounted during the prompt", async () => {
    const { stop } = setup();
    const { result, unmount } = renderHook(() => useRecorder(() => {}));
    let started!: Promise<void>;
    act(() => {
      started = result.current.start();
    });
    unmount();
    await started;
    expect(stop).toHaveBeenCalled();
  });
});
