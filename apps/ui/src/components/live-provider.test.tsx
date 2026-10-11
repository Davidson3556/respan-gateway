import { act, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useLiveMode } from "@/hooks/use-live-requests";
import { queryKeys } from "@/hooks/queries";
import type { RequestRecord } from "@/lib/api";
import { renderApp } from "@/test/render";
import { LiveProvider } from "./live-provider";

const record: RequestRecord = {
  id: "r_1",
  started_at: "2026-10-10T12:00:00.000Z",
  method: "POST",
  path: "/v1/chat/completions",
  format: "openai-chat",
  kind: "chat",
  model: "m",
  provider: "p",
  status: 200,
  duration_ms: 5,
  ttfb_ms: 1,
  stream: false,
  usage: null,
  error: null,
};

function Mode() {
  return <p>mode:{useLiveMode()}</p>;
}

function mount() {
  return renderApp(
    <LiveProvider>
      <Mode />
    </LiveProvider>,
  );
}

/** One open event stream: write to it, and an abort errors it like a real fetch. */
interface Stream {
  write: (text: string) => void;
  aborted: () => boolean;
}

/** Stubs fetch so each call opens a stream, or fails while `failing`. */
function events(failing: () => boolean = () => false) {
  const streams: Stream[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (_: unknown, init: RequestInit) => {
      if (failing()) throw new TypeError("network down");
      const encoder = new TextEncoder();
      let controller!: ReadableStreamDefaultController<Uint8Array>;
      const body = new ReadableStream<Uint8Array>({
        start: (c) => void (controller = c),
      });
      init.signal?.addEventListener("abort", () => {
        try {
          controller.error(new DOMException("aborted", "AbortError"));
        } catch {
          // already closed
        }
      });
      streams.push({
        write: (text) => controller.enqueue(encoder.encode(text)),
        aborted: () => init.signal?.aborted ?? false,
      });
      return new Response(body, { status: 200 });
    }),
  );
  return streams;
}

const advance = (ms: number) => act(() => vi.advanceTimersByTimeAsync(ms));
const sent = (r: RequestRecord) => `event: request\ndata: ${JSON.stringify(r)}\n\n`;

beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
});

describe("LiveProvider", () => {
  it("goes live on open and folds each request event into the cache", async () => {
    const streams = events();
    const { client } = mount();
    expect(screen.getByText("mode:connecting")).toBeInTheDocument();
    await advance(0);
    expect(screen.getByText("mode:live")).toBeInTheDocument();
    streams[0].write(sent(record));
    await advance(0);
    expect(client.getQueryData(queryKeys.requests)).toEqual([record]);
    expect(client.getQueryData(queryKeys.request("r_1"))).toEqual(record);
  });

  it("skips a malformed event and keeps reading", async () => {
    const streams = events();
    const { client } = mount();
    await advance(0);
    streams[0].write("event: request\ndata: {oops\n\n");
    streams[0].write(sent(record));
    await advance(0);
    expect(client.getQueryData(queryKeys.requests)).toEqual([record]);
    expect(screen.getByText("mode:live")).toBeInTheDocument();
  });

  it("refreshes the totals once a second, not on every event", async () => {
    const streams = events();
    const { client } = mount();
    const invalidate = vi.spyOn(client, "invalidateQueries");
    await advance(0);
    invalidate.mockClear();
    for (const id of ["a", "b", "c"]) streams[0].write(sent({ ...record, id }));
    await advance(999);
    expect(invalidate).not.toHaveBeenCalled();
    await advance(1);
    expect(invalidate.mock.calls.map(([f]) => f?.queryKey)).toEqual([
      queryKeys.overview,
      queryKeys.providers,
    ]);
  });

  it("aborts a connection that falls silent for 45 s and reconnects", async () => {
    const streams = events();
    mount();
    await advance(0);
    await advance(44_999);
    expect(streams[0].aborted()).toBe(false);
    await advance(1);
    expect(streams[0].aborted()).toBe(true);
    expect(screen.getByText("mode:polling")).toBeInTheDocument();
    // The first retry waits 2 s.
    await advance(1999);
    expect(streams).toHaveLength(1);
    await advance(1);
    expect(streams).toHaveLength(2);
    expect(screen.getByText("mode:live")).toBeInTheDocument();
  });

  it("counts a keep-alive ping as life", async () => {
    const streams = events();
    mount();
    await advance(0);
    for (let i = 0; i < 4; i++) {
      await advance(30_000);
      streams[0].write(": ping\n\n");
    }
    expect(streams[0].aborted()).toBe(false);
    expect(screen.getByText("mode:live")).toBeInTheDocument();
  });

  it("backs off 2 s, 4 s, 8 s… up to 30 s while the gateway is down, then recovers", async () => {
    let down = true;
    const streams = events(() => down);
    mount();
    const fetches = () => vi.mocked(fetch).mock.calls.length;
    await advance(0);
    expect(fetches()).toBe(1);
    expect(screen.getByText("mode:polling")).toBeInTheDocument();
    let expected = 1;
    for (const wait of [2000, 4000, 8000, 16_000, 30_000, 30_000]) {
      await advance(wait - 1);
      expect(fetches()).toBe(expected);
      await advance(1);
      expected += 1;
      expect(fetches()).toBe(expected);
    }
    down = false;
    await advance(30_000);
    expect(streams).toHaveLength(1);
    expect(screen.getByText("mode:live")).toBeInTheDocument();
  });

  it("stops everything when unmounted", async () => {
    const streams = events();
    const { unmount } = mount();
    await advance(0);
    unmount();
    expect(streams[0].aborted()).toBe(true);
    await advance(60_000);
    expect(streams).toHaveLength(1);
  });
});
