import { describe, expect, it } from "vitest";
import { SseParser, readSse, type SseEvent } from "./sse";

describe("SseParser", () => {
  it("parses named events and ignores comments", () => {
    const p = new SseParser();
    const events = p.push(': ping\n\nevent: request\ndata: {"id":"r_1"}\n\n');
    expect(events).toEqual([{ event: "request", data: '{"id":"r_1"}', id: undefined }]);
  });

  it("reassembles events split at arbitrary points, including inside CRLF", () => {
    const stream = 'event: request\r\ndata: {"a":1}\r\n\r\ndata: second\r\n\r\n';
    for (let cut = 1; cut < stream.length; cut++) {
      const p = new SseParser();
      const events = [...p.push(stream.slice(0, cut)), ...p.push(stream.slice(cut))];
      expect(events.map((e) => [e.event, e.data])).toEqual([
        ["request", '{"a":1}'],
        ["message", "second"],
      ]);
    }
  });

  it("joins multi-line data and drops the single leading space only", () => {
    const p = new SseParser();
    expect(p.push("data: one\ndata:  two\ndata\n\n")[0].data).toBe("one\n two\n");
  });

  it("does not emit an event without data", () => {
    expect(new SseParser().push("event: request\n\n")).toEqual([]);
  });
});

describe("readSse", () => {
  it("decodes a byte stream split mid-character and flushes a final unterminated event", async () => {
    const bytes = new TextEncoder().encode("data: héllo\n\ndata: last");
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(bytes.slice(0, 8)); // splits the two-byte "é"
        controller.enqueue(bytes.slice(8));
        controller.close();
      },
    });
    const seen: SseEvent[] = [];
    await readSse(body, (e) => seen.push(e));
    expect(seen.map((e) => e.data)).toEqual(["héllo", "last"]);
  });
});
