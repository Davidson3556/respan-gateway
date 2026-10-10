// A small Server-Sent Events parser for fetch bodies. EventSource can't send
// an Authorization header, so both the live request feed and the chat
// playground read SSE through fetch and this parser.

export interface SseEvent {
  event: string;
  data: string;
  id?: string;
}

/** Incremental parser: feed it text chunks in any split, get whole events. */
export class SseParser {
  private buffer = "";
  private event = "";
  private data: string[] = [];
  private id: string | undefined;

  push(chunk: string): SseEvent[] {
    this.buffer += chunk;
    const out: SseEvent[] = [];
    // A trailing "\r" may be the first half of "\r\n", so leave it buffered.
    const re = /\r\n|\r(?!$)|\n/g;
    let start = 0;
    for (let m = re.exec(this.buffer); m; m = re.exec(this.buffer)) {
      const line = this.buffer.slice(start, m.index);
      start = m.index + m[0].length;
      const event = this.line(line);
      if (event) out.push(event);
    }
    this.buffer = this.buffer.slice(start);
    return out;
  }

  private line(line: string): SseEvent | null {
    if (line === "") return this.dispatch();
    if (line.startsWith(":")) return null;
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") this.event = value;
    else if (field === "data") this.data.push(value);
    else if (field === "id") this.id = value;
    return null;
  }

  private dispatch(): SseEvent | null {
    const event =
      this.data.length > 0
        ? { event: this.event || "message", data: this.data.join("\n"), id: this.id }
        : null;
    this.event = "";
    this.data = [];
    return event;
  }
}

/** Read an SSE response body to the end, calling onEvent for each event. */
export async function readSse(
  body: ReadableStream<Uint8Array>,
  onEvent: (event: SseEvent) => void,
): Promise<void> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  const parser = new SseParser();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      for (const event of parser.push(decoder.decode(value, { stream: true }))) onEvent(event);
    }
    for (const event of parser.push(decoder.decode() + "\n\n")) onEvent(event);
  } finally {
    reader.releaseLock();
  }
}
