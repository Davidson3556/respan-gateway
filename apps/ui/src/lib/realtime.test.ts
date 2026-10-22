import { afterEach, describe, expect, it, vi } from "vitest";
import {
  initialRealtime,
  OPEN_TIMEOUT_MS,
  openRealtime,
  realtimeReducer,
  type RealtimeAction,
  type ServerEvent,
} from "./realtime";
import { geminiLive, openaiRealtime } from "./realtime-apis";

const run = (...events: (ServerEvent | RealtimeAction)[]) =>
  events.reduce(
    (state, e) => realtimeReducer(state, "type" in e && ["connecting", "open", "close"].includes(e.type)
      ? (e as RealtimeAction)
      : { type: "event", event: e as ServerEvent }),
    initialRealtime,
  );

describe("realtimeReducer", () => {
  it("walks the session phases", () => {
    expect(run({ type: "connecting" }).phase).toBe("connecting");
    expect(run({ type: "connecting" }, { type: "open" }).phase).toBe("live");
    expect(run({ type: "open" }, { type: "close" }).phase).toBe("ended");
    expect(run({ type: "open" }, { type: "close", error: "refused" })).toMatchObject({
      phase: "failed",
      error: "refused",
    });
  });

  it("follows who is speaking", () => {
    expect(run({ type: "input_audio_buffer.speech_started" }).speaker).toBe("user");
    expect(
      run({ type: "input_audio_buffer.speech_started" }, { type: "input_audio_buffer.speech_stopped" })
        .speaker,
    ).toBeNull();
    expect(run({ type: "response.output_audio.delta", delta: "AAA=" }).speaker).toBe("assistant");
    expect(
      run({ type: "response.output_audio.delta" }, { type: "response.done" }).speaker,
    ).toBeNull();
  });

  it("builds a user line from its transcript", () => {
    const state = run(
      { type: "conversation.item.created", item: { id: "u1", role: "user" } },
      {
        type: "conversation.item.input_audio_transcription.completed",
        item_id: "u1",
        transcript: " Hello there \n",
      },
    );
    expect(state.lines).toEqual([
      { id: "u1", role: "user", text: "Hello there", done: true, interrupted: false },
    ]);
  });

  it("streams an assistant line from transcript deltas, in either event naming", () => {
    const state = run(
      { type: "response.output_audio_transcript.delta", item_id: "a1", delta: "Hi " },
      { type: "response.audio_transcript.delta", item_id: "a1", delta: "there" },
    );
    expect(state.lines).toMatchObject([{ id: "a1", role: "assistant", text: "Hi there", done: false }]);
    const done = realtimeReducer(state, {
      type: "event",
      event: { type: "response.output_audio_transcript.done", item_id: "a1", transcript: "Hi there!" },
    });
    expect(done.lines[0]).toMatchObject({ text: "Hi there!", done: true });
  });

  it("keeps lines in the order their items were created, even if a transcript lags", () => {
    const state = run(
      { type: "conversation.item.created", item: { id: "u1", role: "user" } },
      { type: "response.output_audio_transcript.delta", item_id: "a1", delta: "Sure" },
      {
        type: "conversation.item.input_audio_transcription.completed",
        item_id: "u1",
        transcript: "Question",
      },
    );
    expect(state.lines.map((l) => l.id)).toEqual(["u1", "a1"]);
    expect(state.lines[0].text).toBe("Question");
  });

  it("marks the reply interrupted when the user barges in, and ignores its late deltas", () => {
    const state = run(
      { type: "response.output_audio_transcript.delta", item_id: "a1", delta: "Well, " },
      { type: "response.output_audio.delta" },
      { type: "input_audio_buffer.speech_started" },
      { type: "response.output_audio_transcript.delta", item_id: "a1", delta: "actually" },
    );
    expect(state.speaker).toBe("user");
    expect(state.lines[0]).toMatchObject({ text: "Well, ", done: true, interrupted: true });
  });

  it("marks the reply interrupted when it is truncated or its response is cancelled", () => {
    const truncated = run(
      { type: "response.output_audio_transcript.delta", item_id: "a1", delta: "Well, " },
      { type: "conversation.item.truncated", item_id: "a1" },
    );
    expect(truncated.lines[0]).toMatchObject({ done: true, interrupted: true });
    const cancelled = run(
      { type: "response.output_audio_transcript.delta", item_id: "a1", delta: "Well, " },
      { type: "response.done", response: { status: "cancelled" } },
    );
    expect(cancelled.lines[0]).toMatchObject({ done: true, interrupted: true });
    const finished = run(
      { type: "response.output_audio_transcript.delta", item_id: "a1", delta: "Hi" },
      { type: "response.done", response: { status: "completed" } },
    );
    expect(finished.lines[0]).toMatchObject({ done: true, interrupted: false });
  });

  it("records an error without ending the session", () => {
    const state = run({ type: "open" }, { type: "error", error: { message: "bad event" } });
    expect(state).toMatchObject({ phase: "live", error: "bad event" });
  });

  it("finishes open lines when the session closes", () => {
    const state = run(
      { type: "response.output_audio_transcript.delta", item_id: "a1", delta: "x" },
      { type: "close" },
    );
    expect(state.lines[0].done).toBe(true);
  });

  it("ignores events it doesn't know", () => {
    expect(run({ type: "rate_limits.updated" })).toEqual(initialRealtime);
  });
});

describe("the socket", () => {
  class FakeSocket {
    static OPEN = 1;
    static last: FakeSocket;
    readyState = 0;
    sent: string[] = [];
    onopen: (() => void) | null = null;
    onmessage: ((e: { data: unknown }) => void) | null = null;
    onclose: ((e: { code: number; reason: string }) => void) | null = null;
    constructor(
      readonly url: string,
      readonly protocols: string[],
    ) {
      FakeSocket.last = this;
    }
    send(data: string) {
      this.sent.push(data);
    }
    close(code: number) {
      this.onclose?.({ code, reason: "" });
    }
    open() {
      this.readyState = 1;
      this.onopen?.();
    }
  }

  afterEach(() => vi.unstubAllGlobals());

  const connect = (key: string | null, api = openaiRealtime) => {
    vi.stubGlobal("WebSocket", FakeSocket);
    const seen = { open: 0, events: [] as ServerEvent[], closes: [] as unknown[] };
    const socket = openRealtime({
      api,
      model: "gpt realtime",
      mode: "conversation",
      key,
      onOpen: () => seen.open++,
      onEvent: (e) => seen.events.push(e),
      onClose: (info) => seen.closes.push(info),
    });
    return { socket, seen, fake: FakeSocket.last };
  };

  it("builds OpenAI Realtime's URL and sends the key as a subprotocol", () => {
    const openai = openaiRealtime;
    expect(openai.url("a/b", "k", { protocol: "https:", host: "gw.test" }, "conversation")).toBe(
      "wss://gw.test/v1/realtime?model=a%2Fb",
    );
    expect(openai.url("m", null, { protocol: "http:", host: "localhost:8080" }, "conversation")).toBe(
      "ws://localhost:8080/v1/realtime?model=m",
    );
    // A transcription is one from its connect.
    expect(openai.url("m", null, { protocol: "http:", host: "gw.test" }, "transcription")).toBe(
      "ws://gw.test/v1/realtime?model=m&intent=transcription",
    );
    expect(openai.protocols(null)).toEqual(["realtime"]);
    const { fake } = connect("sk-1");
    expect(fake.protocols).toEqual(["realtime", "openai-insecure-api-key.sk-1"]);
    expect(fake.url).toContain("/v1/realtime?model=gpt%20realtime");
  });

  it("puts Gemini Live's model and key in its query, and offers no subprotocol", () => {
    const { fake } = connect("sk-1", geminiLive);
    const url = new URL(fake.url);
    expect(url.pathname).toBe(
      "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
    );
    expect(url.searchParams.get("model")).toBe("gpt realtime");
    expect(url.searchParams.get("key")).toBe("sk-1");
    expect(fake.protocols).toEqual([]);
    expect(new URL(connect(null, geminiLive).fake.url).searchParams.has("key")).toBe(false);
  });

  it("delivers JSON events, drops anything else, and sends only once open", () => {
    const { socket, seen, fake } = connect(null);
    socket.send({ type: "early" });
    expect(fake.sent).toEqual([]);
    fake.open();
    expect(seen.open).toBe(1);
    socket.send({ type: "response.cancel" });
    expect(JSON.parse(fake.sent[0])).toEqual({ type: "response.cancel" });
    fake.onmessage?.({ data: '{"type":"session.created"}' });
    fake.onmessage?.({ data: "not json" });
    fake.onmessage?.({ data: new ArrayBuffer(2) });
    fake.onmessage?.({ data: new TextEncoder().encode('{"type":"session.updated"}').buffer });
    expect(seen.events).toEqual([{ type: "session.created" }, { type: "session.updated" }]);
  });

  it("gives up on a session nothing answers, once", () => {
    vi.useFakeTimers();
    try {
      const { seen, fake } = connect(null, geminiLive);
      vi.advanceTimersByTime(OPEN_TIMEOUT_MS);
      expect(seen.closes).toEqual([
        { opened: false, code: 0, reason: expect.stringMatching(/Nothing answered within 15 s/) },
      ]);
      fake.onclose?.({ code: 1006, reason: "" });
      expect(seen.closes).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not give up on a session that opened", () => {
    vi.useFakeTimers();
    try {
      const { seen, fake } = connect(null);
      fake.open();
      vi.advanceTimersByTime(OPEN_TIMEOUT_MS * 2);
      expect(seen.closes).toEqual([]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("tells a refused upgrade from a closed session", () => {
    const refused = connect(null);
    refused.fake.onclose?.({ code: 1006, reason: "" });
    expect(refused.seen.closes).toEqual([{ opened: false, code: 1006, reason: "" }]);

    const normal = connect(null);
    normal.fake.open();
    normal.socket.close();
    expect(normal.seen.closes).toEqual([{ opened: true, code: 1000, reason: "" }]);
  });
});
