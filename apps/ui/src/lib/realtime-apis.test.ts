import { describe, expect, it } from "vitest";
import { initialRealtime, realtimeReducer, type RealtimeState } from "./realtime";
import type { ServedFormat } from "./api";
import { servedApis } from "./playground-apis";
import { geminiLive, realtimeApis } from "./realtime-apis";

/** The state a session reaches from Gemini Live's `messages`, as the page reads them. */
function geminiSession(...messages: unknown[]): RealtimeState {
  const read = geminiLive.reader();
  const live: RealtimeState = { ...initialRealtime, phase: "live" };
  return messages
    .flatMap((m) => read(m))
    .reduce<RealtimeState>((state, event) => realtimeReducer(state, { type: "event", event }), live);
}

const lines = (state: RealtimeState) =>
  state.lines.map(({ role, text, done, interrupted }) => ({ role, text, done, interrupted }));

describe("Gemini Live", () => {
  it("opens with a setup that asks for audio and both transcripts", () => {
    const gemini = geminiLive;
    expect(gemini.opening("gemini-live-2.5-flash")).toEqual([
      {
        setup: {
          model: "models/gemini-live-2.5-flash",
          generationConfig: { responseModalities: ["AUDIO"] },
          inputAudioTranscription: {},
          outputAudioTranscription: {},
        },
      },
    ]);
    expect(gemini.audio("AAAA")).toEqual({
      realtimeInput: { audio: { mimeType: "audio/pcm;rate=24000", data: "AAAA" } },
    });
    expect(gemini.cancel).toBeUndefined();
  });

  it("reads a turn into the user's line, then the model's, in order", () => {
    const state = geminiSession(
      { setupComplete: {} },
      { serverContent: { inputTranscription: { text: "What time" } } },
      { serverContent: { modelTurn: { parts: [{ inlineData: { mimeType: "audio/pcm;rate=24000", data: "AAAA" } }] } } },
      { serverContent: { inputTranscription: { text: " is it?" } } },
      { serverContent: { outputTranscription: { text: "It is noon." } } },
      { serverContent: { turnComplete: true } },
    );
    expect(lines(state)).toEqual([
      { role: "user", text: "What time is it?", done: false, interrupted: false },
      { role: "assistant", text: "It is noon.", done: true, interrupted: false },
    ]);
    expect(state.speaker).toBeNull();
  });

  it("plays the model's audio as OpenAI's deltas, under its turn's id", () => {
    const read = geminiLive.reader();
    const audio = { inlineData: { mimeType: "audio/pcm;rate=24000", data: "AAAA" } };
    const events = read({ serverContent: { modelTurn: { parts: [audio] } } });
    expect(events.find((e) => e.type === "response.output_audio.delta")).toEqual({
      type: "response.output_audio.delta",
      item_id: "model-0",
      content_index: 0,
      delta: "AAAA",
    });
  });

  it("marks the model's line interrupted when the user speaks over it, and starts a new turn", () => {
    const state = geminiSession(
      { serverContent: { inputTranscription: { text: "Tell me a story" } } },
      { serverContent: { outputTranscription: { text: "Once upon" } } },
      { serverContent: { interrupted: true } },
      // As Gemini sends it: the interrupted turn's end, after it.
      { serverContent: { turnComplete: true } },
      { serverContent: { inputTranscription: { text: "Stop" } } },
    );
    expect(lines(state)).toEqual([
      { role: "user", text: "Tell me a story", done: false, interrupted: false },
      { role: "assistant", text: "Once upon", done: true, interrupted: true },
      { role: "user", text: "Stop", done: false, interrupted: false },
    ]);
    expect(state.speaker).toBe("user");
  });

  it("gives a reply's late transcript to that reply, not a new turn", () => {
    const state = geminiSession(
      { serverContent: { inputTranscription: { text: "Hi" } } },
      { serverContent: { outputTranscription: { text: "Hello" } } },
      { serverContent: { turnComplete: true } },
      { serverContent: { outputTranscription: { text: " there" } } },
    );
    expect(lines(state)).toEqual([
      { role: "user", text: "Hi", done: false, interrupted: false },
      { role: "assistant", text: "Hello there", done: true, interrupted: false },
    ]);
  });

  it("ignores what is no server content, and a model's thoughts", () => {
    const read = geminiLive.reader();
    expect(read({ setupComplete: {} })).toEqual([]);
    expect(read({ usageMetadata: { totalTokenCount: 3 } })).toEqual([]);
    const thought = read({ serverContent: { modelTurn: { parts: [{ text: "planning", thought: true }] } } });
    expect(thought.some((e) => e.type === "response.output_text.delta")).toBe(false);
  });
});

describe("servedApis for realtime", () => {
  it("offers the APIs the gateway serves, else OpenAI Realtime", () => {
    const ids = (serve: Pick<ServedFormat, "format" | "kind">[] | undefined) =>
      servedApis("realtime", realtimeApis, serve).map((a) => a.id);
    expect(ids([{ format: "gemini-live", kind: "realtime" }, { format: "openai-realtime", kind: "realtime" }])).toEqual(
      ["openai-realtime", "gemini-live"],
    );
    expect(ids([{ format: "gemini-live", kind: "realtime" }])).toEqual(["gemini-live"]);
    expect(ids(undefined)).toEqual(["openai-realtime"]);
  });
});
