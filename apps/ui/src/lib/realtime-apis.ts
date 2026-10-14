// The client APIs the realtime playground can speak. Each adapter says where
// its session is, what a client sends (its opening, the mic's audio, a cut),
// and reads what the server sends into OpenAI Realtime's events, which the
// page and its reducer read whatever the API.

import type { ServerEvent } from "@/lib/realtime";
import { REALTIME_RATE } from "@/lib/realtime-audio";

export type RealtimeApiId = "openai-realtime" | "gemini-live";

/** A message a client sends, as the API has it. */
export type ClientMessage = Record<string, unknown>;

export interface RealtimeApi {
  id: RealtimeApiId;
  label: string;
  /** The path its sessions open at, as the gateway serves it. */
  path: string;
  /** Its URL on the page's host, for `model`, carrying `key` if the API takes it there. */
  url: (model: string, key: string | null, location: Pick<Location, "protocol" | "host">) => string;
  /** The subprotocols it offers, carrying `key` if the API takes it there. */
  protocols: (key: string | null) => string[];
  /** What a client sends first, once the session is open. */
  opening: (model: string) => ClientMessage[];
  /** One chunk of the mic: base64 PCM16 mono at 24 kHz. */
  audio: (base64: string) => ClientMessage;
  /**
   * What tells the server the client stopped playing a reply, `heardMs` of it
   * heard; none for an API that keeps no record of what was heard.
   */
  truncate?: (reply: { id: string; index: number; heardMs: number }) => ClientMessage;
  /** What cancels the reply being made; none for an API that cannot. */
  cancel?: ClientMessage;
  /** A reader of one session's server messages, into OpenAI Realtime's events. */
  reader: () => (message: unknown) => ServerEvent[];
}

const socketUrl = (location: Pick<Location, "protocol" | "host">, path: string) =>
  `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}${path}`;

type Json = Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any

/**
 * OpenAI Realtime: `/v1/realtime?model=…`. A browser's WebSocket can set no
 * header, so the gateway key rides in a subprotocol, as OpenAI's own
 * browser clients send theirs.
 */
const openaiRealtime: RealtimeApi = {
  id: "openai-realtime",
  label: "OpenAI Realtime",
  path: "/v1/realtime",
  url: (model, _key, location) =>
    socketUrl(location, `/v1/realtime?model=${encodeURIComponent(model)}`),
  protocols: (key) => (key ? ["realtime", `openai-insecure-api-key.${key}`] : ["realtime"]),
  opening: () => [
    {
      type: "session.update",
      session: {
        type: "realtime",
        audio: { input: { transcription: { model: "gpt-4o-mini-transcribe" } } },
      },
    },
  ],
  audio: (audio) => ({ type: "input_audio_buffer.append", audio }),
  truncate: ({ id, index, heardMs }) => ({
    type: "conversation.item.truncate",
    item_id: id,
    content_index: index,
    audio_end_ms: heardMs,
  }),
  cancel: { type: "response.cancel" },
  reader: () => (message) => [message as ServerEvent],
};

const GEMINI_PATH = "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

/**
 * Gemini Live, at its own path, its model also on `?model=`, which routes
 * it, and the gateway key on `?key=`, as Google's own clients send theirs.
 * Its turns have no ids, so the reader numbers them: the user's line and
 * the model's of turn `n` are `user-n` and `model-n`. Its server notices a
 * barge-in itself and says `interrupted`; a client can neither cancel a
 * reply nor say how much of it was heard.
 */
const geminiLive: RealtimeApi = {
  id: "gemini-live",
  label: "Gemini Live",
  path: GEMINI_PATH,
  url: (model, key, location) => {
    const query = new URLSearchParams({ model });
    if (key) query.set("key", key);
    return socketUrl(location, `${GEMINI_PATH}?${query}`);
  },
  protocols: () => [],
  opening: (model) => [
    {
      setup: {
        model: `models/${model}`,
        generationConfig: { responseModalities: ["AUDIO"] },
        inputAudioTranscription: {},
        outputAudioTranscription: {},
      },
    },
  ],
  audio: (data) => ({
    realtimeInput: { audio: { mimeType: `audio/pcm;rate=${REALTIME_RATE}`, data } },
  }),
  reader: () => {
    let turn = 0;
    let begun = false;
    let answering = false;
    return (message) => {
      const m = message as Json;
      const content = m.serverContent as Json | undefined;
      if (!content) return [];
      const user = `user-${turn}`;
      const model = `model-${turn}`;
      const out: ServerEvent[] = [];
      if (!begun) {
        // The user's line first, even if its transcript comes after the reply.
        out.push({ type: "conversation.item.created", item: { id: user, role: "user" } });
        begun = true;
      }
      const heard = content.inputTranscription?.text;
      if (typeof heard === "string" && heard) {
        out.push({ type: "conversation.item.input_audio_transcription.delta", item_id: user, delta: heard });
      }
      if (content.interrupted === true) {
        out.push({ type: "input_audio_buffer.speech_started" });
      }
      const parts: Json[] = content.modelTurn?.parts ?? [];
      for (const part of parts) {
        if (!answering && (part.inlineData || part.text)) {
          out.push({ type: "input_audio_buffer.speech_stopped" });
          out.push({ type: "conversation.item.created", item: { id: model, role: "assistant" } });
          answering = true;
        }
        const audio = part.inlineData?.data;
        if (typeof audio === "string" && String(part.inlineData?.mimeType ?? "").startsWith("audio/")) {
          out.push({ type: "response.output_audio.delta", item_id: model, content_index: 0, delta: audio });
        }
        if (typeof part.text === "string" && part.text && part.thought !== true) {
          out.push({ type: "response.output_text.delta", item_id: model, delta: part.text });
        }
      }
      const said = content.outputTranscription?.text;
      if (typeof said === "string" && said) {
        out.push({ type: "response.output_audio_transcript.delta", item_id: model, delta: said });
      }
      if (content.turnComplete === true || content.interrupted === true) {
        const status = content.interrupted === true ? "cancelled" : "completed";
        out.push({ type: "response.done", response: { status } });
        turn += 1;
        begun = false;
        answering = false;
      }
      return out;
    };
  },
};

export const realtimeApis: RealtimeApi[] = [openaiRealtime, geminiLive];

export function realtimeApi(id: RealtimeApiId): RealtimeApi {
  return realtimeApis.find((a) => a.id === id) ?? openaiRealtime;
}

/** The APIs the gateway serves sessions in; OpenAI Realtime if it lists none. */
export function servedRealtimeApis(
  serve: { format: string; kind: string }[] | undefined,
): RealtimeApi[] {
  const served = realtimeApis.filter((a) => serve?.some((s) => s.format === a.id));
  return served.length > 0 ? served : [openaiRealtime];
}
