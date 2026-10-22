// A realtime voice session against the gateway: the WebSocket, in any API
// the playground speaks (realtime-apis), and the reducer that turns its
// events, as OpenAI Realtime's, into what the page shows. The audio itself
// is handled beside this, in realtime-io.

import type { ClientMessage, RealtimeApi, RealtimeMode } from "@/lib/realtime-apis";

export interface ServerEvent {
  type: string;
  [field: string]: unknown;
}

// --- the socket -------------------------------------------------------------

/** How long a session may take to open before it is given up. */
export const OPEN_TIMEOUT_MS = 15_000;

export interface RealtimeSocket {
  send: (message: ClientMessage) => void;
  close: () => void;
}

/**
 * A session in `api` on the page's own host. `onEvent` gets what the server
 * sends as OpenAI Realtime's events, whatever the API; a server may send its
 * JSON in text or binary frames.
 */
export function openRealtime({
  api,
  model,
  mode,
  key,
  onOpen,
  onEvent,
  onClose,
}: {
  api: RealtimeApi;
  model: string;
  mode: RealtimeMode;
  key: string | null;
  onOpen: () => void;
  onEvent: (event: ServerEvent) => void;
  /** `opened` is false when the gateway refused the upgrade. */
  onClose: (info: { opened: boolean; code: number; reason: string }) => void;
}): RealtimeSocket {
  const socket = new WebSocket(api.url(model, key, window.location, mode), api.protocols(key));
  socket.binaryType = "arraybuffer";
  const read = api.reader();
  const text = new TextDecoder();
  let opened = false;
  let ended = false;
  const end = (info: { opened: boolean; code: number; reason: string }) => {
    if (ended) return;
    ended = true;
    clearTimeout(giveUp);
    onClose(info);
  };
  // Nothing answering the upgrade, as a proxy that does not forward it, would
  // leave the session connecting for good.
  const giveUp = setTimeout(() => {
    end({
      opened: false,
      code: 0,
      reason: `Nothing answered within ${OPEN_TIMEOUT_MS / 1000} s. Check that the gateway (or the dev server's proxy) serves ${api.label} at ${api.path}.`,
    });
    socket.close();
  }, OPEN_TIMEOUT_MS);
  socket.onopen = () => {
    opened = true;
    clearTimeout(giveUp);
    onOpen();
  };
  socket.onmessage = (message) => {
    let parsed: unknown;
    try {
      const data: unknown = message.data;
      parsed = JSON.parse(typeof data === "string" ? data : text.decode(data as ArrayBuffer));
    } catch {
      return; // Not JSON text; neither protocol sends anything else.
    }
    read(parsed).forEach(onEvent);
  };
  socket.onclose = (e) => end({ opened, code: e.code, reason: e.reason });
  return {
    send: (message) => {
      if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
    },
    close: () => socket.close(1000),
  };
}

// --- the transcript ---------------------------------------------------------

export type Phase = "idle" | "connecting" | "live" | "ended" | "failed";

export interface Line {
  id: string;
  role: "user" | "assistant";
  text: string;
  /** The model finished this line; false while it is still being said. */
  done: boolean;
  /** The user spoke over it. */
  interrupted: boolean;
}

export interface RealtimeState {
  phase: Phase;
  lines: Line[];
  /** Who is speaking right now, as the server hears or sends it. */
  speaker: "user" | "assistant" | null;
  error: string | null;
}

export type RealtimeAction =
  | { type: "connecting" }
  | { type: "open" }
  | { type: "close"; error?: string }
  | { type: "event"; event: ServerEvent };

export const initialRealtime: RealtimeState = {
  phase: "idle",
  lines: [],
  speaker: null,
  error: null,
};

const str = (value: unknown) => (typeof value === "string" ? value : "");

function upsert(
  lines: Line[],
  id: string,
  role: Line["role"],
  change: (line: Line) => Line,
): Line[] {
  const at = lines.findIndex((l) => l.id === id);
  if (at === -1) {
    return [...lines, change({ id, role, text: "", done: false, interrupted: false })];
  }
  return lines.map((l, i) => (i === at ? change(l) : l));
}

export function realtimeReducer(state: RealtimeState, action: RealtimeAction): RealtimeState {
  switch (action.type) {
    case "connecting":
      return { ...initialRealtime, phase: "connecting" };
    case "open":
      return { ...state, phase: "live" };
    case "close":
      return {
        ...state,
        phase: action.error ? "failed" : "ended",
        error: action.error ?? state.error,
        speaker: null,
        lines: state.lines.map((l) => ({ ...l, done: true })),
      };
    case "event":
      return onEvent(state, action.event);
  }
}

function cancelled(event: ServerEvent): boolean {
  return (event.response as { status?: string } | undefined)?.status === "cancelled";
}

function onEvent(state: RealtimeState, event: ServerEvent): RealtimeState {
  const itemId = str(event.item_id);
  switch (event.type) {
    case "conversation.item.created":
    case "conversation.item.added": {
      const item = event.item as { id?: string; role?: string } | undefined;
      if (!item?.id || (item.role !== "user" && item.role !== "assistant")) return state;
      const role = item.role;
      return { ...state, lines: upsert(state.lines, item.id, role, (l) => l) };
    }
    case "input_audio_buffer.speech_started":
      // Speaking over the model cuts it off: whatever it was saying is done.
      return {
        ...state,
        speaker: "user",
        lines: state.lines.map((l) =>
          l.role === "assistant" && !l.done ? { ...l, done: true, interrupted: true } : l,
        ),
      };
    case "conversation.item.truncated":
      // The client cut the reply short (Interrupt): what it had said stays.
      return {
        ...state,
        lines: state.lines.map((l) => (l.id === itemId ? { ...l, done: true, interrupted: true } : l)),
      };
    case "input_audio_buffer.speech_stopped":
      return state.speaker === "user" ? { ...state, speaker: null } : state;
    case "conversation.item.input_audio_transcription.delta":
      return {
        ...state,
        lines: upsert(state.lines, itemId, "user", (l) => ({ ...l, text: l.text + str(event.delta) })),
      };
    case "conversation.item.input_audio_transcription.completed":
      return {
        ...state,
        lines: upsert(state.lines, itemId, "user", (l) => ({
          ...l,
          text: str(event.transcript).trim(),
          done: true,
        })),
      };
    case "response.output_audio_transcript.delta":
    case "response.audio_transcript.delta":
    case "response.output_text.delta":
    case "response.text.delta":
      return {
        ...state,
        lines: upsert(state.lines, itemId, "assistant", (l) =>
          l.interrupted ? l : { ...l, text: l.text + str(event.delta) },
        ),
      };
    case "response.output_audio_transcript.done":
    case "response.audio_transcript.done":
      return {
        ...state,
        lines: upsert(state.lines, itemId, "assistant", (l) =>
          l.interrupted ? l : { ...l, text: str(event.transcript) || l.text, done: true },
        ),
      };
    case "response.output_audio.delta":
    case "response.audio.delta":
      return state.speaker === "user" ? state : { ...state, speaker: "assistant" };
    case "response.output_audio.done":
    case "response.audio.done":
    case "response.done":
      return {
        ...state,
        speaker: state.speaker === "assistant" ? null : state.speaker,
        lines:
          event.type === "response.done"
            ? state.lines.map((l) => {
                if (l.role !== "assistant") return l;
                const cut = !l.done && cancelled(event);
                return { ...l, done: true, interrupted: l.interrupted || cut };
              })
            : state.lines,
      };
    case "error": {
      const error = event.error as { message?: string } | undefined;
      return { ...state, error: error?.message ?? "The realtime session reported an error" };
    }
    default:
      return state;
  }
}
