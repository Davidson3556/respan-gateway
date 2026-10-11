// A realtime voice session against the gateway's OpenAI Realtime endpoint:
// the WebSocket, and the reducer that turns its events into what the page
// shows. The audio itself is handled beside this, in realtime-audio.

export interface ServerEvent {
  type: string;
  [field: string]: unknown;
}

// --- the socket -------------------------------------------------------------

/** `/v1/realtime?model=…` on the page's own host, over ws or wss. */
export function realtimeUrl(model: string, location: Pick<Location, "protocol" | "host">): string {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${location.host}/v1/realtime?model=${encodeURIComponent(model)}`;
}

/**
 * A browser's WebSocket can set no header, so the gateway key rides in a
 * subprotocol, as OpenAI's own browser clients send theirs.
 */
export function realtimeProtocols(key: string | null): string[] {
  return key ? ["realtime", `openai-insecure-api-key.${key}`] : ["realtime"];
}

export interface RealtimeSocket {
  send: (event: ServerEvent) => void;
  close: () => void;
}

export function openRealtime({
  model,
  key,
  onOpen,
  onEvent,
  onClose,
}: {
  model: string;
  key: string | null;
  onOpen: () => void;
  onEvent: (event: ServerEvent) => void;
  /** `opened` is false when the gateway refused the upgrade. */
  onClose: (info: { opened: boolean; code: number; reason: string }) => void;
}): RealtimeSocket {
  const socket = new WebSocket(realtimeUrl(model, window.location), realtimeProtocols(key));
  let opened = false;
  socket.onopen = () => {
    opened = true;
    onOpen();
  };
  socket.onmessage = (message) => {
    if (typeof message.data !== "string") return;
    try {
      onEvent(JSON.parse(message.data) as ServerEvent);
    } catch {
      // Not JSON; the protocol sends nothing else.
    }
  };
  socket.onclose = (e) => onClose({ opened, code: e.code, reason: e.reason });
  return {
    send: (event) => {
      if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(event));
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
    case "input_audio_buffer.speech_stopped":
      return state.speaker === "user" ? { ...state, speaker: null } : state;
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
            ? state.lines.map((l) => (l.role === "assistant" ? { ...l, done: true } : l))
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
