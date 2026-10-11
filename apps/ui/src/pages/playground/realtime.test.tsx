import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { setGatewayKey } from "@/lib/api";
import { json, renderApp, serve } from "@/test/render";
import { RealtimeTab } from "./realtime";

const mic = vi.hoisted(() => ({ stop: vi.fn(), onChunk: null as null | ((b64: string) => void) }));
const player = vi.hoisted(() => ({ play: vi.fn(), stop: vi.fn(() => 120), close: vi.fn() }));
vi.mock("@/lib/realtime-io", () => ({
  startMic: vi.fn(async (onChunk: (b64: string) => void) => {
    mic.onChunk = onChunk;
    return { stop: mic.stop };
  }),
  Player: class {
    play = player.play;
    stop = player.stop;
    close = player.close;
  },
}));

class FakeSocket {
  static OPEN = 1;
  static last: FakeSocket;
  readyState = 0;
  sent: Record<string, unknown>[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  onclose: ((e: { code: number; reason: string }) => void) | null = null;
  constructor(
    readonly url: string,
    readonly protocols: string[],
  ) {
    FakeSocket.last = this;
  }
  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  close(code = 1000) {
    this.onclose?.({ code, reason: "" });
  }
  open() {
    this.readyState = 1;
    this.onopen?.();
  }
  emit(event: unknown) {
    this.onmessage?.({ data: JSON.stringify(event) });
  }
}

const modelsWithRealtime = () =>
  json({ object: "list", data: [{ id: "gpt-realtime", object: "model", owned_by: "openai", kind: "realtime" }] });
const routes = (realtime: boolean) => () =>
  json({
    serve: [],
    routes: [],
    defaults: realtime ? [{ kind: "realtime", provider: "openai" }] : [{ kind: "chat", provider: "openai" }],
  });

beforeEach(() => {
  vi.stubGlobal("WebSocket", FakeSocket);
  player.play.mockClear();
  player.stop.mockClear();
  mic.stop.mockClear();
});
afterEach(() => setGatewayKey(null));

describe("RealtimeTab", () => {
  it("says so when no provider serves realtime", async () => {
    serve({
      "GET /v1/models": () => json({ object: "list", data: [{ id: "gpt-4o", object: "model", owned_by: "o", kind: "chat" }] }),
      "GET /ui/api/routes": routes(false),
    });
    renderApp(<RealtimeTab />);
    expect(await screen.findByText("No realtime provider")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /start/i })).not.toBeInTheDocument();
  });

  it("runs a session: key as subprotocol, mic out, audio in, transcripts, barge-in", async () => {
    setGatewayKey("sk-gw");
    serve({ "GET /v1/models": modelsWithRealtime, "GET /ui/api/routes": routes(true) });
    renderApp(<RealtimeTab />);
    await userEvent.click(await screen.findByRole("button", { name: "Start session" }));

    const ws = FakeSocket.last;
    expect(ws.url).toContain("/v1/realtime?model=gpt-realtime");
    expect(ws.protocols).toEqual(["realtime", "openai-insecure-api-key.sk-gw"]);
    expect(screen.getByRole("status")).toHaveTextContent("Connecting");

    await act(async () => ws.open());
    expect(screen.getByRole("status")).toHaveTextContent("Listening");
    expect(ws.sent[0]).toMatchObject({ type: "session.update" });
    act(() => mic.onChunk?.("AAAA"));
    expect(ws.sent.at(-1)).toEqual({ type: "input_audio_buffer.append", audio: "AAAA" });

    act(() => {
      ws.emit({ type: "input_audio_buffer.speech_started" });
      ws.emit({ type: "conversation.item.created", item: { id: "u1", role: "user" } });
      ws.emit({
        type: "conversation.item.input_audio_transcription.completed",
        item_id: "u1",
        transcript: "What time is it?",
      });
    });
    expect(screen.getByRole("status")).toHaveTextContent("You're speaking");
    expect(screen.getByText("What time is it?")).toBeInTheDocument();

    act(() => {
      ws.emit({ type: "input_audio_buffer.speech_stopped" });
      ws.emit({ type: "response.output_audio.delta", item_id: "a1", content_index: 0, delta: "AAAA" });
      ws.emit({ type: "response.output_audio_transcript.delta", item_id: "a1", delta: "It is noon." });
    });
    expect(player.play).toHaveBeenCalledWith("AAAA");
    expect(screen.getByRole("status")).toHaveTextContent("Model is speaking");
    expect(screen.getByText("It is noon.")).toBeInTheDocument();

    // Speaking over the model stops playback and tells the server how much was heard.
    act(() => ws.emit({ type: "input_audio_buffer.speech_started" }));
    expect(player.stop).toHaveBeenCalled();
    expect(ws.sent.at(-1)).toEqual({
      type: "conversation.item.truncate",
      item_id: "a1",
      content_index: 0,
      audio_end_ms: 120,
    });
    expect(screen.getByText("interrupted")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /end session/i }));
    expect(mic.stop).toHaveBeenCalled();
    expect(screen.getByRole("status")).toHaveTextContent("Session ended");
  });

  it("explains a session the gateway refused", async () => {
    serve({ "GET /v1/models": modelsWithRealtime, "GET /ui/api/routes": routes(true) });
    renderApp(<RealtimeTab />);
    await userEvent.click(await screen.findByRole("button", { name: "Start session" }));
    act(() => FakeSocket.last.onclose?.({ code: 1006, reason: "" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(/refused the session/);
    expect(screen.getByRole("button", { name: /start a new session/i })).toBeInTheDocument();
  });
});
