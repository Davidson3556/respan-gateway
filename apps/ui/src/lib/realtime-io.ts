// The browser's side of a realtime session: the mic into 24 kHz PCM16 chunks
// and the model's audio back out, both through one AudioContext, so the
// browser's echo canceller hears what is played and takes it out of the mic.
// Needs a real AudioContext, so it is exercised in the browser; the maths it
// leans on is in realtime-audio.

import {
  MIC_WORKLET,
  REALTIME_RATE,
  base64ToPcm16,
  encodeMic,
  pcm16ToFloat,
  pcmMs,
} from "@/lib/realtime-audio";

/** Seconds of mic audio sent per chunk. */
const CHUNK_SECONDS = 0.04;

export interface Mic {
  stop: () => void;
}

/**
 * Captures the mic, in `player`'s AudioContext, and calls `onChunk` with
 * base64 PCM16 at 24 kHz, ~40 ms apiece.
 */
export async function startMic(
  player: Player,
  onChunk: (base64: string) => void,
  onLevel: (level: number) => void,
): Promise<Mic> {
  const stream = await navigator.mediaDevices.getUserMedia({
    audio: {
      channelCount: 1,
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
    },
  });
  // What the browser applied, which may not be what was asked: without echo
  // cancellation, the speaker's audio comes back through the mic.
  console.info("[realtime] microphone settings", stream.getAudioTracks()[0]?.getSettings());
  const { context } = player;
  const url = URL.createObjectURL(new Blob([MIC_WORKLET], { type: "text/javascript" }));
  try {
    await context.audioWorklet.addModule(url);
  } catch (error) {
    stream.getTracks().forEach((t) => t.stop());
    throw error;
  } finally {
    URL.revokeObjectURL(url);
  }
  const tap = new AudioWorkletNode(context, "mic-tap");
  const wanted = Math.round(context.sampleRate * CHUNK_SECONDS);
  let pending: Float32Array[] = [];
  let count = 0;
  tap.port.onmessage = (e: MessageEvent<Float32Array>) => {
    pending.push(e.data);
    count += e.data.length;
    if (count < wanted) return;
    const chunk = new Float32Array(count);
    let at = 0;
    for (const part of pending) {
      chunk.set(part, at);
      at += part.length;
    }
    pending = [];
    count = 0;
    let sum = 0;
    for (const v of chunk) sum += v * v;
    onLevel(Math.min(1, Math.sqrt(sum / chunk.length) * 4));
    onChunk(encodeMic(chunk, context.sampleRate));
  };
  const source = context.createMediaStreamSource(stream);
  source.connect(tap);
  return {
    stop() {
      tap.port.onmessage = null;
      source.disconnect();
      tap.disconnect();
      stream.getTracks().forEach((t) => t.stop());
      onLevel(0);
    },
  };
}

/**
 * Plays the model's audio deltas gaplessly, and can cut them off. Its
 * AudioContext, at the device's own rate, is the session's: the mic is
 * captured in it too ([`startMic`]).
 */
export class Player {
  readonly context = new AudioContext();
  private sources = new Set<AudioBufferSourceNode>();
  private next = 0;
  private began = 0;
  private samples = 0;

  play(base64: string): void {
    const pcm = base64ToPcm16(base64);
    if (pcm.length === 0) return;
    const buffer = this.context.createBuffer(1, pcm.length, REALTIME_RATE);
    buffer.copyToChannel(pcm16ToFloat(pcm) as Float32Array<ArrayBuffer>, 0);
    const source = this.context.createBufferSource();
    source.buffer = buffer;
    source.connect(this.context.destination);
    const now = this.context.currentTime;
    if (this.sources.size === 0) {
      this.next = now + 0.05;
      this.began = this.next;
      this.samples = 0;
    }
    source.start(this.next);
    this.next += buffer.duration;
    this.samples += pcm.length;
    this.sources.add(source);
    source.onended = () => this.sources.delete(source);
  }

  /** Whether any audio is queued or playing. */
  get playing(): boolean {
    return this.sources.size > 0;
  }

  /** Stops everything queued; returns how many ms of the response were heard. */
  stop(): number {
    const heard = Math.max(0, (this.context.currentTime - this.began) * 1000);
    const heardMs = Math.min(heard, pcmMs(this.samples));
    this.sources.forEach((s) => {
      s.onended = null;
      try {
        s.stop();
      } catch {
        // Already finished.
      }
    });
    this.sources.clear();
    this.samples = 0;
    return Math.round(heardMs);
  }

  close(): void {
    this.stop();
    void this.context.close();
  }
}
