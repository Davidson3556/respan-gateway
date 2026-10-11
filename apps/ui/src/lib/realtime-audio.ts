// Audio helpers for the realtime playground: the OpenAI Realtime API speaks
// PCM16 mono at 24 kHz, base64 in JSON, while a browser captures floats at
// whatever rate the device runs.

export const REALTIME_RATE = 24_000;

/**
 * Resamples mono floats from one rate to another: each output sample is the
 * mean of the input it spans going down, and interpolated going up.
 */
export function resample(input: Float32Array, from: number, to: number): Float32Array {
  if (from === to || input.length === 0) return input;
  const ratio = from / to;
  const out = new Float32Array(Math.floor(input.length / ratio));
  for (let i = 0; i < out.length; i++) {
    if (ratio > 1) {
      const start = Math.floor(i * ratio);
      const end = Math.min(input.length, Math.max(start + 1, Math.floor((i + 1) * ratio)));
      let sum = 0;
      for (let j = start; j < end; j++) sum += input[j];
      out[i] = sum / (end - start);
    } else {
      const at = i * ratio;
      const j = Math.floor(at);
      const next = input[Math.min(j + 1, input.length - 1)];
      out[i] = input[j] + (next - input[j]) * (at - j);
    }
  }
  return out;
}

export function floatToPcm16(input: Float32Array): Int16Array {
  const out = new Int16Array(input.length);
  for (let i = 0; i < input.length; i++) {
    const v = Math.max(-1, Math.min(1, input[i]));
    out[i] = v < 0 ? v * 0x8000 : v * 0x7fff;
  }
  return out;
}

export function pcm16ToFloat(input: Int16Array): Float32Array {
  const out = new Float32Array(input.length);
  for (let i = 0; i < input.length; i++) out[i] = input[i] / (input[i] < 0 ? 0x8000 : 0x7fff);
  return out;
}

/** Little-endian bytes of `samples`, as base64. */
export function pcm16ToBase64(samples: Int16Array): string {
  const bytes = new Uint8Array(samples.length * 2);
  const view = new DataView(bytes.buffer);
  samples.forEach((v, i) => view.setInt16(i * 2, v, true));
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(binary);
}

export function base64ToPcm16(base64: string): Int16Array {
  const binary = atob(base64);
  const view = new DataView(new ArrayBuffer(binary.length - (binary.length % 2)));
  for (let i = 0; i < view.byteLength; i++) view.setUint8(i, binary.charCodeAt(i));
  const out = new Int16Array(view.byteLength / 2);
  for (let i = 0; i < out.length; i++) out[i] = view.getInt16(i * 2, true);
  return out;
}

/** Mic floats at the device rate to a base64 PCM16 chunk at 24 kHz. */
export function encodeMic(input: Float32Array, deviceRate: number): string {
  return pcm16ToBase64(floatToPcm16(resample(input, deviceRate, REALTIME_RATE)));
}

/** Milliseconds `samples` of 24 kHz audio last. */
export const pcmMs = (samples: number) => (samples / REALTIME_RATE) * 1000;

/** The AudioWorklet that hands the main thread the mic's raw 128-frame blocks. */
export const MIC_WORKLET = `
class MicTap extends AudioWorkletProcessor {
  process(inputs) {
    const channel = inputs[0] && inputs[0][0];
    if (channel) this.port.postMessage(channel.slice());
    return true;
  }
}
registerProcessor("mic-tap", MicTap);
`;
