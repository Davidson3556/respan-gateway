import { describe, expect, it } from "vitest";
import {
  base64ToPcm16,
  encodeMic,
  floatToPcm16,
  pcm16ToBase64,
  pcm16ToFloat,
  pcmMs,
  resample,
} from "./realtime-audio";

describe("resample", () => {
  it("returns the input when the rates match", () => {
    const input = new Float32Array([0.1, 0.2]);
    expect(resample(input, 24_000, 24_000)).toBe(input);
  });

  it("averages down: 48 kHz to 24 kHz halves the length", () => {
    const out = resample(new Float32Array([0, 1, 1, 0, 0.5, 0.5]), 48_000, 24_000);
    expect(Array.from(out)).toEqual([0.5, 0.5, 0.5]);
  });

  it("downsamples a 44.1 kHz second to about a 24 kHz second", () => {
    const out = resample(new Float32Array(44_100).fill(0.25), 44_100, 24_000);
    expect(out.length).toBe(24_000);
    expect(out.every((v) => Math.abs(v - 0.25) < 1e-6)).toBe(true);
  });

  it("keeps a sine's frequency when downsampling", () => {
    const rate = 48_000;
    const sine = Float32Array.from({ length: rate / 10 }, (_, i) => Math.sin((2 * Math.PI * 440 * i) / rate));
    const out = resample(sine, rate, 24_000);
    let crossings = 0;
    for (let i = 1; i < out.length; i++) if (out[i - 1] < 0 !== out[i] < 0) crossings++;
    // 0.1 s of 440 Hz is 44 cycles, 88 zero crossings.
    expect(Math.abs(crossings - 88)).toBeLessThanOrEqual(2);
  });

  it("interpolates up: 8 kHz to 24 kHz triples the length", () => {
    const out = resample(new Float32Array([0, 0.3]), 8_000, 24_000);
    expect(out.length).toBe(6);
    expect(out[0]).toBe(0);
    expect(out[1]).toBeCloseTo(0.1);
  });

  it("copes with empty input", () => {
    expect(resample(new Float32Array(0), 48_000, 24_000)).toHaveLength(0);
  });
});

describe("PCM16", () => {
  it("clamps and scales floats", () => {
    expect(Array.from(floatToPcm16(new Float32Array([0, 1, -1, 2, -2, 0.5])))).toEqual([
      0, 32767, -32768, 32767, -32768, 16383,
    ]);
  });

  it("round-trips floats within a quantisation step", () => {
    const input = new Float32Array([0, 0.25, -0.25, 0.9, -0.9]);
    const back = pcm16ToFloat(floatToPcm16(input));
    input.forEach((v, i) => expect(back[i]).toBeCloseTo(v, 3));
  });

  it("round-trips through base64, little-endian", () => {
    const samples = new Int16Array([0, 1, -1, 256, 32767, -32768]);
    const b64 = pcm16ToBase64(samples);
    // 1 is bytes 01 00; 256 is 00 01.
    expect(atob(b64).charCodeAt(2)).toBe(1);
    expect(atob(b64).charCodeAt(3)).toBe(0);
    expect(Array.from(base64ToPcm16(b64))).toEqual(Array.from(samples));
  });

  it("encodes a large chunk without overflowing the call stack", () => {
    const samples = new Int16Array(200_000).fill(7);
    expect(base64ToPcm16(pcm16ToBase64(samples))).toHaveLength(200_000);
  });

  it("drops a trailing odd byte", () => {
    expect(base64ToPcm16(btoa("\x01\x00\x02"))).toHaveLength(1);
  });
});

describe("encodeMic", () => {
  it("turns 40 ms of 48 kHz audio into 40 ms of 24 kHz PCM16", () => {
    const samples = base64ToPcm16(encodeMic(new Float32Array(1920).fill(0.5), 48_000));
    expect(samples).toHaveLength(960);
    expect(pcmMs(samples.length)).toBe(40);
    expect(samples[0]).toBe(16383);
  });
});
