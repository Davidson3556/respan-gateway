import { describe, expect, it } from "vitest";
import { peaksOf } from "./waveform";

describe("peaksOf", () => {
  it("takes each slice's loudest sample, scaled to the loudest slice", () => {
    const samples = new Float32Array([0.1, -0.2, 0.5, 0, -1, 0.3, 0, 0]);
    peaksOf(samples, 4).forEach((peak, i) => expect(peak).toBeCloseTo([0.2, 0.5, 1, 0][i]));
  });

  it("stays flat for silence", () => {
    expect(peaksOf(new Float32Array(8), 4)).toEqual([0, 0, 0, 0]);
  });

  it("copes with fewer samples than bars", () => {
    expect(peaksOf(new Float32Array([0.5]), 3)).toEqual([1, 0, 0]);
  });
});
