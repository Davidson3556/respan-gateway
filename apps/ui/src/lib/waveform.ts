/** The loudest sample in each of `bars` equal slices of `samples`, scaled so the loudest bar is 1. */
export function peaksOf(samples: Float32Array, bars: number): number[] {
  const size = Math.max(1, Math.floor(samples.length / bars));
  const peaks = Array.from({ length: bars }, (_, bar) => {
    let peak = 0;
    for (let i = bar * size; i < Math.min(samples.length, (bar + 1) * size); i++) {
      peak = Math.max(peak, Math.abs(samples[i]));
    }
    return peak;
  });
  const loudest = Math.max(...peaks);
  return loudest > 0 ? peaks.map((p) => p / loudest) : peaks;
}

/** The peaks of an audio file, or null if the browser can't decode it. */
export async function decodePeaks(blob: Blob, bars: number): Promise<number[] | null> {
  try {
    const context = new AudioContext();
    try {
      const audio = await context.decodeAudioData(await blob.arrayBuffer());
      return peaksOf(audio.getChannelData(0), bars);
    } finally {
      void context.close();
    }
  } catch {
    return null;
  }
}
