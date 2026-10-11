import { useEffect, useState } from "react";

/** The volume of `stream`, 0 to 1, refreshed every frame; 0 once it is null. */
export function useMicLevel(stream: MediaStream | null): number {
  const [level, setLevel] = useState(0);
  useEffect(() => {
    if (!stream || typeof AudioContext === "undefined") return;
    const context = new AudioContext();
    const analyser = context.createAnalyser();
    analyser.fftSize = 512;
    context.createMediaStreamSource(stream).connect(analyser);
    const samples = new Uint8Array(analyser.fftSize);
    let frame = 0;
    const tick = () => {
      analyser.getByteTimeDomainData(samples);
      const sum = samples.reduce((total, v) => total + ((v - 128) / 128) ** 2, 0);
      setLevel(Math.min(1, Math.sqrt(sum / samples.length) * 4));
      frame = requestAnimationFrame(tick);
    };
    tick();
    return () => {
      cancelAnimationFrame(frame);
      void context.close();
      setLevel(0);
    };
  }, [stream]);
  return stream ? level : 0;
}
