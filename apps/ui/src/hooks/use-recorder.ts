import { useEffect, useRef, useState } from "react";

export type RecorderState = "idle" | "recording" | "unsupported";

/** Records microphone audio with MediaRecorder; `onDone` gets the clip as a File. */
export function useRecorder(onDone: (file: File) => void) {
  const supported = typeof window !== "undefined" && "MediaRecorder" in window;
  const [state, setState] = useState<RecorderState>(supported ? "idle" : "unsupported");
  const [seconds, setSeconds] = useState(0);
  const recorder = useRef<MediaRecorder | null>(null);
  const done = useRef(onDone);

  useEffect(() => {
    done.current = onDone;
  });

  useEffect(() => {
    if (state !== "recording") return;
    const started = Date.now();
    const id = setInterval(() => setSeconds(Math.floor((Date.now() - started) / 1000)), 250);
    return () => clearInterval(id);
  }, [state]);

  // Leaving the page mid-recording releases the microphone and drops the clip.
  useEffect(
    () => () => {
      const rec = recorder.current;
      if (!rec) return;
      rec.onstop = null;
      rec.stop();
      rec.stream.getTracks().forEach((t) => t.stop());
    },
    [],
  );

  const start = async () => {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const rec = new MediaRecorder(stream);
    const chunks: Blob[] = [];
    rec.ondataavailable = (e) => chunks.push(e.data);
    rec.onstop = () => {
      stream.getTracks().forEach((t) => t.stop());
      recorder.current = null;
      setState("idle");
      const type = rec.mimeType || "audio/webm";
      const ext = type.includes("ogg") ? "ogg" : type.includes("mp4") ? "m4a" : "webm";
      done.current(new File(chunks, `recording.${ext}`, { type }));
    };
    recorder.current = rec;
    rec.start();
    setSeconds(0);
    setState("recording");
  };

  const stop = () => recorder.current?.stop();

  return { state, seconds, start, stop };
}
