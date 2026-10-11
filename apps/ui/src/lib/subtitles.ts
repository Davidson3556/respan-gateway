export interface Cue {
  /** Seconds from the start of the audio. */
  start: number;
  end: number;
  text: string;
}

/** "00:01:02,500" or "01:02.500" as seconds. */
function seconds(stamp: string): number {
  const parts = stamp.trim().replace(",", ".").split(":").map(Number);
  return parts.reduce((total, part) => total * 60 + part, 0);
}

/** The cues of an SRT or WebVTT document; blocks without a time range are skipped. */
export function parseCues(document: string): Cue[] {
  const cues: Cue[] = [];
  for (const block of document.replace(/\r/g, "").split(/\n{2,}/)) {
    const lines = block.split("\n");
    const at = lines.findIndex((l) => l.includes("-->"));
    if (at === -1) continue;
    const [start, end] = lines[at].split("-->").map((s) => seconds(s.trim().split(" ")[0]));
    const text = lines.slice(at + 1).join("\n").trim();
    if (Number.isFinite(start) && Number.isFinite(end) && text) cues.push({ start, end, text });
  }
  return cues;
}

/** 75.4 as "1:15"; an hour or more as "1:02:03". */
export function formatStamp(totalSeconds: number): string {
  const s = Math.floor(totalSeconds);
  const pad = (n: number) => String(n).padStart(2, "0");
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  return h > 0 ? `${h}:${pad(m)}:${pad(s % 60)}` : `${m}:${pad(s % 60)}`;
}
