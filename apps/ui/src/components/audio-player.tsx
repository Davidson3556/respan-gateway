import { Download, Pause, Play } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { useObjectUrl } from "@/hooks/use-object-url";
import { formatStamp } from "@/lib/subtitles";
import { decodePeaks } from "@/lib/waveform";

const BARS = 64;
// Until (or unless) the audio decodes, a quiet even skyline.
const placeholder = Array.from({ length: BARS }, (_, i) => 0.25 + 0.2 * Math.abs(Math.sin(i / 4)));

/** Plays a clip with its waveform as the scrubber, and offers it for download. */
export function AudioPlayer({
  blob,
  filename,
  label,
}: {
  blob: Blob;
  filename: string;
  label: string;
}) {
  const url = useObjectUrl(blob);
  const audio = useRef<HTMLAudioElement>(null);
  const [peaks, setPeaks] = useState<{ blob: Blob; peaks: number[] } | null>(null);
  const [playing, setPlaying] = useState(false);
  const [time, setTime] = useState(0);
  const [duration, setDuration] = useState(0);

  useEffect(() => {
    let current = true;
    void decodePeaks(blob, BARS).then((p) => current && p && setPeaks({ blob, peaks: p }));
    return () => {
      current = false;
    };
  }, [blob]);

  const bars = peaks?.blob === blob ? peaks.peaks : placeholder;
  const progress = duration > 0 ? time / duration : 0;
  const toggle = () => {
    const el = audio.current;
    if (el) void (el.paused ? el.play() : el.pause());
  };

  return (
    <div className="grid gap-3">
      <audio
        ref={audio}
        src={url ?? undefined}
        autoPlay
        aria-label={label}
        onPlay={() => setPlaying(true)}
        onPause={() => setPlaying(false)}
        onEnded={() => setPlaying(false)}
        onTimeUpdate={(e) => setTime(e.currentTarget.currentTime)}
        onLoadedMetadata={(e) => 
          setDuration(Number.isFinite(e.currentTarget.duration) ? e.currentTarget.duration : 0)
        }
      />
      <div className="flex items-center gap-3">
        <Button
          type="button"
          size="icon"
          className="size-10 shrink-0 rounded-full bg-brand text-white hover:bg-brand/90"
          onClick={toggle}
          aria-label={playing ? "Pause" : "Play"}
        >
          {playing ? <Pause className="fill-current" /> : <Play className="fill-current" />}
        </Button>
        <div className="relative h-12 min-w-0 flex-1 rounded-md focus-within:ring-[3px] focus-within:ring-ring/50">
          <div className="flex h-full items-center gap-[3px]" aria-hidden="true">
            {bars.map((peak, i) => (
              <span
                key={i}
                className={`flex-1 rounded-full transition-colors ${
                  i / BARS < progress ? "bg-brand" : "bg-muted-foreground/30"
                }`}
                style={{ height: `${Math.max(8, peak * 100)}%` }}
              />
            ))}
          </div>
          <input
            type="range"
            min={0}
            max={duration || 1}
            step="any"
            value={time}
            disabled={!duration}
            onChange={(e) => {
              if (audio.current) audio.current.currentTime = Number(e.target.value);
            }}
            aria-label="Seek"
            className="absolute inset-0 size-full cursor-pointer opacity-0 disabled:cursor-default"
          />
        </div>
      </div>
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span className="tabular">
          {formatStamp(time)} / {formatStamp(duration)}
        </span>
        {url && (
          <Button asChild variant="ghost" size="sm">
            <a href={url} download={filename}>
              <Download /> Download
            </a>
          </Button>
        )}
      </div>
    </div>
  );
}
