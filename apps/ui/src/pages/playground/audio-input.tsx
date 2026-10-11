import { FileAudio, Mic, Square, Upload, X } from "lucide-react";
import { useState, type DragEvent } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { useObjectUrl } from "@/hooks/use-object-url";
import { useRecorder } from "@/hooks/use-recorder";
import { formatStamp } from "@/lib/subtitles";
import { cn } from "@/lib/utils";

const METER_BARS = 24;

/** A row of bars that light up from the left as the input gets louder. */
function LevelMeter({ level }: { level: number }) {
  return (
    <div
      role="meter"
      aria-label="Input level"
      aria-valuemin={0}
      aria-valuemax={1}
      aria-valuenow={Number(level.toFixed(2))}
      className="flex h-8 min-w-0 flex-1 items-center gap-[3px]"
    >
      {Array.from({ length: METER_BARS }, (_, i) => (
        <span
          key={i}
          className={cn(
            "flex-1 rounded-full transition-colors duration-75",
            level > i / METER_BARS ? "bg-destructive" : "bg-destructive/20",
          )}
          style={{ height: `${40 + 60 * Math.abs(Math.sin(i / 2.5))}%` }}
        />
      ))}
    </div>
  );
}

function Chosen({ file, onClear }: { file: File; onClear: () => void }) {
  const url = useObjectUrl(file);
  return (
    <div className="grid gap-3 rounded-lg border p-3">
      <div className="flex items-center gap-3">
        <span className="flex size-9 shrink-0 items-center justify-center rounded-md bg-brand-soft text-brand">
          <FileAudio className="size-4" />
        </span>
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm font-medium">{file.name}</div>
          <div className="text-xs text-muted-foreground tabular">{(file.size / 1024).toFixed(0)} KB</div>
        </div>
        <Button type="button" variant="ghost" size="icon" className="size-7" onClick={onClear} aria-label="Remove audio">
          <X />
        </Button>
      </div>
      {url && <audio controls src={url} className="h-9 w-full" aria-label="Chosen audio" />}
    </div>
  );
}

function Recording({ seconds, level, onStop }: { seconds: number; level: number; onStop: () => void }) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-destructive/40 bg-destructive/5 p-3">
      <span className="relative flex size-2.5 shrink-0">
        <span className="absolute inset-0 animate-ping rounded-full bg-destructive opacity-60" />
        <span className="relative size-2.5 rounded-full bg-destructive" />
      </span>
      <span className="w-10 shrink-0 text-sm tabular">{formatStamp(seconds)}</span>
      <LevelMeter level={level} />
      <Button type="button" size="sm" variant="outline" onClick={onStop}>
        <Square className="size-3 fill-current" /> Stop
      </Button>
    </div>
  );
}

/** Pick, drop or record the audio to transcribe. */
export function AudioInput({ file, onFile }: { file: File | null; onFile: (f: File | null) => void }) {
  const [over, setOver] = useState(false);
  const recorder = useRecorder(onFile);

  const drop = (e: DragEvent) => {
    e.preventDefault();
    setOver(false);
    const dropped = e.dataTransfer.files[0];
    if (dropped) onFile(dropped);
  };
  const record = () =>
    recorder.start().catch(() => toast.error("Microphone access was denied or is unavailable"));

  if (file) return <Chosen file={file} onClear={() => onFile(null)} />;
  if (recorder.state === "recording") {
    return <Recording seconds={recorder.seconds} level={recorder.level} onStop={recorder.stop} />;
  }
  return (
    <div className="grid gap-2">
      <label
        onDragOver={(e) => {
          e.preventDefault();
          setOver(true);
        }}
        onDragLeave={() => setOver(false)}
        onDrop={drop}
        className={cn(
          "flex cursor-pointer flex-col items-center gap-1.5 rounded-lg border border-dashed px-4 py-6 text-center text-sm transition-colors hover:bg-muted/50 focus-within:ring-[3px] focus-within:ring-ring/50",
          over && "border-brand bg-brand-soft/50",
        )}
      >
        <span className="mb-1 flex size-9 items-center justify-center rounded-full bg-muted text-muted-foreground">
          <Upload className="size-4" />
        </span>
        <span>
          <span className="font-medium">Drop an audio file</span> or click to browse
        </span>
        <span className="text-xs text-muted-foreground">mp3, wav, m4a, webm, ogg</span>
        <input
          id="transcribe-file"
          type="file"
          accept="audio/*,video/webm"
          className="sr-only"
          onChange={(e) => onFile(e.target.files?.[0] ?? null)}
        />
      </label>
      {recorder.state !== "unsupported" && (
        <Button type="button" variant="outline" onClick={record}>
          <Mic /> Record from microphone
        </Button>
      )}
    </div>
  );
}
