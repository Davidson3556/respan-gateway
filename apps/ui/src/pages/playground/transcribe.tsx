import { useMutation } from "@tanstack/react-query";
import { FileAudio, Mic, Square, Upload, X } from "lucide-react";
import { useState, type DragEvent } from "react";
import { toast } from "sonner";
import { CopyButton } from "@/components/copy-button";
import { EmptyState } from "@/components/empty-state";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { useModelChoice } from "@/hooks/use-model-choice";
import { useRecorder } from "@/hooks/use-recorder";
import { gatewayFetch } from "@/lib/api";
import { cn } from "@/lib/utils";
import { MediaLayout } from "./media-form";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";

function AudioInput({ file, onFile }: { file: File | null; onFile: (f: File | null) => void }) {
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

  if (file) {
    return (
      <div className="flex items-center gap-3 rounded-lg border p-3">
        <FileAudio className="size-5 shrink-0 text-brand" />
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm font-medium">{file.name}</div>
          <div className="text-xs text-muted-foreground tabular">
            {(file.size / 1024).toFixed(0)} KB
          </div>
        </div>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="size-7"
          onClick={() => onFile(null)}
          aria-label="Remove audio"
        >
          <X />
        </Button>
      </div>
    );
  }

  if (recorder.state === "recording") {
    return (
      <div className="flex items-center gap-3 rounded-lg border border-destructive/40 bg-destructive/5 p-3">
        <span className="relative flex size-2.5">
          <span className="absolute inset-0 animate-ping rounded-full bg-destructive opacity-60" />
          <span className="relative size-2.5 rounded-full bg-destructive" />
        </span>
        <span className="flex-1 text-sm tabular">
          Recording… {Math.floor(recorder.seconds / 60)}:
          {String(recorder.seconds % 60).padStart(2, "0")}
        </span>
        <Button type="button" size="sm" variant="outline" onClick={recorder.stop}>
          <Square className="size-3 fill-current" /> Stop
        </Button>
      </div>
    );
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
          "flex cursor-pointer flex-col items-center gap-2 rounded-lg border border-dashed px-4 py-6 text-center text-sm transition-colors hover:bg-muted/50 focus-within:ring-[3px] focus-within:ring-ring/50",
          over && "border-brand bg-brand-soft/50",
        )}
      >
        <Upload className="size-5 text-muted-foreground" />
        <span>
          <span className="font-medium">Drop an audio file</span> or click to browse
        </span>
        <span className="text-xs text-muted-foreground">mp3, wav, m4a, webm, ogg…</span>
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

export function TranscribeTab() {
  const choice = useModelChoice("transcription", "whisper-1");
  const [file, setFile] = useState<File | null>(null);
  const transcribe = useMutation({
    mutationFn: async (audio: File) => {
      const form = new FormData();
      form.append("file", audio);
      form.append("model", choice.model);
      form.append("response_format", "json");
      const res = await gatewayFetch("/v1/audio/transcriptions", { method: "POST", body: form });
      return ((await res.json()) as { text: string }).text;
    },
  });

  return (
    <MediaLayout
      onSubmit={() => file && transcribe.mutate(file)}
      pending={transcribe.isPending}
      disabled={!file}
      submitLabel={transcribe.isPending ? "Transcribing…" : "Transcribe"}
      fields={
        <>
          <div className="grid gap-2">
            <Label htmlFor="transcribe-file">Audio</Label>
            <AudioInput file={file} onFile={setFile} />
          </div>
          <ModelPicker id="transcribe-model" choice={choice} />
        </>
      }
    >
      {transcribe.isError ? (
        <RequestError error={transcribe.error} what="transcription" />
      ) : transcribe.isPending ? (
        <Card className="gap-3 p-6">
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-11/12" />
          <Skeleton className="h-4 w-2/3" />
        </Card>
      ) : transcribe.data !== undefined ? (
        <Card className="relative p-6 pr-12">
          <p className="text-sm leading-relaxed whitespace-pre-wrap">
            {transcribe.data || <span className="text-muted-foreground">No speech detected.</span>}
          </p>
          <CopyButton value={transcribe.data} label="Copy transcript" className="absolute top-3 right-3" />
        </Card>
      ) : (
        <EmptyState icon={Mic} title="Turn speech into text">
          <p>
            Upload or record audio; it's sent as multipart to <code>/v1/audio/transcriptions</code>.
          </p>
        </EmptyState>
      )}
    </MediaLayout>
  );
}
