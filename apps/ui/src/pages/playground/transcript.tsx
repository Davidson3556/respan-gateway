import { Download } from "lucide-react";
import { useMemo } from "react";
import { CopyButton } from "@/components/copy-button";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { useObjectUrl } from "@/hooks/use-object-url";
import { formatStamp, parseCues } from "@/lib/subtitles";

export type TranscriptFormat = "json" | "srt" | "vtt";

/** A transcript: timestamped lines for subtitles, running text otherwise. */
export function Transcript({ body, format }: { body: string; format: TranscriptFormat }) {
  const cues = useMemo(() => (format === "json" ? [] : parseCues(body)), [body, format]);
  const download = useObjectUrl(useMemo(() => new Blob([body]), [body]));

  return (
    <Card className="gap-0 py-0">
      <div className="flex items-center justify-between gap-2 border-b py-2 pr-2 pl-4">
        <span className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
          {format === "json" ? "Transcript" : `Subtitles (${format})`}
        </span>
        <div className="flex items-center">
          {format !== "json" && download && (
            <Button asChild variant="ghost" size="sm">
              <a href={download} download={`transcript.${format}`}>
                <Download /> Download
              </a>
            </Button>
          )}
          <CopyButton value={body} label="Copy transcript" />
        </div>
      </div>
      {cues.length ? (
        <ol className="divide-y">
          {cues.map((cue) => (
            <li key={cue.start} className="grid grid-cols-[3.5rem_1fr] items-baseline gap-3 px-4 py-2.5 text-sm">
              <time className="font-mono text-xs text-muted-foreground tabular">
                {formatStamp(cue.start)}
              </time>
              <p className="leading-relaxed whitespace-pre-wrap">{cue.text}</p>
            </li>
          ))}
        </ol>
      ) : (
        <p className="p-4 text-sm leading-relaxed whitespace-pre-wrap">
          {body || <span className="text-muted-foreground">No speech detected.</span>}
        </p>
      )}
    </Card>
  );
}
