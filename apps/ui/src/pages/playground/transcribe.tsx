import { useMutation } from "@tanstack/react-query";
import { Mic } from "lucide-react";
import { useState } from "react";
import { ChoiceGroup } from "@/components/choice-group";
import { EmptyState } from "@/components/empty-state";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { useModelChoice } from "@/hooks/use-model-choice";
import { useApiChoice, useLastCall } from "@/hooks/use-playground";
import { gatewayFetch } from "@/lib/api";
import { mediaApis } from "@/lib/playground-apis";
import { AudioInput } from "./audio-input";
import { MediaLayout } from "./media-form";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";
import { Transcript, type TranscriptFormat } from "./transcript";

const formats = [
  { value: "json", label: "Text", hint: "Plain transcript" },
  { value: "srt", label: "SRT", hint: "Timestamps" },
  { value: "vtt", label: "WebVTT", hint: "Timestamps" },
];

export function TranscribeTab() {
  const choice = useModelChoice("transcription", "whisper-1");
  const api = useApiChoice("transcription", mediaApis.transcription);
  const call = useLastCall();
  const [file, setFile] = useState<File | null>(null);
  const [format, setFormat] = useState<TranscriptFormat>("json");
  const transcribe = useMutation({
    mutationFn: ({ audio, as }: { audio: File; as: TranscriptFormat }) =>
      call.track({ api: api.api.id, model: choice.model }, async () => {
        const form = new FormData();
        form.append("file", audio);
        form.append("model", choice.model);
        form.append("response_format", as);
        const res = await gatewayFetch("/v1/audio/transcriptions", { method: "POST", body: form });
        const body = as === "json" ? ((await res.json()) as { text: string }).text : await res.text();
        return { body, as };
      }),
  });

  return (
    <MediaLayout
      id="transcribe"
      choice={api}
      record={call.record}
      onSubmit={() => file && transcribe.mutate({ audio: file, as: format })}
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
          <ChoiceGroup
            label="Output"
            value={format}
            onChange={(v) => setFormat(v as TranscriptFormat)}
            choices={formats}
            className="grid-cols-3"
          />
        </>
      }
    >
      {transcribe.isError ? (
        <RequestError error={transcribe.error} what="transcription" />
      ) : transcribe.isPending ? (
        <Card className="gap-3 p-6" aria-busy="true">
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-11/12" />
          <Skeleton className="h-4 w-2/3" />
        </Card>
      ) : transcribe.data ? (
        <Transcript body={transcribe.data.body} format={transcribe.data.as} />
      ) : (
        <EmptyState icon={Mic} title="Turn speech into text">
          <p>
            Upload or record audio; it's sent as multipart to <code>/v1/audio/transcriptions</code>.
            Pick SRT or WebVTT to get a timestamp for every line.
          </p>
        </EmptyState>
      )}
    </MediaLayout>
  );
}
