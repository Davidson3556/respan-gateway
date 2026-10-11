import { useMutation } from "@tanstack/react-query";
import { AudioLines } from "lucide-react";
import { useState } from "react";
import { AudioPlayer } from "@/components/audio-player";
import { ChoiceGroup } from "@/components/choice-group";
import { EmptyState } from "@/components/empty-state";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";
import { useModelChoice } from "@/hooks/use-model-choice";
import { gatewayFetch } from "@/lib/api";
import { MediaLayout } from "./media-form";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";
import { SelectField } from "./select-field";

const voices = ["alloy", "ash", "ballad", "coral", "echo", "fable", "nova", "onyx", "sage", "shimmer"];
const voiceChoices = voices.map((v) => ({ value: v, label: v[0].toUpperCase() + v.slice(1) }));
const formats = ["mp3", "opus", "aac", "flac", "wav"];
// OpenAI's limit; a provider may allow less.
const MAX_CHARS = 4096;

export function SpeechTab() {
  const choice = useModelChoice("speech", "gpt-4o-mini-tts");
  const [text, setText] = useState("");
  const [voice, setVoice] = useState("alloy");
  const [format, setFormat] = useState("mp3");
  const speak = useMutation({
    mutationFn: async () => {
      const res = await gatewayFetch("/v1/audio/speech", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: choice.model,
          input: text.trim(),
          voice,
          response_format: format,
        }),
      });
      return { audio: await res.blob(), voice, format, model: choice.model };
    },
  });
  const result = speak.data;

  return (
    <MediaLayout
      onSubmit={() => speak.mutate()}
      pending={speak.isPending}
      disabled={!text.trim() || text.length > MAX_CHARS}
      submitLabel={speak.isPending ? "Synthesizing…" : "Speak"}
      fields={
        <>
          <div className="grid gap-2">
            <div className="flex items-baseline justify-between">
              <Label htmlFor="speech-text">Text</Label>
              <span
                className={`text-xs tabular ${text.length > MAX_CHARS ? "text-destructive" : "text-muted-foreground"}`}
              >
                {text.length.toLocaleString()} / {MAX_CHARS.toLocaleString()}
              </span>
            </div>
            <Textarea
              id="speech-text"
              value={text}
              onChange={(e) => setText(e.target.value)}
              placeholder="Hello from the gateway."
              className="min-h-28"
            />
          </div>
          <ModelPicker id="speech-model" choice={choice} />
          <ChoiceGroup label="Voice" value={voice} onChange={setVoice} choices={voiceChoices} className="grid-cols-2" />
          <SelectField id="speech-format" label="Format" value={format} onChange={setFormat} options={formats} />
        </>
      }
    >
      {speak.isError ? (
        <RequestError error={speak.error} what="speech" />
      ) : speak.isPending ? (
        <Card className="gap-3 p-6" aria-busy="true">
          <div className="flex items-center gap-3">
            <Skeleton className="size-10 rounded-full" />
            <Skeleton className="h-12 flex-1" />
          </div>
          <Skeleton className="h-4 w-24" />
        </Card>
      ) : result ? (
        <Card className="gap-4 p-6">
          <div className="flex items-center justify-between text-sm">
            <span className="font-medium capitalize">{result.voice}</span>
            <span className="font-mono text-xs text-muted-foreground">
              {result.model} · {result.format}
            </span>
          </div>
          <AudioPlayer blob={result.audio} filename={`speech.${result.format}`} label="Generated speech" />
        </Card>
      ) : (
        <EmptyState icon={AudioLines} title="Turn text into speech">
          <p>
            Sent to <code>/v1/audio/speech</code>; the audio plays here when it's ready.
          </p>
        </EmptyState>
      )}
    </MediaLayout>
  );
}
