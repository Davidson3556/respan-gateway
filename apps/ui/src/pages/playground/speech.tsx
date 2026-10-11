import { useMutation } from "@tanstack/react-query";
import { AudioLines, Download } from "lucide-react";
import { useState } from "react";
import { EmptyState } from "@/components/empty-state";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";
import { useModelChoice } from "@/hooks/use-model-choice";
import { useObjectUrl } from "@/hooks/use-object-url";
import { gatewayFetch } from "@/lib/api";
import { MediaLayout } from "./media-form";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";
import { SelectField } from "./select-field";

const voices = ["alloy", "ash", "ballad", "coral", "echo", "fable", "nova", "onyx", "sage", "shimmer"];

export function SpeechTab() {
  const choice = useModelChoice("speech", "gpt-4o-mini-tts");
  const [text, setText] = useState("");
  const [voice, setVoice] = useState("alloy");
  const speak = useMutation({
    mutationFn: async () => {
      const res = await gatewayFetch("/v1/audio/speech", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: choice.model, input: text.trim(), voice }),
      });
      return res.blob();
    },
  });
  const url = useObjectUrl(speak.data);

  return (
    <MediaLayout
      onSubmit={() => speak.mutate()}
      pending={speak.isPending}
      disabled={!text.trim()}
      submitLabel={speak.isPending ? "Synthesizing…" : "Speak"}
      fields={
        <>
          <div className="grid gap-2">
            <Label htmlFor="speech-text">Text</Label>
            <Textarea
              id="speech-text"
              value={text}
              onChange={(e) => setText(e.target.value)}
              placeholder="Hello from the gateway."
              className="min-h-28"
            />
          </div>
          <ModelPicker id="speech-model" choice={choice} />
          <SelectField
            id="speech-voice"
            label="Voice"
            value={voice}
            onChange={setVoice}
            options={voices}
            className="capitalize"
          />
        </>
      }
    >
      {speak.isError ? (
        <RequestError error={speak.error} what="speech" />
      ) : speak.isPending ? (
        <Skeleton className="h-24 rounded-xl" />
      ) : url ? (
        <Card className="gap-4 p-6">
          <audio controls autoPlay src={url} className="w-full" aria-label="Generated speech" />
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span className="capitalize">
              {voice} · {choice.model}
            </span>
            <Button asChild variant="ghost" size="sm">
              <a href={url} download="speech.mp3">
                <Download /> Download
              </a>
            </Button>
          </div>
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
