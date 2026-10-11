import { useMutation, useQuery } from "@tanstack/react-query";
import { Clapperboard, Download } from "lucide-react";
import { useState } from "react";
import { EmptyState } from "@/components/empty-state";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { useModelChoice } from "@/hooks/use-model-choice";
import { useObjectUrl } from "@/hooks/use-object-url";
import { describeError } from "@/lib/query-client";
import { gatewayFetch, getJson, postJson } from "@/lib/api";
import { MediaLayout } from "./media-form";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";
import { SelectField } from "./select-field";

interface VideoJob {
  id: string;
  status: "queued" | "in_progress" | "completed" | "failed" | string;
  progress?: number;
  error?: { message?: string } | null;
}

const done = (job: VideoJob | undefined) => job?.status === "completed" || job?.status === "failed";

function useVideoJob(id: string | undefined) {
  const job = useQuery({
    queryKey: ["video", id],
    queryFn: ({ signal }) => getJson<VideoJob>(`/v1/videos/${encodeURIComponent(id!)}`, signal),
    enabled: !!id,
    // A failing poll (a 404 after a restart, say) is final: stop asking.
    refetchInterval: (q) => (done(q.state.data) || q.state.error ? false : 2000),
  });
  const content = useQuery({
    queryKey: ["video-content", id],
    queryFn: async ({ signal }) => (await gatewayFetch(`/v1/videos/${encodeURIComponent(id!)}/content`, { signal })).blob(),
    enabled: job.data?.status === "completed",
    staleTime: Infinity,
  });
  return { job, content };
}

function Progress({ job }: { job: VideoJob }) {
  const pct = Math.max(2, Math.min(100, job.progress ?? 0));
  const label = job.status === "queued" ? "Queued" : "Rendering";
  return (
    <Card className="gap-3 p-6">
      <div className="flex justify-between text-sm">
        <span className="font-medium">{label}…</span>
        <span className="text-muted-foreground tabular">{job.progress ?? 0}%</span>
      </div>
      <div
        className="h-2 overflow-hidden rounded-full bg-muted"
        role="progressbar"
        aria-valuenow={job.progress ?? 0}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-label="Video progress"
      >
        <div className="h-full rounded-full bg-brand transition-[width] duration-700" style={{ width: `${pct}%` }} />
      </div>
      <p className="font-mono text-xs text-muted-foreground">{job.id}</p>
    </Card>
  );
}

export function VideoTab() {
  const choice = useModelChoice("video", "sora-2");
  const [prompt, setPrompt] = useState("");
  const [seconds, setSeconds] = useState("4");
  const create = useMutation({
    mutationFn: () =>
      postJson<VideoJob>("/v1/videos", { model: choice.model, prompt: prompt.trim(), seconds }),
  });
  const { job, content } = useVideoJob(create.data?.id);
  const current = job.data ?? create.data;
  const url = useObjectUrl(content.data);
  const error = create.error ?? content.error;
  const running = create.isPending || (!!current && !done(current) && !job.error);

  return (
    <MediaLayout
      onSubmit={() => create.mutate()}
      pending={running}
      disabled={!prompt.trim()}
      submitLabel={running ? "Generating…" : "Create video"}
      fields={
        <>
          <div className="grid gap-2">
            <Label htmlFor="video-prompt">Prompt</Label>
            <Textarea
              id="video-prompt"
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
              placeholder="A paper boat drifting down a rainy street, close-up"
              className="min-h-28"
            />
          </div>
          <ModelPicker id="video-model" choice={choice} />
          <SelectField
            id="video-seconds"
            label="Length"
            value={seconds}
            onChange={setSeconds}
            options={["4", "8", "12"]}
            optionLabel={(s) => `${s} seconds`}
          />
        </>
      }
    >
      {error ? (
        <RequestError error={error} what="video" />
      ) : job.error ? (
        <RequestError
          error={new Error(`Lost track of the job: ${describeError(job.error)}`)}
          what="video"
        />
      ) : current?.status === "failed" ? (
        <RequestError error={new Error(current.error?.message ?? "The job failed")} what="video" />
      ) : url ? (
        <Card className="gap-3 overflow-hidden p-0">
          <video src={url} controls autoPlay loop className="w-full bg-black" aria-label="Generated video" />
          <div className="flex items-center justify-between px-4 pb-3 text-xs text-muted-foreground">
            <span className="font-mono">{current?.id}</span>
            <Button asChild variant="ghost" size="sm">
              <a href={url} download="video.mp4">
                <Download /> Download
              </a>
            </Button>
          </div>
        </Card>
      ) : current ? (
        <Progress job={current.status === "completed" ? { ...current, progress: 100 } : current} />
      ) : (
        <EmptyState icon={Clapperboard} title="Generate a video">
          <p>
            Creates a job at <code>/v1/videos</code>, follows its progress, and plays the result
            when it's done.
          </p>
        </EmptyState>
      )}
    </MediaLayout>
  );
}
