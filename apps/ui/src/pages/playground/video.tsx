import { useMutation, useQuery } from "@tanstack/react-query";
import { CircleAlert, Clapperboard, Download, RotateCcw } from "lucide-react";
import { useState } from "react";
import { CopyButton } from "@/components/copy-button";
import { EmptyState } from "@/components/empty-state";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { useModelChoice } from "@/hooks/use-model-choice";
import { useNow } from "@/hooks/use-now";
import { useObjectUrl } from "@/hooks/use-object-url";
import { gatewayFetch, getJson, postJson } from "@/lib/api";
import { formatMs } from "@/lib/format";
import { describeError } from "@/lib/query-client";
import { MediaLayout } from "./media-form";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";
import { SelectField } from "./select-field";
import { submitOnModEnter } from "./submit-on-mod-enter";

interface VideoJob {
  id: string;
  status: "queued" | "in_progress" | "completed" | "failed" | string;
  progress?: number;
  error?: { message?: string } | null;
}

const POLL_MS = 2000;
const done = (job: VideoJob | undefined) => job?.status === "completed" || job?.status === "failed";

function useVideoJob(id: string | undefined) {
  const job = useQuery({
    queryKey: ["video", id],
    queryFn: ({ signal }) => getJson<VideoJob>(`/v1/videos/${encodeURIComponent(id!)}`, signal),
    enabled: !!id,
    // A failing poll (a 404 after a restart, say) is final: stop asking.
    refetchInterval: (q) => (done(q.state.data) || q.state.error ? false : POLL_MS),
  });
  const content = useQuery({
    queryKey: ["video-content", id],
    queryFn: async ({ signal }) =>
      (await gatewayFetch(`/v1/videos/${encodeURIComponent(id!)}/content`, { signal })).blob(),
    enabled: job.data?.status === "completed",
    staleTime: Infinity,
  });
  return { job, content };
}

/** A running job: its stage, a determinate bar, and how long it has been going. */
function Progress({ job, since, prompt }: { job: VideoJob; since: number; prompt: string }) {
  const now = useNow(1000);
  const percent = Math.min(100, Math.max(0, job.progress ?? 0));
  const queued = job.status === "queued";
  return (
    <Card className="gap-4 p-6" aria-busy="true">
      <div className="flex items-center justify-between gap-3">
        <span className="inline-flex items-center gap-2 text-sm font-medium">
          <span className="relative flex size-2">
            <span className="absolute inset-0 animate-ping rounded-full bg-brand opacity-60" />
            <span className="relative size-2 rounded-full bg-brand" />
          </span>
          {queued ? "Queued" : "Rendering"}
        </span>
        <span className="text-sm text-muted-foreground tabular">{percent}%</span>
      </div>
      <div
        className="h-2 overflow-hidden rounded-full bg-muted"
        role="progressbar"
        aria-valuenow={percent}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-label="Video progress"
      >
        <div
          className="h-full rounded-full bg-brand transition-[width] duration-700"
          style={{ width: `${Math.max(2, percent)}%` }}
        />
      </div>
      <p className="line-clamp-2 text-sm text-muted-foreground">{prompt}</p>
      <div className="flex items-center justify-between gap-3 text-xs text-muted-foreground">
        <span className="flex min-w-0 items-center gap-1">
          {job.id ? (
            <>
              <span className="truncate font-mono">{job.id}</span>
              <CopyButton value={job.id} label="Copy job id" />
            </>
          ) : (
            "Submitting the job…"
          )}
        </span>
        <span className="shrink-0 tabular">
          {formatMs(now - since)} · checking every {POLL_MS / 1000} s
        </span>
      </div>
    </Card>
  );
}

function Failure({ title, message, onRetry }: { title: string; message: string; onRetry: () => void }) {
  return (
    <div role="alert" className="grid gap-3 rounded-xl border border-destructive/30 bg-destructive/5 p-5 text-sm">
      <div className="flex gap-3">
        <CircleAlert className="mt-0.5 size-4 shrink-0 text-destructive" />
        <div className="min-w-0">
          <p className="font-medium">{title}</p>
          <p className="break-words text-muted-foreground">{message}</p>
        </div>
      </div>
      <Button type="button" variant="outline" size="sm" className="w-fit" onClick={onRetry}>
        <RotateCcw /> Try again
      </Button>
    </div>
  );
}

export function VideoTab() {
  const choice = useModelChoice("video", "sora-2");
  const [prompt, setPrompt] = useState("");
  const [seconds, setSeconds] = useState("4");
  const create = useMutation({
    mutationFn: (request: { prompt: string; seconds: string }) =>
      postJson<VideoJob>("/v1/videos", { model: choice.model, ...request }),
  });
  const { job, content } = useVideoJob(create.data?.id);
  const current = job.data ?? create.data;
  const url = useObjectUrl(content.data);
  const submit = () => create.mutate({ prompt: prompt.trim(), seconds });
  const running = create.isPending || (!!current && !done(current) && !job.error);

  return (
    <MediaLayout
      onSubmit={submit}
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
              onKeyDown={submitOnModEnter}
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
      {create.isError ? (
        <RequestError error={create.error} what="video" />
      ) : job.error ? (
        <Failure
          title="Lost track of the job"
          message={describeError(job.error)}
          onRetry={submit}
        />
      ) : current?.status === "failed" ? (
        <Failure
          title="The video couldn't be made"
          message={current.error?.message ?? "The provider reported a failure."}
          onRetry={submit}
        />
      ) : content.error ? (
        <Failure
          title="The video is ready, but its file wouldn't download"
          message={describeError(content.error)}
          onRetry={() => void content.refetch()}
        />
      ) : url ? (
        <Card className="gap-0 overflow-hidden p-0">
          <video src={url} controls autoPlay loop muted playsInline className="w-full bg-black" aria-label="Generated video" />
          <div className="flex items-center justify-between gap-3 border-t px-4 py-2.5 text-xs text-muted-foreground">
            <span className="flex min-w-0 items-center gap-1">
              <span className="truncate font-mono">{current?.id}</span>
              <CopyButton value={current?.id ?? ""} label="Copy job id" />
            </span>
            <Button asChild variant="ghost" size="sm">
              <a href={url} download="video.mp4">
                <Download /> Download
              </a>
            </Button>
          </div>
        </Card>
      ) : create.isPending ? (
        <Progress job={{ id: "", status: "queued" }} since={create.submittedAt} prompt={prompt} />
      ) : current ? (
        <Progress
          job={current.status === "completed" ? { ...current, progress: 100 } : current}
          since={create.submittedAt}
          prompt={create.variables?.prompt ?? ""}
        />
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
